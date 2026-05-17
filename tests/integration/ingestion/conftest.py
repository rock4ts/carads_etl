from __future__ import annotations

import json
from collections.abc import AsyncGenerator, Callable, Generator
from datetime import datetime, timezone, tzinfo
from urllib import error, request

import pytest
import pytest_asyncio
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from app.clients import processed_storage
from app.clients.elasticsearch_http import ElasticsearchHttpClient
from app.database.session import build_postgres_session_factory
from app.repositories import elasticsearch_processing_docs as processing_docs_repo
from app.services.ingestion_service import main as ingestion_main
from app.services.ingestion_service.core.config import IngestionServiceSettings
from app.services.processing_service import mapper as processing_mapper
from app.uow.ingestion_state_uow import SqlAlchemyIngestionStateUnitOfWork

POSTGRES_DATABASE_URL = (
    "postgresql+psycopg://postgres:postgres@localhost:5432/car_intel"
)
ELASTICSEARCH_URL = "http://localhost:19200"
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "etl_db"
RAW_COLLECTION_NAME = "raw_ads"
TEST_INDEX = "carads1_local_test"

T0 = datetime(2026, 1, 1, 0, 0, 0)
T1 = datetime(2026, 1, 1, 0, 10, 0)
T2 = datetime(2026, 1, 1, 0, 20, 0)
LOAD_TILL = datetime(2026, 1, 1, 1, 0, 0)


def _es_request(
    method: str, path: str, payload: dict[str, object] | None = None
) -> dict[str, object]:
    url = f"{ELASTICSEARCH_URL}{path}"
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = request.Request(
        url=url,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with request.urlopen(req, timeout=10) as response:
        body = response.read().decode("utf-8")
    if not body:
        return {}
    parsed = json.loads(body)
    return parsed if isinstance(parsed, dict) else {}


class ParserStub:
    def __init__(self) -> None:
        self.calls: list[tuple[str, datetime]] = []
        self._handler: Callable[[str, datetime], list[dict[str, object]]] = (
            lambda _site, _from_dt: []
        )

    def set_handler(
        self, handler: Callable[[str, datetime], list[dict[str, object]]]
    ) -> None:
        self._handler = handler

    async def fetch(
        self,
        *,
        parser_api_url: str,
        parser_api_key: str,
        site_name: str,
        current_from: datetime,
        on_backoff_notify: Callable[[str], None] | None = None,
    ) -> list[dict[str, object]]:
        del parser_api_url, parser_api_key, on_backoff_notify
        self.calls.append((site_name, current_from))
        return self._handler(site_name, current_from)


@pytest.fixture
def app_settings() -> IngestionServiceSettings:
    return IngestionServiceSettings(
        POSTGRES_DATABASE_URL=POSTGRES_DATABASE_URL,
        ELASTICSEARCH_URL=ELASTICSEARCH_URL,
        PROCESSED_INDEX=TEST_INDEX,
        ELASTICSEARCH_API_KEY="",
        ELASTICSEARCH_USERNAME="",
        ELASTICSEARCH_PASSWORD="",
        MONGO_URI=MONGO_URI,
        MONGO_DB=MONGO_DB,
        RAW_COLLECTION_NAME=RAW_COLLECTION_NAME,
        PARSER_API_URL="https://parser.example.test",
        PARSER_API_KEY="test-key",
        TELEGRAM_REPORTING_ENABLED=False,
    )


@pytest.fixture
def frozen_now() -> datetime:
    return datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def session_factory() -> sessionmaker[Session]:
    return build_postgres_session_factory(POSTGRES_DATABASE_URL)


@pytest.fixture
def postgres_session(
    session_factory: sessionmaker[Session],
) -> Generator[Session, None, None]:
    with session_factory() as session:
        yield session


@pytest.fixture
def state_uow_factory(
    session_factory: sessionmaker[Session],
) -> Callable[[], SqlAlchemyIngestionStateUnitOfWork]:
    def _factory() -> SqlAlchemyIngestionStateUnitOfWork:
        return SqlAlchemyIngestionStateUnitOfWork(session_factory)

    return _factory


@pytest_asyncio.fixture
async def mongo_collection() -> AsyncGenerator[AsyncIOMotorCollection, None]:
    client = AsyncIOMotorClient(MONGO_URI)
    try:
        await client.admin.command("ping")
    except Exception as exc:
        client.close()
        pytest.skip(f"MongoDB is not reachable at {MONGO_URI}: {exc}")
    yield client[MONGO_DB][RAW_COLLECTION_NAME]
    client.close()


@pytest.fixture
def es_client() -> ElasticsearchHttpClient:
    return ElasticsearchHttpClient(ELASTICSEARCH_URL)


@pytest.fixture
def parser_stub(monkeypatch: pytest.MonkeyPatch) -> ParserStub:
    stub = ParserStub()
    monkeypatch.setattr(ingestion_main, "fetch_parser_ads", stub.fetch)
    return stub


@pytest.fixture(autouse=True)
def _use_test_index(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(processed_storage, "PROCESSED_INDEX_NAME", TEST_INDEX)


@pytest.fixture(autouse=True)
def _freeze_current_time(monkeypatch: pytest.MonkeyPatch, frozen_now: datetime) -> None:
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> datetime:
            if tz is None:
                return frozen_now.replace(tzinfo=None)
            return frozen_now.astimezone(tz)

    monkeypatch.setattr(ingestion_main, "datetime", FrozenDateTime)
    monkeypatch.setattr(processing_mapper, "datetime", FrozenDateTime)
    monkeypatch.setattr(processing_docs_repo, "datetime", FrozenDateTime)


@pytest_asyncio.fixture(autouse=True)
async def _clean_backends(
    session_factory: sessionmaker[Session],
    mongo_collection: AsyncIOMotorCollection,
) -> None:
    try:
        with session_factory() as session:
            session.execute(text("SELECT 1"))
            session.execute(text("DROP TABLE IF EXISTS marker_timestamps"))
            session.execute(text("DROP TABLE IF EXISTS upload_timestamps"))
            session.execute(
                text(
                    "CREATE TABLE marker_timestamps ("
                    "site VARCHAR(64) PRIMARY KEY, "
                    "timestamp TIMESTAMP NOT NULL)"
                )
            )
            session.execute(
                text(
                    "CREATE TABLE upload_timestamps ("
                    "site VARCHAR(64) PRIMARY KEY, "
                    "timestamp TIMESTAMP NOT NULL)"
                )
            )
            session.commit()
    except Exception as exc:
        pytest.skip(f"PostgreSQL is not reachable at {POSTGRES_DATABASE_URL}: {exc}")

    await mongo_collection.delete_many({})

    try:
        _es_request("GET", "/")
        try:
            _es_request("DELETE", f"/{TEST_INDEX}")
        except error.HTTPError as exc:
            if exc.code != 404:
                raise
        _es_request("PUT", f"/{TEST_INDEX}", payload={})
    except Exception as exc:
        pytest.skip(f"Elasticsearch is not reachable at {ELASTICSEARCH_URL}: {exc}")


def seed_upload_timestamp(
    state_uow_factory: Callable[[], SqlAlchemyIngestionStateUnitOfWork],
    *,
    site_name: str,
    timestamp: datetime,
) -> None:
    with state_uow_factory() as uow:
        uow.ingestion_states.upsert_upload_timestamp(site_name, timestamp)
        uow.commit()


def fetch_upload_timestamp(
    state_uow_factory: Callable[[], SqlAlchemyIngestionStateUnitOfWork],
    *,
    site_name: str,
) -> datetime | None:
    with state_uow_factory() as uow:
        return uow.ingestion_states.get_upload_timestamp(site_name)


def build_parser_ad(
    *,
    checked: datetime,
    unique_id: int,
    price: int,
    site_id: int = 2,
) -> dict[str, object]:
    checked_str = checked.strftime("%Y-%m-%d %H:%M:%S")
    return {
        "checked": checked_str,
        "parsed": checked_str,
        "added": checked_str,
        "unique_id": unique_id,
        "id": f"source-{unique_id}",
        "site_id": site_id,
        "url": f"https://example.test/ads/{unique_id}",
        "name": f"Test car {unique_id}",
        "price": price,
        "actual": 1,
        "mark": "Toyota",
        "model": "Camry",
        "year": 2020,
        "run": 12345,
        "run_type": "km",
    }
