from __future__ import annotations

import gzip
from collections.abc import AsyncGenerator, Callable, Generator
from datetime import datetime, timezone, tzinfo
from typing import cast

import boto3
import pytest
import pytest_asyncio
from bson.json_util import loads as bson_json_loads
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from sqlalchemy import select, text
from sqlalchemy.orm import Session, sessionmaker

from app.clients.archive_storage import YandexObjectStorageArchiveStorage
from app.database.models import ArchiveBatch
from app.database.session import build_postgres_session_factory
from app.repositories.mongo_raw_archive import MongoRawArchiveRepository
from app.services.archiving_service import main as archiving_main
from app.services.archiving_service.core.config import ArchivingServiceSettings
from app.uow.archive_state_uow import SqlAlchemyArchiveStateUnitOfWork

POSTGRES_DATABASE_URL = (
    "postgresql+psycopg://postgres:postgres@localhost:5432/car_intel"
)
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "etl_db"
RAW_COLLECTION_NAME = "raw_ads"
S3_ENDPOINT = "http://localhost:19000"
S3_BUCKET = "archive-integration-test"
S3_PREFIX = "raw-archive"
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin"

FROZEN_NOW = datetime(2026, 5, 3, 12, 0, 0, tzinfo=timezone.utc)
T1 = datetime(2026, 1, 10, 0, 0, 0)
T2 = datetime(2026, 1, 10, 1, 0, 0)
T3 = datetime(2026, 2, 2, 0, 0, 0)


@pytest.fixture
def archive_settings() -> ArchivingServiceSettings:
    return ArchivingServiceSettings(
        MONGO_URI=MONGO_URI,
        MONGO_DB=MONGO_DB,
        RAW_COLLECTION_NAME=RAW_COLLECTION_NAME,
        POSTGRES_DATABASE_URL=POSTGRES_DATABASE_URL,
        ARCHIVE_RETENTION_DAYS=1,
        ARCHIVE_BATCH_SIZE=1000,
        S3_BUCKET=S3_BUCKET,
        S3_PREFIX=S3_PREFIX,
        S3_ENDPOINT=S3_ENDPOINT,
        AWS_ACCESS_KEY_ID=S3_ACCESS_KEY,
        AWS_SECRET_ACCESS_KEY=S3_SECRET_KEY,
        AWS_REGION="us-east-1",
    )


@pytest.fixture
def frozen_now() -> datetime:
    return FROZEN_NOW


@pytest.fixture
def session_factory() -> sessionmaker[Session]:
    return build_postgres_session_factory(POSTGRES_DATABASE_URL)


@pytest.fixture
def postgres_session(
    session_factory: sessionmaker[Session],
) -> Generator[Session, None, None]:
    with session_factory() as session:
        yield session


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
def raw_archive_repository(
    mongo_collection: AsyncIOMotorCollection,
) -> MongoRawArchiveRepository:
    return MongoRawArchiveRepository(mongo_collection)


@pytest.fixture
def state_uow_factory(
    session_factory: sessionmaker[Session],
) -> Callable[[], SqlAlchemyArchiveStateUnitOfWork]:
    def _factory() -> SqlAlchemyArchiveStateUnitOfWork:
        return SqlAlchemyArchiveStateUnitOfWork(session_factory)

    return _factory


@pytest.fixture
def s3_client() -> BaseClient:
    client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
    )
    try:
        client.list_buckets()
    except Exception as exc:
        pytest.skip(f"S3-compatible storage is not reachable at {S3_ENDPOINT}: {exc}")
    _ensure_bucket(client, S3_BUCKET)
    return client


@pytest.fixture
def archive_storage() -> YandexObjectStorageArchiveStorage:
    return YandexObjectStorageArchiveStorage(
        bucket=S3_BUCKET,
        endpoint_url=S3_ENDPOINT,
        access_key_id=S3_ACCESS_KEY,
        secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
    )


@pytest.fixture(autouse=True)
def _freeze_archive_time(
    monkeypatch: pytest.MonkeyPatch, frozen_now: datetime
) -> None:
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> datetime:
            if tz is None:
                return frozen_now.replace(tzinfo=None)
            return frozen_now.astimezone(tz)

    monkeypatch.setattr(archiving_main, "datetime", FrozenDateTime)


@pytest_asyncio.fixture(autouse=True)
async def _clean_backends(
    session_factory: sessionmaker[Session],
    mongo_collection: AsyncIOMotorCollection,
    s3_client: BaseClient,
) -> None:
    try:
        with session_factory() as session:
            session.execute(text("SELECT 1"))
            session.execute(text("TRUNCATE TABLE archive_batches"))
            session.commit()
    except Exception as exc:
        pytest.skip(f"PostgreSQL is not reachable at {POSTGRES_DATABASE_URL}: {exc}")

    await mongo_collection.delete_many({})
    delete_s3_prefix(s3_client=s3_client, bucket=S3_BUCKET, prefix=S3_PREFIX)


def _ensure_bucket(s3_client: BaseClient, bucket: str) -> None:
    try:
        s3_client.head_bucket(Bucket=bucket)
    except ClientError:
        s3_client.create_bucket(Bucket=bucket)


def delete_s3_prefix(*, s3_client: BaseClient, bucket: str, prefix: str) -> None:
    continuation_token: str | None = None
    while True:
        kwargs: dict[str, object] = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token is not None:
            kwargs["ContinuationToken"] = continuation_token
        response = s3_client.list_objects_v2(**kwargs)
        contents = cast(list[dict[str, object]], response.get("Contents", []))
        if contents:
            s3_client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": item["Key"]} for item in contents]},
            )
        if not response.get("IsTruncated"):
            break
        continuation_token = cast(str, response.get("NextContinuationToken"))


def list_archive_keys(*, s3_client: BaseClient, bucket: str, prefix: str) -> list[str]:
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = cast(list[dict[str, object]], response.get("Contents", []))
    return sorted(cast(str, item["Key"]) for item in contents)


def object_exists(*, s3_client: BaseClient, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError:
        return False
    return True


def read_archive_jsonl(
    *, s3_client: BaseClient, bucket: str, key: str
) -> list[dict[str, object]]:
    body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    decompressed = gzip.decompress(body).decode("utf-8")
    lines = [line for line in decompressed.splitlines() if line.strip()]
    return [cast(dict[str, object], bson_json_loads(line)) for line in lines]


async def seed_raw_docs(
    mongo_collection: AsyncIOMotorCollection, docs: list[dict[str, object]]
) -> None:
    if not docs:
        return
    await mongo_collection.insert_many(docs)


def build_raw_doc(*, doc_id: str, ingested_at: datetime, payload: str) -> dict[str, object]:
    return {
        "_id": doc_id,
        "ingested_at": ingested_at.isoformat(),
        "payload": {"value": payload},
    }


def fetch_archive_batches(
    session_factory: sessionmaker[Session],
) -> list[ArchiveBatch]:
    with session_factory() as session:
        stmt = select(ArchiveBatch).order_by(
            ArchiveBatch.from_ingested_at.asc(), ArchiveBatch.id.asc()
        )
        return list(session.scalars(stmt))
