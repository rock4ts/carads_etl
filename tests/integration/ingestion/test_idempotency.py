from __future__ import annotations

from collections import deque
from collections.abc import Callable

import pytest
from motor.motor_asyncio import AsyncIOMotorCollection

from app.clients.elasticsearch_http import ElasticsearchHttpClient
from app.services.ingestion_service import main as ingestion_main
from app.services.ingestion_service.core.config import IngestionServiceSettings
from app.uow.ingestion_state_uow import SqlAlchemyIngestionStateUnitOfWork
from tests.integration.ingestion.conftest import (
    LOAD_TILL,
    T0,
    T1,
    T2,
    ParserStub,
    build_parser_ad,
    fetch_upload_timestamp,
    seed_upload_timestamp,
)


@pytest.mark.asyncio
async def test_repeat_ingestion_keeps_raw_history_and_single_es_logical_document(
    app_settings: IngestionServiceSettings,
    state_uow_factory: Callable[[], SqlAlchemyIngestionStateUnitOfWork],
    parser_stub: ParserStub,
    mongo_collection: AsyncIOMotorCollection,
    es_client: ElasticsearchHttpClient,
) -> None:
    first_snapshot = build_parser_ad(checked=T1, unique_id=3001, price=1_000_000)
    second_snapshot = build_parser_ad(checked=T2, unique_id=3001, price=1_250_000)

    batches = deque(
        [
            [first_snapshot],
            [],
            [second_snapshot],
            [],
        ]
    )
    parser_stub.set_handler(lambda _site_name, _current_from: batches.popleft() if batches else [])

    seed_upload_timestamp(state_uow_factory, site_name="avito", timestamp=T0)
    await ingestion_main._process_site(
        site_name="avito",
        state_uow_factory=state_uow_factory,
        load_till=LOAD_TILL,
        app_settings=app_settings,
        reporter=None,
    )
    assert fetch_upload_timestamp(state_uow_factory, site_name="avito") == T1

    seed_upload_timestamp(state_uow_factory, site_name="avito", timestamp=T0)
    await ingestion_main._process_site(
        site_name="avito",
        state_uow_factory=state_uow_factory,
        load_till=LOAD_TILL,
        app_settings=app_settings,
        reporter=None,
    )

    raw_docs = [doc async for doc in mongo_collection.find({}).sort("payload.checked", 1)]
    assert len(raw_docs) == 2
    assert raw_docs[0]["payload"]["price"] == 1_000_000
    assert raw_docs[1]["payload"]["price"] == 1_250_000

    await es_client._request_json("POST", f"/{app_settings.processed_index}/_refresh")
    es_response = await es_client.search(
        index=app_settings.processed_index,
        query={"match_all": {}},
        size=10,
    )
    hits = es_response.get("hits", {}).get("hits", [])
    assert len(hits) == 1
    assert hits[0]["_id"] == "[avito]3001"
    assert hits[0]["_source"]["latest_price"] == 1_250_000.0
    assert hits[0]["_source"]["last_checked"].startswith("2026-01-01T00:20:00")

    assert fetch_upload_timestamp(state_uow_factory, site_name="avito") == T2
