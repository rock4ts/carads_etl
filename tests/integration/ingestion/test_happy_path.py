from __future__ import annotations

from collections.abc import Callable
from datetime import datetime

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
    ParserStub,
    build_parser_ad,
    fetch_upload_timestamp,
    seed_upload_timestamp,
)


@pytest.mark.asyncio
async def test_happy_path_persists_all_storages_and_updates_checkpoint(
    app_settings: IngestionServiceSettings,
    state_uow_factory: Callable[[], SqlAlchemyIngestionStateUnitOfWork],
    parser_stub: ParserStub,
    mongo_collection: AsyncIOMotorCollection,
    es_client: ElasticsearchHttpClient,
) -> None:
    ad = build_parser_ad(checked=T1, unique_id=1001, price=1_500_000)
    responses: dict[datetime, list[dict[str, object]]] = {
        T0: [ad],
        T1: [],
    }
    parser_stub.set_handler(lambda _site, current_from: responses.get(current_from, []))

    seed_upload_timestamp(state_uow_factory, site_name="avito", timestamp=T0)

    await ingestion_main._process_site(
        site_name="avito",
        state_uow_factory=state_uow_factory,
        load_till=LOAD_TILL,
        app_settings=app_settings,
        reporter=None,
    )

    raw_docs = [doc async for doc in mongo_collection.find({})]
    assert len(raw_docs) == 1
    assert raw_docs[0]["request_params"]["from_datetime"] == T0.strftime("%Y-%m-%d %H:%M:%S")
    assert raw_docs[0]["payload"]["unique_id"] == 1001

    await es_client._request_json("POST", f"/{app_settings.processed_index}/_refresh")
    es_response = await es_client.search(
        index=app_settings.processed_index,
        query={"match_all": {}},
        size=10,
    )
    hits = es_response.get("hits", {}).get("hits", [])
    assert len(hits) == 1
    assert hits[0]["_id"] == "[avito]1001"
    assert hits[0]["_source"]["parapi_unique_id"] == 1001

    assert fetch_upload_timestamp(state_uow_factory, site_name="avito") == T1
    assert parser_stub.calls == [("avito", T0), ("avito", T1)]
