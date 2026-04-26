from __future__ import annotations

from collections.abc import Callable, Sequence

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
    TEST_INDEX,
    ParserStub,
    build_parser_ad,
    fetch_upload_timestamp,
    seed_upload_timestamp,
)


@pytest.mark.asyncio
async def test_retry_safety_keeps_checkpoint_until_es_write_succeeds(
    monkeypatch: pytest.MonkeyPatch,
    app_settings: IngestionServiceSettings,
    state_uow_factory: Callable[[], SqlAlchemyIngestionStateUnitOfWork],
    parser_stub: ParserStub,
    mongo_collection: AsyncIOMotorCollection,
    es_client: ElasticsearchHttpClient,
) -> None:
    ad = build_parser_ad(checked=T1, unique_id=2001, price=1_100_000)

    def _responses(_site_name: str, current_from: object) -> list[dict[str, object]]:
        if current_from == T0:
            return [ad]
        return []

    parser_stub.set_handler(_responses)
    seed_upload_timestamp(state_uow_factory, site_name="avito", timestamp=T0)

    original_bulk = ElasticsearchHttpClient.bulk
    fail_counter = {"count": 0}

    async def _flaky_bulk(
        self: ElasticsearchHttpClient,
        *,
        operations: Sequence[dict[str, object]],
        refresh: bool = False,
    ) -> dict[str, object]:
        if fail_counter["count"] == 0:
            fail_counter["count"] += 1
            raise RuntimeError("simulated elasticsearch write failure")
        return await original_bulk(self, operations=operations, refresh=refresh)

    monkeypatch.setattr(ElasticsearchHttpClient, "bulk", _flaky_bulk)

    with pytest.raises(RuntimeError, match="simulated elasticsearch write failure"):
        await ingestion_main._process_site(
            site_name="avito",
            state_uow_factory=state_uow_factory,
            load_till=LOAD_TILL,
            app_settings=app_settings,
            reporter=None,
        )

    assert fetch_upload_timestamp(state_uow_factory, site_name="avito") == T0

    await ingestion_main._process_site(
        site_name="avito",
        state_uow_factory=state_uow_factory,
        load_till=LOAD_TILL,
        app_settings=app_settings,
        reporter=None,
    )

    raw_count = await mongo_collection.count_documents({})
    assert raw_count == 2

    await es_client._request_json("POST", f"/{TEST_INDEX}/_refresh")
    es_response = await es_client.search(
        index=TEST_INDEX,
        query={"match_all": {}},
        size=10,
        sort=[{"_id": {"order": "asc"}}],
    )
    hits = es_response.get("hits", {}).get("hits", [])
    assert len(hits) == 1
    assert hits[0]["_id"] == "[avito]2001"

    assert fetch_upload_timestamp(state_uow_factory, site_name="avito") == T1
    assert parser_stub.calls[0] == ("avito", T0)
    assert parser_stub.calls[1] == ("avito", T0)
    assert parser_stub.calls[-1] == ("avito", T1)
