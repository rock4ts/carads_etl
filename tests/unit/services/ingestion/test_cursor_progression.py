from __future__ import annotations

import asyncio
from datetime import datetime

import pytest

from app.services.ingestion_service import main as ingestion_main
from app.services.ingestion_service.core.config import IngestionServiceSettings
from tests.unit.services.ingestion.support.builders import build_ad
from tests.unit.services.ingestion.support.mocks import (
    FakeParserClient,
    FakeStateUowFactory,
    FakeUploadTimestampRepo,
)


def test_cursor_progression_uses_previous_batch_max_checked(
    monkeypatch: pytest.MonkeyPatch,
    app_settings: IngestionServiceSettings,
    timestamps: dict[str, datetime],
) -> None:
    parser = FakeParserClient(
        [
            [build_ad(checked=timestamps["T1"], unique_id=5001), build_ad(checked=timestamps["T2"], unique_id=5002)],
            [build_ad(checked=timestamps["T3"], unique_id=5003)],
            [],
        ]
    )
    state_repo = FakeUploadTimestampRepo({"avito": timestamps["T0"]})
    state_uow_factory = FakeStateUowFactory(state_repo)

    async def _noop_persist(**kwargs: object) -> None:
        return

    monkeypatch.setattr(ingestion_main, "fetch_parser_ads", parser.fetch)
    monkeypatch.setattr(ingestion_main, "_persist_batch", _noop_persist)

    asyncio.run(
        ingestion_main._process_site(
            site_name="avito",
            state_uow_factory=state_uow_factory,
            load_till=timestamps["LOAD_TILL"],
            app_settings=app_settings,
        )
    )

    call_sequence = [call["current_from"] for call in parser.calls]
    assert call_sequence == [timestamps["T0"], timestamps["T2"], timestamps["T3"]]
    assert state_repo.upserts == [("avito", timestamps["T2"]), ("avito", timestamps["T3"])]


def test_load_till_boundary_stops_fetching_beyond_fixed_upper_bound(
    monkeypatch: pytest.MonkeyPatch,
    app_settings: IngestionServiceSettings,
    timestamps: dict[str, datetime],
) -> None:
    parser = FakeParserClient(
        [
            [build_ad(checked=timestamps["T1"], unique_id=6001)],
            [build_ad(checked=timestamps["T2"], unique_id=6002)],
            [build_ad(checked=timestamps["T3"], unique_id=6003)],
        ]
    )
    state_repo = FakeUploadTimestampRepo({"avito": timestamps["T0"]})
    state_uow_factory = FakeStateUowFactory(state_repo)

    async def _noop_persist(**kwargs: object) -> None:
        return

    monkeypatch.setattr(ingestion_main, "fetch_parser_ads", parser.fetch)
    monkeypatch.setattr(ingestion_main, "_persist_batch", _noop_persist)

    asyncio.run(
        ingestion_main._process_site(
            site_name="avito",
            state_uow_factory=state_uow_factory,
            load_till=timestamps["T2"],
            app_settings=app_settings,
        )
    )

    call_sequence = [call["current_from"] for call in parser.calls]
    assert call_sequence == [timestamps["T0"], timestamps["T1"]]
    assert state_repo.timestamps["avito"] == timestamps["T2"]


def test_empty_second_batch_stops_without_extra_commit(
    monkeypatch: pytest.MonkeyPatch,
    app_settings: IngestionServiceSettings,
    timestamps: dict[str, datetime],
) -> None:
    parser = FakeParserClient(
        [
            [build_ad(checked=timestamps["T1"], unique_id=7001)],
            [],
        ]
    )
    state_repo = FakeUploadTimestampRepo({"avito": timestamps["T0"]})
    state_uow_factory = FakeStateUowFactory(state_repo)

    async def _noop_persist(**kwargs: object) -> None:
        return

    monkeypatch.setattr(ingestion_main, "fetch_parser_ads", parser.fetch)
    monkeypatch.setattr(ingestion_main, "_persist_batch", _noop_persist)

    asyncio.run(
        ingestion_main._process_site(
            site_name="avito",
            state_uow_factory=state_uow_factory,
            load_till=timestamps["LOAD_TILL"],
            app_settings=app_settings,
        )
    )

    assert state_repo.upserts == [("avito", timestamps["T1"])]
    assert state_uow_factory.commit_log == [1]
    assert len(parser.calls) == 2
