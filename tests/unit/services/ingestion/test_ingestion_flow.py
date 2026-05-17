from __future__ import annotations

import asyncio
from datetime import datetime

import pytest

from app.services.ingestion_service import main as ingestion_main
from app.services.ingestion_service.core.config import IngestionServiceSettings
from tests.unit.services.ingestion.support.builders import build_ad
from tests.unit.services.ingestion.support.mocks import (
    FakeES,
    FakeMongo,
    FakeParserClient,
    FakeStateUowFactory,
    FakeUploadTimestampRepo,
)


def test_basic_flow_persists_data_and_updates_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
    app_settings: IngestionServiceSettings,
    timestamps: dict[str, datetime],
) -> None:
    parser = FakeParserClient(
        [
            [build_ad(checked=timestamps["T1"], unique_id=1001), build_ad(checked=timestamps["T1"], unique_id=1002)],
            [build_ad(checked=timestamps["T2"], unique_id=1003)],
            [],
        ]
    )
    mongo = FakeMongo()
    es = FakeES()
    state_repo = FakeUploadTimestampRepo({"avito": timestamps["T0"]})
    state_uow_factory = FakeStateUowFactory(state_repo)

    monkeypatch.setattr(ingestion_main, "fetch_parser_ads", parser.fetch)
    monkeypatch.setattr(ingestion_main, "save_raw_ads", mongo.save)
    monkeypatch.setattr(ingestion_main, "save_processed_docs", es.save)

    asyncio.run(
        ingestion_main._process_site(
            site_name="avito",
            state_uow_factory=state_uow_factory,
            load_till=timestamps["LOAD_TILL"],
            app_settings=app_settings,
        )
    )

    assert len(mongo.saved_raw_ads) == 3
    assert len(es.indexed_docs) == 3
    assert state_repo.timestamps["avito"] == timestamps["T2"]
    assert state_repo.upserts == [("avito", timestamps["T1"]), ("avito", timestamps["T2"])]
    assert state_uow_factory.commit_log == [1, 1]


def test_duplicate_ads_are_kept_in_raw_and_overwritten_in_es(
    monkeypatch: pytest.MonkeyPatch,
    app_settings: IngestionServiceSettings,
    timestamps: dict[str, datetime],
) -> None:
    duplicate_checked = timestamps["T1"]
    parser = FakeParserClient(
        [
            [build_ad(checked=duplicate_checked, unique_id=2001), build_ad(checked=duplicate_checked, unique_id=2001)],
            [],
        ]
    )
    mongo = FakeMongo()
    es = FakeES()
    state_repo = FakeUploadTimestampRepo({"avito": timestamps["T0"]})
    state_uow_factory = FakeStateUowFactory(state_repo)

    monkeypatch.setattr(ingestion_main, "fetch_parser_ads", parser.fetch)
    monkeypatch.setattr(ingestion_main, "save_raw_ads", mongo.save)
    monkeypatch.setattr(ingestion_main, "save_processed_docs", es.save)

    asyncio.run(
        ingestion_main._process_site(
            site_name="avito",
            state_uow_factory=state_uow_factory,
            load_till=timestamps["LOAD_TILL"],
            app_settings=app_settings,
        )
    )

    assert len(mongo.saved_raw_ads) == 2
    assert len(es.indexed_docs) == 2
    assert list(es.docs_by_id.keys()) == ["[avito]2001"]
    assert state_repo.timestamps["avito"] == duplicate_checked
    assert state_uow_factory.commit_log == [1]


def test_failure_during_processed_write_does_not_update_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
    app_settings: IngestionServiceSettings,
    timestamps: dict[str, datetime],
) -> None:
    parser = FakeParserClient([[build_ad(checked=timestamps["T1"], unique_id=3001)]])
    mongo = FakeMongo()
    state_repo = FakeUploadTimestampRepo({"avito": timestamps["T0"]})
    state_uow_factory = FakeStateUowFactory(state_repo)

    async def _raise_on_processed(*args: object, **kwargs: object) -> None:
        raise RuntimeError("simulated processed write failure")

    monkeypatch.setattr(ingestion_main, "fetch_parser_ads", parser.fetch)
    monkeypatch.setattr(ingestion_main, "save_raw_ads", mongo.save)
    monkeypatch.setattr(ingestion_main, "save_processed_docs", _raise_on_processed)

    with pytest.raises(RuntimeError, match="processed write failure"):
        asyncio.run(
            ingestion_main._process_site(
                site_name="avito",
                state_uow_factory=state_uow_factory,
                load_till=timestamps["LOAD_TILL"],
                app_settings=app_settings,
            )
        )

    assert len(mongo.saved_raw_ads) == 1
    assert state_repo.upserts == []
    assert state_uow_factory.commit_log == []


def test_failure_during_mapping_does_not_update_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
    app_settings: IngestionServiceSettings,
    timestamps: dict[str, datetime],
) -> None:
    parser = FakeParserClient([[build_ad(checked=timestamps["T1"], unique_id=4001)]])
    mongo = FakeMongo()
    state_repo = FakeUploadTimestampRepo({"avito": timestamps["T0"]})
    state_uow_factory = FakeStateUowFactory(state_repo)

    def _raise_on_mapping(raw: object) -> object:
        raise RuntimeError("simulated mapping failure")

    monkeypatch.setattr(ingestion_main, "fetch_parser_ads", parser.fetch)
    monkeypatch.setattr(ingestion_main, "save_raw_ads", mongo.save)
    monkeypatch.setattr(ingestion_main, "map_raw_to_processed", _raise_on_mapping)

    with pytest.raises(RuntimeError, match="mapping failure"):
        asyncio.run(
            ingestion_main._process_site(
                site_name="avito",
                state_uow_factory=state_uow_factory,
                load_till=timestamps["LOAD_TILL"],
                app_settings=app_settings,
            )
        )

    assert len(mongo.saved_raw_ads) == 1
    assert state_repo.upserts == []
    assert state_uow_factory.commit_log == []


def test_persist_batch_uses_processed_index_from_settings(
    monkeypatch: pytest.MonkeyPatch,
    app_settings: IngestionServiceSettings,
    timestamps: dict[str, datetime],
) -> None:
    captured_index_name: str | None = None

    async def _noop_save_raw_ads(*args: object, **kwargs: object) -> None:
        return

    def _fake_map_raw_to_processed(raw: object) -> object:
        return {"doc": "value"}

    async def _capture_save_processed_docs(*args: object, **kwargs: object) -> None:
        nonlocal captured_index_name
        index_name = kwargs.get("index_name")
        assert isinstance(index_name, str)
        captured_index_name = index_name

    custom_settings = app_settings.model_copy(update={"processed_index": "custom_index"})

    monkeypatch.setattr(ingestion_main, "save_raw_ads", _noop_save_raw_ads)
    monkeypatch.setattr(ingestion_main, "map_raw_to_processed", _fake_map_raw_to_processed)
    monkeypatch.setattr(ingestion_main, "save_processed_docs", _capture_save_processed_docs)

    asyncio.run(
        ingestion_main._persist_batch(
            site_name="avito",
            request_params={"site": "avito", "from_datetime": "2026-01-01 00:00:00"},
            ads_batch=[build_ad(checked=timestamps["T1"], unique_id=7777)],
            app_settings=custom_settings,
        )
    )

    assert captured_index_name == "custom_index"
