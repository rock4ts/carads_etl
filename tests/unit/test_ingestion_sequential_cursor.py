from __future__ import annotations

import asyncio
from datetime import datetime

import pytest

from app.services.ingestion_service.core.config import IngestionServiceSettings
from app.services.ingestion_service import main as ingestion_main


class _FakeIngestionStates:
    def __init__(self, initial_timestamp: datetime) -> None:
        self._timestamp = initial_timestamp
        self.upserted: list[datetime] = []

    def get_upload_timestamp(self, site_name: str) -> datetime | None:
        return self._timestamp

    def upsert_upload_timestamp(self, site_name: str, timestamp: datetime) -> None:
        self._timestamp = timestamp
        self.upserted.append(timestamp)


class _FakeUow:
    def __init__(self, states: _FakeIngestionStates, commit_calls: list[int]) -> None:
        self.ingestion_states = states
        self._commit_calls = commit_calls

    def __enter__(self) -> "_FakeUow":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return

    def commit(self) -> None:
        self._commit_calls.append(1)

    def rollback(self) -> None:
        return


def _build_settings() -> IngestionServiceSettings:
    return IngestionServiceSettings(
        PARSER_API_URL="https://highload.kocherov.net/parser/api/auto_api/",
        PARSER_API_KEY="test-key",
        MONGO_URI="mongodb://localhost:27017",
        MONGO_DB="etl",
        RAW_COLLECTION_NAME="raw_ads",
        ELASTICSEARCH_URL="http://localhost:9200",
        ELASTICSEARCH_API_KEY="api-key",
        ELASTICSEARCH_USERNAME="elastic",
        ELASTICSEARCH_PASSWORD="password",
        POSTGRES_DATABASE_URL="postgresql://unused",
    )


def _build_state_factory(initial_timestamp: datetime) -> tuple[ingestion_main.StateUowFactory, _FakeIngestionStates, list[int]]:
    states = _FakeIngestionStates(initial_timestamp)
    commit_calls: list[int] = []

    def _factory() -> _FakeUow:
        return _FakeUow(states, commit_calls)

    return _factory, states, commit_calls


def test_empty_batch_stops_without_checkpoint_update(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fetch_parser_ads(**kwargs: object) -> list[dict[str, object]]:
        return []

    async def _persist_batch(**kwargs: object) -> None:
        raise AssertionError("Batch persistence must not run for empty batches")

    monkeypatch.setattr(ingestion_main, "fetch_parser_ads", _fetch_parser_ads)
    monkeypatch.setattr(ingestion_main, "_persist_batch", _persist_batch)

    initial = datetime(2026, 1, 1, 0, 0, 0)
    state_uow_factory, states, commit_calls = _build_state_factory(initial)

    asyncio.run(
        ingestion_main._process_site(
            site_name="avito",
            state_uow_factory=state_uow_factory,
            load_till=datetime(2026, 1, 1, 0, 5, 0),
            app_settings=_build_settings(),
        )
    )

    assert states.upserted == []
    assert commit_calls == []


def test_cursor_advances_to_max_checked_in_batch(monkeypatch: pytest.MonkeyPatch) -> None:
    batches: list[list[dict[str, object]]] = [
        [
            {"checked": "2026-01-01 00:00:05"},
            {"checked": "2026-01-01 00:00:09"},
        ],
        [],
    ]

    async def _fetch_parser_ads(**kwargs: object) -> list[dict[str, object]]:
        return batches.pop(0)

    async def _persist_batch(**kwargs: object) -> None:
        return

    monkeypatch.setattr(ingestion_main, "fetch_parser_ads", _fetch_parser_ads)
    monkeypatch.setattr(ingestion_main, "_persist_batch", _persist_batch)

    initial = datetime(2026, 1, 1, 0, 0, 0)
    state_uow_factory, states, commit_calls = _build_state_factory(initial)

    asyncio.run(
        ingestion_main._process_site(
            site_name="avito",
            state_uow_factory=state_uow_factory,
            load_till=datetime(2026, 1, 1, 0, 10, 0),
            app_settings=_build_settings(),
        )
    )

    assert states.upserted == [datetime(2026, 1, 1, 0, 0, 9)]
    assert commit_calls == [1]


def test_non_advancing_cursor_stops_without_checkpoint_update(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fetch_parser_ads(**kwargs: object) -> list[dict[str, object]]:
        return [{"checked": "2026-01-01 00:00:00"}]

    async def _persist_batch(**kwargs: object) -> None:
        return

    monkeypatch.setattr(ingestion_main, "fetch_parser_ads", _fetch_parser_ads)
    monkeypatch.setattr(ingestion_main, "_persist_batch", _persist_batch)

    initial = datetime(2026, 1, 1, 0, 0, 0)
    state_uow_factory, states, commit_calls = _build_state_factory(initial)

    asyncio.run(
        ingestion_main._process_site(
            site_name="avito",
            state_uow_factory=state_uow_factory,
            load_till=datetime(2026, 1, 1, 0, 2, 0),
            app_settings=_build_settings(),
        )
    )

    assert states.upserted == []
    assert commit_calls == []


def test_failure_does_not_commit_checkpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fetch_parser_ads(**kwargs: object) -> list[dict[str, object]]:
        return [{"checked": "2026-01-01 00:00:05"}]

    async def _persist_batch(**kwargs: object) -> None:
        raise RuntimeError("simulated persistence failure")

    monkeypatch.setattr(ingestion_main, "fetch_parser_ads", _fetch_parser_ads)
    monkeypatch.setattr(ingestion_main, "_persist_batch", _persist_batch)

    initial = datetime(2026, 1, 1, 0, 0, 0)
    state_uow_factory, states, commit_calls = _build_state_factory(initial)

    with pytest.raises(RuntimeError, match="simulated persistence failure"):
        asyncio.run(
            ingestion_main._process_site(
                site_name="avito",
                state_uow_factory=state_uow_factory,
                load_till=datetime(2026, 1, 1, 0, 10, 0),
                app_settings=_build_settings(),
            )
        )

    assert states.upserted == []
    assert commit_calls == []
