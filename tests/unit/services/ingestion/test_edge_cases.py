from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Self, cast

import pytest

from app.services.ingestion_service import main as ingestion_main
from app.services.ingestion_service.core.config import IngestionServiceSettings
from app.services.telegram_notifier import TelegramReporter
from tests.unit.services.ingestion.support.builders import build_ad
from tests.unit.services.ingestion.support.mocks import FakeStateUowFactory, FakeUploadTimestampRepo


def test_empty_first_batch_stops_without_checkpoint_update(
    monkeypatch: pytest.MonkeyPatch,
    app_settings: IngestionServiceSettings,
    timestamps: dict[str, datetime],
) -> None:
    async def _fetch_empty(**kwargs: object) -> list[dict[str, object]]:
        return []

    async def _persist_should_not_run(**kwargs: object) -> None:
        raise AssertionError("Batch persistence must not run when parser returns an empty batch")

    state_repo = FakeUploadTimestampRepo({"avito": timestamps["T0"]})
    state_uow_factory = FakeStateUowFactory(state_repo)

    monkeypatch.setattr(ingestion_main, "fetch_parser_ads", _fetch_empty)
    monkeypatch.setattr(ingestion_main, "_persist_batch", _persist_should_not_run)

    asyncio.run(
        ingestion_main._process_site(
            site_name="avito",
            state_uow_factory=state_uow_factory,
            load_till=timestamps["LOAD_TILL"],
            app_settings=app_settings,
        )
    )

    assert state_repo.upserts == []
    assert state_uow_factory.commit_log == []


def test_non_advancing_cursor_logs_warning_and_stops(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    app_settings: IngestionServiceSettings,
    timestamps: dict[str, datetime],
) -> None:
    async def _fetch_same_timestamp(**kwargs: object) -> list[dict[str, object]]:
        return [build_ad(checked=timestamps["T0"], unique_id=8001)]

    async def _noop_persist(**kwargs: object) -> None:
        return

    state_repo = FakeUploadTimestampRepo({"avito": timestamps["T0"]})
    state_uow_factory = FakeStateUowFactory(state_repo)

    monkeypatch.setattr(ingestion_main, "fetch_parser_ads", _fetch_same_timestamp)
    monkeypatch.setattr(ingestion_main, "_persist_batch", _noop_persist)

    with caplog.at_level(logging.WARNING):
        asyncio.run(
            ingestion_main._process_site(
                site_name="avito",
                state_uow_factory=state_uow_factory,
                load_till=timestamps["LOAD_TILL"],
                app_settings=app_settings,
            )
        )

    assert "non-advancing cursor" in caplog.text
    assert state_repo.upserts == []
    assert state_uow_factory.commit_log == []


def test_parse_ads_batch_contract_accepts_supported_shapes() -> None:
    as_list = ingestion_main._parse_ads_batch([{"id": 1}, {"id": 2}])
    as_dict = ingestion_main._parse_ads_batch({"ads": [{"id": 3}]})

    assert as_list == [{"id": 1}, {"id": 2}]
    assert as_dict == [{"id": 3}]


def test_parse_ads_batch_contract_rejects_invalid_shape() -> None:
    with pytest.raises(RuntimeError, match="Parser response must be either a list"):
        ingestion_main._parse_ads_batch({"items": [{"id": 1}]})


def test_invalid_checked_timestamp_raises() -> None:
    with pytest.raises(RuntimeError, match="valid 'checked' timestamp"):
        ingestion_main._max_checked_timestamp([{"checked": "not-a-date"}])


def test_multi_site_processing_is_independent(
    monkeypatch: pytest.MonkeyPatch,
    app_settings: IngestionServiceSettings,
    timestamps: dict[str, datetime],
) -> None:
    responses = {
        ("avito", timestamps["T0"]): [build_ad(checked=timestamps["T1"], unique_id=9001, site_id=2)],
        ("avito", timestamps["T1"]): [],
        ("drom", timestamps["T0"]): [build_ad(checked=timestamps["T2"], unique_id=9101, site_id=4)],
        ("drom", timestamps["T2"]): [],
    }
    parser_calls: list[tuple[str, datetime]] = []

    async def _fetch_by_site_and_cursor(
        *,
        parser_api_url: str,
        parser_api_key: str,
        site_name: str,
        current_from: datetime,
    ) -> list[dict[str, object]]:
        parser_calls.append((site_name, current_from))
        return responses[(site_name, current_from)]

    async def _noop_persist(**kwargs: object) -> None:
        return

    state_repo = FakeUploadTimestampRepo({"avito": timestamps["T0"], "drom": timestamps["T0"]})
    state_uow_factory = FakeStateUowFactory(state_repo)

    monkeypatch.setattr(ingestion_main, "fetch_parser_ads", _fetch_by_site_and_cursor)
    monkeypatch.setattr(ingestion_main, "_persist_batch", _noop_persist)

    asyncio.run(
        ingestion_main._process_site(
            site_name="avito",
            state_uow_factory=state_uow_factory,
            load_till=timestamps["LOAD_TILL"],
            app_settings=app_settings,
        )
    )
    asyncio.run(
        ingestion_main._process_site(
            site_name="drom",
            state_uow_factory=state_uow_factory,
            load_till=timestamps["LOAD_TILL"],
            app_settings=app_settings,
        )
    )

    assert parser_calls == [
        ("avito", timestamps["T0"]),
        ("avito", timestamps["T1"]),
        ("drom", timestamps["T0"]),
        ("drom", timestamps["T2"]),
    ]
    assert state_repo.timestamps == {"avito": timestamps["T1"], "drom": timestamps["T2"]}


def test_run_uses_single_fixed_load_till_for_all_sites(
    monkeypatch: pytest.MonkeyPatch,
    app_settings: IngestionServiceSettings,
) -> None:
    frozen_now = datetime(2026, 2, 1, 10, 0, 0, tzinfo=timezone.utc)
    state_repo = FakeUploadTimestampRepo(
        {"avito": datetime(2026, 2, 1, 0, 0, 0), "drom": datetime(2026, 2, 1, 0, 0, 0)}
    )
    commit_log: list[int] = []
    process_calls: list[tuple[str, datetime]] = []
    frozen_now_calls: list[datetime | None] = []

    class _FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> datetime:
            frozen_now_calls.append(frozen_now)
            if tz is None:
                return frozen_now.replace(tzinfo=None)
            return frozen_now.astimezone(tz)

    class _FakeSqlAlchemyIngestionStateUnitOfWork:
        def __init__(self, session_factory: object) -> None:
            self.ingestion_states = state_repo
            self._session_factory = session_factory

        def __enter__(self) -> Self:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return

        def commit(self) -> None:
            commit_log.append(1)

        def rollback(self) -> None:
            return

    async def _capture_process_site(
        *,
        site_name: str,
        state_uow_factory: object,
        load_till: datetime,
        app_settings: IngestionServiceSettings,
        reporter: object | None = None,
        progress_report_interval_minutes: int = 5,
    ) -> None:
        process_calls.append((site_name, load_till))

    monkeypatch.setattr(ingestion_main, "settings", app_settings)
    monkeypatch.setattr(ingestion_main, "datetime", _FrozenDatetime)
    monkeypatch.setattr(ingestion_main, "ensure_etl_state_tables", lambda _: None)
    monkeypatch.setattr(ingestion_main, "build_postgres_session_factory", lambda _: object())
    monkeypatch.setattr(ingestion_main, "SqlAlchemyIngestionStateUnitOfWork", _FakeSqlAlchemyIngestionStateUnitOfWork)
    monkeypatch.setattr(ingestion_main, "_process_site", _capture_process_site)

    asyncio.run(ingestion_main.run_ingestion())

    assert frozen_now_calls == [frozen_now], (
        "load_till must be computed once at run start via datetime.now(timezone.utc); "
        f"got {len(frozen_now_calls)} now() call(s)"
    )
    assert [site for site, _ in process_calls] == ["avito", "drom"]
    assert all(load_till == frozen_now.replace(tzinfo=None) for _, load_till in process_calls)
    assert commit_log == []


def test_process_site_reports_progress_updates(
    monkeypatch: pytest.MonkeyPatch,
    app_settings: IngestionServiceSettings,
    timestamps: dict[str, datetime],
) -> None:
    async def _fetch_batches(**kwargs: object) -> list[dict[str, object]]:
        current_from = kwargs["current_from"]
        if current_from == timestamps["T0"]:
            return [build_ad(checked=timestamps["T1"], unique_id=9801)]
        return []

    async def _noop_persist(**kwargs: object) -> None:
        return

    class _Reporter:
        def __init__(self) -> None:
            self.progress_messages: list[str] = []

        async def send_progress(self, message: str) -> None:
            self.progress_messages.append(message)

    state_repo = FakeUploadTimestampRepo({"avito": timestamps["T0"]})
    state_uow_factory = FakeStateUowFactory(state_repo)
    reporter = _Reporter()

    monkeypatch.setattr(ingestion_main, "fetch_parser_ads", _fetch_batches)
    monkeypatch.setattr(ingestion_main, "_persist_batch", _noop_persist)

    asyncio.run(
        ingestion_main._process_site(
            site_name="avito",
            state_uow_factory=state_uow_factory,
            load_till=timestamps["LOAD_TILL"],
            app_settings=app_settings,
            reporter=cast(TelegramReporter, reporter),
        )
    )

    assert any("status=start" in msg for msg in reporter.progress_messages)
    assert any("status=in_progress" in msg for msg in reporter.progress_messages)
    assert any("status=finished" in msg for msg in reporter.progress_messages)


def test_run_reports_critical_message_when_site_fails(
    monkeypatch: pytest.MonkeyPatch,
    app_settings: IngestionServiceSettings,
) -> None:
    state_repo = FakeUploadTimestampRepo({"avito": datetime(2026, 2, 1, 0, 0, 0)})
    critical_messages: list[str] = []

    class _FakeSqlAlchemyIngestionStateUnitOfWork:
        def __init__(self, session_factory: object) -> None:
            self.ingestion_states = state_repo
            self._session_factory = session_factory

        def __enter__(self) -> Self:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return

        def commit(self) -> None:
            return

        def rollback(self) -> None:
            return

    class _Reporter:
        @classmethod
        def from_settings(cls, **kwargs: object) -> "_Reporter":
            return cls()

        async def send_critical(self, message: str) -> None:
            critical_messages.append(message)

    async def _raise_process_site(**kwargs: object) -> None:
        raise RuntimeError("site processing exploded")

    monkeypatch.setattr(ingestion_main, "settings", app_settings)
    monkeypatch.setattr(ingestion_main, "ensure_etl_state_tables", lambda _: None)
    monkeypatch.setattr(ingestion_main, "build_postgres_session_factory", lambda _: object())
    monkeypatch.setattr(ingestion_main, "SqlAlchemyIngestionStateUnitOfWork", _FakeSqlAlchemyIngestionStateUnitOfWork)
    monkeypatch.setattr(ingestion_main, "TelegramReporter", _Reporter)
    monkeypatch.setattr(ingestion_main, "_process_site", _raise_process_site)

    asyncio.run(ingestion_main.run_ingestion())

    assert len(critical_messages) == 1
    assert "status=failed" in critical_messages[0]


def test_process_site_throttles_loop_progress_reports(
    monkeypatch: pytest.MonkeyPatch,
    app_settings: IngestionServiceSettings,
    timestamps: dict[str, datetime],
) -> None:
    throttled_settings = app_settings.model_copy(update={"telegram_progress_interval_minutes": 10})

    responses = {
        timestamps["T0"]: [build_ad(checked=timestamps["T1"], unique_id=9811)],
        timestamps["T1"]: [build_ad(checked=timestamps["T2"], unique_id=9812)],
        timestamps["T2"]: [],
    }

    async def _fetch_batches(**kwargs: object) -> list[dict[str, object]]:
        current_from = kwargs["current_from"]
        assert isinstance(current_from, datetime)
        return responses[current_from]

    async def _noop_persist(**kwargs: object) -> None:
        return

    progress_messages: list[str] = []

    class _Reporter:
        async def send_progress(self, message: str) -> None:
            progress_messages.append(message)

    frozen_now = datetime(2026, 3, 1, 10, 0, 0, tzinfo=timezone.utc)
    now_values = iter([frozen_now, frozen_now + timedelta(minutes=1)])

    class _FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> datetime:
            current = next(now_values)
            if tz is None:
                return current.replace(tzinfo=None)
            return current.astimezone(tz)

    state_repo = FakeUploadTimestampRepo({"avito": timestamps["T0"]})
    state_uow_factory = FakeStateUowFactory(state_repo)

    monkeypatch.setattr(ingestion_main, "datetime", _FrozenDatetime)
    monkeypatch.setattr(ingestion_main, "fetch_parser_ads", _fetch_batches)
    monkeypatch.setattr(ingestion_main, "_persist_batch", _noop_persist)

    asyncio.run(
        ingestion_main._process_site(
            site_name="avito",
            state_uow_factory=state_uow_factory,
            load_till=timestamps["LOAD_TILL"],
            app_settings=throttled_settings,
            reporter=cast(TelegramReporter, _Reporter()),
            progress_report_interval_minutes=throttled_settings.telegram_progress_interval_minutes,
        )
    )

    progress_messages = [msg for msg in progress_messages if "status=in_progress" in msg]
    assert len(progress_messages) == 1
