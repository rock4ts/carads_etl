from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone, tzinfo
from collections.abc import Callable, Sequence
from typing import Self, cast

import pytest

from app.repositories.elasticsearch_processed_ads import ElasticsearchProcessedAdsRepository
from app.services.matching_service import main as matching_main
from app.services.matching_service.main import _iter_sites
from app.services.telegram_notifier import TelegramReporter
from app.uow.matching_state_uow import MatchingStateUnitOfWork


def test_iter_sites_filters_requested_subset() -> None:
    sites = ["avito", "drom", "auto"]

    selected = _iter_sites(sites, [" auto ", "missing"])

    assert selected == ["auto"]


def test_process_site_reports_progress_for_started_and_finished_window() -> None:
    lower_bound = datetime(2026, 1, 1, 0, 0, 0)
    upper_bound = datetime(2026, 1, 1, 0, 5, 0)

    class _MatchingStates:
        def list_upload_sites(self) -> Sequence[str]:
            return ()

        def get_upload_timestamp(self, site_name: str) -> datetime:
            return upper_bound

        def get_marker_timestamp(self, site_name: str) -> datetime:
            return lower_bound

        def upsert_marker_timestamp(self, site_name: str, marker_timestamp: datetime) -> None:
            return

    class _StateUow:
        def __init__(self) -> None:
            self.matching_states = _MatchingStates()

        def __enter__(self) -> Self:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return

        def commit(self) -> None:
            return

        def rollback(self) -> None:
            return

    class _ProcessedAdsRepo:
        async def search_window(self, **kwargs: object) -> list[dict[str, object]]:
            return []

    class _Reporter:
        def __init__(self) -> None:
            self.messages: list[str] = []

        async def send_progress(self, message: str) -> None:
            self.messages.append(message)

    reporter = _Reporter()
    state_uow_factory = cast(Callable[[], MatchingStateUnitOfWork], _StateUow)
    processed_ads_repo = cast(ElasticsearchProcessedAdsRepository, _ProcessedAdsRepo())

    asyncio.run(
        matching_main._process_site(
            site_name="avito",
            state_uow_factory=state_uow_factory,
            processed_ads_repo=processed_ads_repo,
            batch_size=500,
            reporter=cast(TelegramReporter, reporter),
        )
    )

    assert any("status=start" in msg for msg in reporter.messages)
    assert any("status=finished" in msg for msg in reporter.messages)


def test_run_reports_critical_when_site_processing_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    critical_messages: list[str] = []

    class _MatchingStates:
        def list_upload_sites(self) -> list[str]:
            return ["avito"]

    class _FakeSqlAlchemyMatchingStateUnitOfWork:
        def __init__(self, session_factory: object) -> None:
            self.matching_states = _MatchingStates()

        def __enter__(self) -> Self:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return

        def commit(self) -> None:
            return

        def rollback(self) -> None:
            return

    class _FakeEsClient:
        def __init__(self, *args: object, **kwargs: object) -> None:
            return

    class _FakeProcessedAdsRepo:
        def __init__(self, *args: object, **kwargs: object) -> None:
            return

        async def ensure_mapping(self, body: dict[str, object]) -> None:
            return

    class _Reporter:
        @classmethod
        def from_settings(cls, **kwargs: object) -> "_Reporter":
            return cls()

        async def send_critical(self, message: str) -> None:
            critical_messages.append(message)

    async def _raise_process_site(**kwargs: object) -> None:
        raise RuntimeError("simulated site failure")

    monkeypatch.setattr(matching_main, "build_postgres_session_factory", lambda _: object())
    monkeypatch.setattr(matching_main, "SqlAlchemyMatchingStateUnitOfWork", _FakeSqlAlchemyMatchingStateUnitOfWork)
    monkeypatch.setattr(matching_main, "ElasticsearchHttpClient", _FakeEsClient)
    monkeypatch.setattr(matching_main, "ElasticsearchProcessedAdsRepository", _FakeProcessedAdsRepo)
    monkeypatch.setattr(matching_main, "configure_matcher", lambda **kwargs: None)
    monkeypatch.setattr(matching_main, "TelegramReporter", _Reporter)
    monkeypatch.setattr(matching_main, "_process_site", _raise_process_site)

    with pytest.raises(RuntimeError, match="simulated site failure"):
        asyncio.run(matching_main.run_matcher())

    assert len(critical_messages) == 1
    assert "site=avito status=failed" in critical_messages[0]


def test_process_site_throttles_loop_progress_reports(monkeypatch: pytest.MonkeyPatch) -> None:
    marker_start = datetime(2026, 1, 1, 0, 0, 0)
    upload_end = datetime(2026, 1, 1, 0, 10, 0)

    class _MatchingStates:
        def __init__(self) -> None:
            self._marker = marker_start

        def list_upload_sites(self) -> Sequence[str]:
            return ()

        def get_upload_timestamp(self, site_name: str) -> datetime:
            return upload_end

        def get_marker_timestamp(self, site_name: str) -> datetime:
            return self._marker

        def upsert_marker_timestamp(self, site_name: str, marker_timestamp: datetime) -> None:
            self._marker = marker_timestamp

    class _StateUow:
        def __init__(self) -> None:
            self.matching_states = _MatchingStates()

        def __enter__(self) -> Self:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return

        def commit(self) -> None:
            return

        def rollback(self) -> None:
            return

    class _ProcessedAdsRepo:
        def __init__(self) -> None:
            self._calls = 0

        async def search_window(self, **kwargs: object) -> list[dict[str, object]]:
            self._calls += 1
            if self._calls == 1:
                return [
                    {
                        "_id": "id-1",
                        "_source": {"offer_start": "2026-01-01T00:01:00+00:00"},
                        "sort": ["2026-01-01T00:01:00+00:00", "id-1"],
                    }
                ]
            if self._calls == 2:
                return [
                    {
                        "_id": "id-2",
                        "_source": {"offer_start": "2026-01-01T00:02:00+00:00"},
                        "sort": ["2026-01-01T00:02:00+00:00", "id-2"],
                    }
                ]
            return []

        async def claim_duplicate(self, **kwargs: object) -> bool:
            return False

        async def link_predecessors(self, **kwargs: object) -> int:
            return 0

    class _Reporter:
        def __init__(self) -> None:
            self.messages: list[str] = []

        async def send_progress(self, message: str) -> None:
            self.messages.append(message)

    class _Candidate:
        predecessor_id = None
        offer_start = datetime(2026, 1, 1, 0, 5, 0, tzinfo=timezone.utc)

    async def _fake_find_best_duplicate(*args: object, **kwargs: object) -> tuple[None, float, dict[str, object]]:
        return (None, 0.0, {})

    frozen_now = datetime(2026, 3, 1, 10, 0, 0, tzinfo=timezone.utc)
    now_values = iter([frozen_now, frozen_now + timedelta(minutes=1)])

    class _FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> datetime:
            current = next(now_values)
            if tz is None:
                return current.replace(tzinfo=None)
            return current.astimezone(tz)

    monkeypatch.setattr(matching_main, "datetime", _FrozenDatetime)
    monkeypatch.setattr(matching_main, "_build_candidate", lambda source, doc_id: _Candidate())
    monkeypatch.setattr(matching_main, "find_best_duplicate", _fake_find_best_duplicate)

    reporter = _Reporter()
    state_uow_factory = cast(Callable[[], MatchingStateUnitOfWork], _StateUow)
    processed_ads_repo = cast(ElasticsearchProcessedAdsRepository, _ProcessedAdsRepo())

    asyncio.run(
        matching_main._process_site(
            site_name="avito",
            state_uow_factory=state_uow_factory,
            processed_ads_repo=processed_ads_repo,
            batch_size=500,
            reporter=cast(TelegramReporter, reporter),
            progress_report_interval_minutes=10,
        )
    )

    batch_messages = [msg for msg in reporter.messages if "status=in_progress" in msg]
    assert len(batch_messages) == 1
