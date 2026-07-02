from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database.models import EtlStateBase
from scripts import backfill_matcher


def test_build_reset_operations_clears_is_duplicate_and_links() -> None:
    operations = backfill_matcher._build_reset_operations(
        index_name="processed",
        doc_ids=["doc-1"],
    )

    assert len(operations) == 2
    assert operations[0] == {"update": {"_index": "processed", "_id": "doc-1"}}
    script = operations[1]["script"]["source"]
    assert "predecessor_id = null" in script
    assert "successor_id = null" in script
    assert "is_duplicate = null" in script


def test_build_site_candidates_query_uses_backfill_start() -> None:
    month_end = datetime(2024, 8, 1, 0, 0, 0, tzinfo=timezone.utc)
    query = backfill_matcher._build_site_candidates_query(
        site_name="avito",
        lower_bound=backfill_matcher.BACKFILL_START,
        upper_bound=month_end,
    )

    assert query["bool"]["filter"][0] == {"term": {"site_name.keyword": "avito"}}
    assert query["bool"]["filter"][1]["range"]["offer_start"]["gte"] == backfill_matcher.BACKFILL_START.isoformat()
    assert query["bool"]["filter"][1]["range"]["offer_start"]["lt"] == month_end.isoformat()


def test_process_site_links_on_successful_claim(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Candidate:
        offer_start = datetime(2024, 7, 11, 0, 0, 0, tzinfo=timezone.utc)

    class _ProcessedAdsRepo:
        def __init__(self) -> None:
            self.search_calls: list[dict[str, Any]] = []

        async def search_window(self, **kwargs: Any) -> list[dict[str, Any]]:
            self.search_calls.append(kwargs)
            if len(self.search_calls) == 1:
                return [
                    {
                        "_id": "new-1",
                        "_source": {"offer_start": "2024-07-11T00:00:00+00:00", "site_name": "avito"},
                        "sort": ["2024-07-11T00:00:00+00:00", "new-1"],
                    }
                ]
            return []

        async def claim_duplicate(self, *, duplicate_id: str, candidate_id: str) -> bool:
            return duplicate_id == "old-1" and candidate_id == "new-1"

    class _Client:
        def __init__(self) -> None:
            self.bulk_calls = 0
            self.last_operations: list[dict[str, Any]] = []

        async def bulk(self, *, operations: list[dict[str, Any]], refresh: bool) -> dict[str, Any]:
            self.bulk_calls += 1
            self.last_operations = operations
            return {"errors": False, "items": [{"update": {"result": "updated"}}]}

        async def update(self, **kwargs: Any) -> dict[str, Any]:
            raise AssertionError("Rollback should not run for successful predecessor updates")

    monkeypatch.setattr(backfill_matcher, "_build_candidate_from_source", lambda source, doc_id: _Candidate())

    async def _fake_find_best_duplicate(*args: Any, **kwargs: Any) -> tuple[str | None, float, dict[str, Any]]:
        return "old-1", 0.99, {"offer_start": datetime(2024, 7, 10, 0, 0, 0, tzinfo=timezone.utc)}

    monkeypatch.setattr(backfill_matcher, "find_best_duplicate", _fake_find_best_duplicate)

    repo = _ProcessedAdsRepo()
    client = _Client()
    stats = asyncio.run(
        backfill_matcher._process_site(
            site_name="avito",
            processed_ads_repo=repo,  # type: ignore[arg-type]
            client=client,  # type: ignore[arg-type]
            index_name="processed",
            batch_size=500,
            month_start=backfill_matcher.BACKFILL_START,
            month_end=datetime(2024, 8, 1, 0, 0, 0, tzinfo=timezone.utc),
        )
    )

    assert stats.processed == 1
    assert stats.matches_found == 1
    assert stats.linked == 1
    assert stats.claim_conflicts == 0
    assert client.bulk_calls == 1
    assert repo.search_calls[0]["query"]["bool"]["filter"][1]["range"]["offer_start"]["gte"] == (
        backfill_matcher.BACKFILL_START.isoformat()
    )
    assert repo.search_calls[0]["query"]["bool"]["filter"][1]["range"]["offer_start"]["lt"] == datetime(
        2024, 8, 1, 0, 0, 0, tzinfo=timezone.utc
    ).isoformat()
    assert repo.search_calls[1]["search_after"] == ["2024-07-11T00:00:00+00:00", "new-1"]
    assert client.last_operations[0] == {"update": {"_index": "processed", "_id": "new-1"}}


def test_process_site_skips_predecessor_update_when_claim_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Candidate:
        offer_start = datetime(2024, 7, 11, 0, 0, 0, tzinfo=timezone.utc)

    class _ProcessedAdsRepo:
        async def search_window(self, **kwargs: Any) -> list[dict[str, Any]]:
            if kwargs.get("search_after") is None:
                return [
                    {
                        "_id": "new-1",
                        "_source": {"offer_start": "2024-07-11T00:00:00+00:00", "site_name": "avito"},
                        "sort": ["2024-07-11T00:00:00+00:00", "new-1"],
                    }
                ]
            return []

        async def claim_duplicate(self, *, duplicate_id: str, candidate_id: str) -> bool:
            return False

    class _Client:
        def __init__(self) -> None:
            self.bulk_calls = 0

        async def bulk(self, *, operations: list[dict[str, Any]], refresh: bool) -> dict[str, Any]:
            self.bulk_calls += 1
            return {"errors": False, "items": []}

        async def update(self, **kwargs: Any) -> dict[str, Any]:
            return {"result": "noop"}

    monkeypatch.setattr(backfill_matcher, "_build_candidate_from_source", lambda source, doc_id: _Candidate())

    async def _fake_find_best_duplicate(*args: Any, **kwargs: Any) -> tuple[str | None, float, dict[str, Any]]:
        return "old-1", 0.99, {"offer_start": datetime(2024, 7, 10, 0, 0, 0, tzinfo=timezone.utc)}

    monkeypatch.setattr(backfill_matcher, "find_best_duplicate", _fake_find_best_duplicate)

    client = _Client()
    stats = asyncio.run(
        backfill_matcher._process_site(
            site_name="avito",
            processed_ads_repo=_ProcessedAdsRepo(),  # type: ignore[arg-type]
            client=client,  # type: ignore[arg-type]
            index_name="processed",
            batch_size=500,
            month_start=backfill_matcher.BACKFILL_START,
            month_end=datetime(2024, 8, 1, 0, 0, 0, tzinfo=timezone.utc),
        )
    )

    assert stats.processed == 1
    assert stats.matches_found == 1
    assert stats.linked == 0
    assert stats.claim_conflicts == 1
    assert client.bulk_calls == 0


def test_next_month_boundary_from_mid_month_start() -> None:
    next_boundary = backfill_matcher._month_window_end(backfill_matcher.BACKFILL_START)
    assert next_boundary == datetime(2024, 8, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_state_store_persists_month_checkpoint() -> None:
    def _as_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    engine = create_engine("sqlite:///:memory:")
    EtlStateBase.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    state_store = backfill_matcher.BackfillStateStore(session_factory=factory)

    initial = state_store.get_or_create(site_name="avito")
    assert _as_utc(initial.next_from) == backfill_matcher.BACKFILL_START
    assert initial.reset_completed is False

    after_reset = state_store.mark_reset_completed(site_name="avito")
    assert after_reset.reset_completed is True
    assert _as_utc(after_reset.next_from) == backfill_matcher.BACKFILL_START

    checkpoint_at = datetime(2024, 8, 1, 0, 0, 0, tzinfo=timezone.utc)
    checkpoint = state_store.mark_month_finished(site_name="avito", next_from=checkpoint_at)
    assert checkpoint.reset_completed is True
    assert _as_utc(checkpoint.next_from) == checkpoint_at


def test_run_backfill_reports_after_month_checkpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    messages: list[str] = []

    class _FakeReporter:
        @classmethod
        def from_settings(cls, **kwargs: Any) -> "_FakeReporter":
            return cls()

        async def send_progress(self, message: str) -> None:
            messages.append(message)

    class _FakeStateStore:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self._state = backfill_matcher.SiteProgressState(
                site="avito",
                next_from=backfill_matcher.BACKFILL_START,
                reset_completed=False,
            )

        def get_or_create(self, *, site_name: str) -> backfill_matcher.SiteProgressState:
            return self._state

        def mark_reset_completed(self, *, site_name: str) -> backfill_matcher.SiteProgressState:
            self._state = backfill_matcher.SiteProgressState(
                site=site_name,
                next_from=self._state.next_from,
                reset_completed=True,
            )
            return self._state

        def mark_month_finished(self, *, site_name: str, next_from: datetime) -> backfill_matcher.SiteProgressState:
            self._state = backfill_matcher.SiteProgressState(
                site=site_name,
                next_from=next_from,
                reset_completed=True,
            )
            return self._state

    class _FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            current = datetime(2024, 9, 15, 0, 0, 0, tzinfo=timezone.utc)
            if tz is None:
                return current.replace(tzinfo=None)
            return current.astimezone(tz)

    class _FakeClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            return

        async def refresh_index(self, *, index: str) -> dict[str, Any]:
            return {"ok": True}

    class _FakeRepo:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            return

        async def ensure_mapping(self, *, body: dict[str, Any]) -> None:
            return

    async def _fake_iter_site_names(**kwargs: Any) -> list[str]:
        return ["avito"]

    async def _fake_reset_site(**kwargs: Any) -> int:
        return 10

    async def _fake_process_site(**kwargs: Any) -> backfill_matcher.SiteStats:
        return backfill_matcher.SiteStats(processed=2, matches_found=1, linked=1, claim_conflicts=0)

    async def _fake_validate(**kwargs: Any) -> None:
        return

    monkeypatch.setattr(backfill_matcher, "datetime", _FrozenDatetime)
    monkeypatch.setattr(backfill_matcher, "TelegramReporter", _FakeReporter)
    monkeypatch.setattr(backfill_matcher, "ensure_etl_state_tables", lambda _: None)
    monkeypatch.setattr(backfill_matcher, "build_postgres_session_factory", lambda _: object())
    monkeypatch.setattr(backfill_matcher, "BackfillStateStore", _FakeStateStore)
    monkeypatch.setattr(backfill_matcher, "ElasticsearchHttpClient", _FakeClient)
    monkeypatch.setattr(backfill_matcher, "ElasticsearchProcessedAdsRepository", _FakeRepo)
    monkeypatch.setattr(backfill_matcher, "configure_matcher", lambda **kwargs: None)
    monkeypatch.setattr(backfill_matcher, "_iter_site_names", _fake_iter_site_names)
    monkeypatch.setattr(backfill_matcher, "_reset_matching_state_for_site", _fake_reset_site)
    monkeypatch.setattr(backfill_matcher, "_process_site", _fake_process_site)
    monkeypatch.setattr(backfill_matcher, "_validate_chains", _fake_validate)

    asyncio.run(backfill_matcher.run_backfill())

    assert any("status=month_finished" in message for message in messages)


def test_validate_chains_raises_on_inconsistent_links() -> None:
    class _Client:
        def __init__(self) -> None:
            self.calls = 0

        async def search(self, **kwargs: Any) -> dict[str, Any]:
            self.calls += 1
            if self.calls == 1:
                return {
                    "hits": {
                        "hits": [
                            {
                                "_id": "A",
                                "_source": {"successor_id": "B", "predecessor_id": None},
                                "sort": ["A"],
                            },
                            {
                                "_id": "B",
                                "_source": {"successor_id": None, "predecessor_id": "C"},
                                "sort": ["B"],
                            },
                            {
                                "_id": "C",
                                "_source": {"successor_id": None, "predecessor_id": None},
                                "sort": ["C"],
                            },
                        ]
                    }
                }
            return {"hits": {"hits": []}}

    with pytest.raises(RuntimeError, match="validation failed"):
        asyncio.run(
            backfill_matcher._validate_chains(
                client=_Client(),  # type: ignore[arg-type]
                index_name="processed",
                batch_size=500,
            )
        )


def test_validate_chains_raises_on_cycles() -> None:
    class _Client:
        def __init__(self) -> None:
            self.calls = 0

        async def search(self, **kwargs: Any) -> dict[str, Any]:
            self.calls += 1
            if self.calls == 1:
                return {
                    "hits": {
                        "hits": [
                            {
                                "_id": "A",
                                "_source": {"successor_id": "B", "predecessor_id": "B"},
                                "sort": ["A"],
                            },
                            {
                                "_id": "B",
                                "_source": {"successor_id": "A", "predecessor_id": "A"},
                                "sort": ["B"],
                            },
                        ]
                    }
                }
            return {"hits": {"hits": []}}

    with pytest.raises(RuntimeError, match="Cycle detected"):
        asyncio.run(
            backfill_matcher._validate_chains(
                client=_Client(),  # type: ignore[arg-type]
                index_name="processed",
                batch_size=500,
            )
        )
