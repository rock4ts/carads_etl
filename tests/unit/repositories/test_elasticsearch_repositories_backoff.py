from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest

from app.clients.elasticsearch_http import ElasticsearchHttpClient
from app.repositories.elasticsearch_processed_ads import ElasticsearchProcessedAdsRepository
from app.repositories.elasticsearch_processing_docs import ElasticsearchProcessingDocsRepository


@pytest.fixture(autouse=True)
def _disable_backoff_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("backoff._sync.time.sleep", lambda _: None)


class _ProcessedAdsClient:
    def __init__(self) -> None:
        self.bulk_calls = 0

    async def bulk(self, *, operations: list[dict[str, Any]], refresh: bool) -> dict[str, Any]:
        self.bulk_calls += 1
        if self.bulk_calls == 1:
            raise TimeoutError("temporary timeout")
        return {"errors": False, "items": []}


def test_processed_ads_repo_retries_client_bulk_call() -> None:
    client = _ProcessedAdsClient()
    notifications: list[str] = []
    repo = ElasticsearchProcessedAdsRepository(
        client=cast(ElasticsearchHttpClient, client),
        index_name="carads",
        on_backoff_notify=notifications.append,
    )

    updated = asyncio.run(repo.link_predecessors(links=[("cand-1", "dup-1")]))

    assert updated == 1
    assert client.bulk_calls == 2
    assert notifications
    assert "operation=processed_ads.link_predecessors" in notifications[0]


class _ProcessingDocsClient:
    def __init__(self) -> None:
        self.bulk_calls = 0

    async def bulk(self, *, operations: list[dict[str, Any]]) -> dict[str, Any]:
        self.bulk_calls += 1
        if self.bulk_calls == 1:
            raise TimeoutError("temporary timeout")
        return {"errors": False, "items": []}


class _FakeDoc:
    site_name = "avito"
    parapi_unique_id = "42"

    def model_dump(self, *, mode: str) -> dict[str, object]:
        return {
            "site_name": self.site_name,
            "parapi_unique_id": self.parapi_unique_id,
            "original_id": "42",
            "successor_id": None,
            "predecessor_id": None,
        }


def test_processing_docs_repo_retries_client_bulk_call() -> None:
    client = _ProcessingDocsClient()
    repo = ElasticsearchProcessingDocsRepository(
        client=cast(ElasticsearchHttpClient, client),
        index_name="carads",
    )

    failed = asyncio.run(repo.bulk_index_processed_docs([_FakeDoc()]))  # type: ignore[list-item]

    assert failed == []
    assert client.bulk_calls == 2
