"""Repository for processed-ad read/write operations in Elasticsearch."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any

from app.shared.clients import ElasticsearchHttpClient


class ElasticsearchProcessedAdsRepository:
    """Encapsulates matching-related Elasticsearch access."""

    def __init__(self, *, client: ElasticsearchHttpClient, index_name: str) -> None:
        self._client = client
        self._index_name = index_name

    async def search(self, **kwargs: Any) -> Mapping[str, Any]:
        return await self._client.search(**kwargs)

    async def ensure_mapping(self, *, body: dict[str, Any]) -> None:
        await self._client.put_mapping(index=self._index_name, body=body)

    async def search_window(
        self,
        *,
        query: dict[str, Any],
        batch_size: int,
        search_after: list[Any] | None,
    ) -> list[Mapping[str, Any]]:
        response = await self._client.search(
            index=self._index_name,
            query=query,
            size=batch_size,
            sort=[
                {"offer_start": {"order": "asc"}},
                {"_id": {"order": "asc"}},
            ],
            source=True,
            search_after=search_after,
        )
        hits = response.get("hits", {}).get("hits", [])
        if not isinstance(hits, list):
            return []
        return [hit for hit in hits if isinstance(hit, Mapping)]

    async def claim_duplicate(self, *, duplicate_id: str, candidate_id: str) -> bool:
        response = await self._client.update(
            index=self._index_name,
            doc_id=duplicate_id,
            body={
                "script": {
                    "source": """
                        if (ctx._source.successor_id == null) {
                            ctx._source.successor_id = params.new_id;
                        } else {
                            ctx.op = 'none';
                        }
                    """,
                    "params": {
                        "new_id": candidate_id,
                    },
                }
            },
            refresh=True,
        )
        return response.get("result") == "updated"

    async def link_predecessors(self, *, links: Sequence[tuple[str, str]]) -> int:
        if not links:
            return 0

        operations: list[dict[str, Any]] = []
        for candidate_id, duplicate_id in links:
            operations.append({"update": {"_index": self._index_name, "_id": candidate_id}})
            operations.append({"doc": {"predecessor_id": duplicate_id}})

        response = await self._client.bulk(
            operations=operations,
            refresh=True,
        )
        if not response.get("errors"):
            return len(links)

        successful_updates = 0
        items = response.get("items", [])
        if not isinstance(items, list):
            raise RuntimeError("Bulk predecessor update failed with malformed response.")
        for item in items:
            if not isinstance(item, dict):
                continue
            action = item.get("update")
            if not isinstance(action, dict):
                continue
            if "error" in action:
                raise RuntimeError(f"Bulk predecessor update failed: {json.dumps(action['error'])}")
            successful_updates += 1
        return successful_updates
