"""Repository for processing-service writes into Elasticsearch."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from app.shared.clients.elasticsearch_http import ElasticsearchHttpClient
from app.shared.schemas.processed import CaradDocData


class ElasticsearchProcessingDocsRepository:
    """Encapsulates processing-stage document writes to Elasticsearch."""

    def __init__(self, *, client: ElasticsearchHttpClient, index_name: str) -> None:
        self._client = client
        self._index_name = index_name

    @staticmethod
    def _build_doc_id(doc: CaradDocData) -> str:
        return f"[{doc.site_name}]{doc.parapi_unique_id}"

    async def bulk_index_processed_docs(self, docs: Sequence[CaradDocData]) -> list[Mapping[str, Any]]:
        operations: list[dict[str, Any]] = []
        for doc in docs:
            operations.append({"index": {"_index": self._index_name, "_id": self._build_doc_id(doc)}})
            operations.append(doc.model_dump(mode="json"))

        response = await self._client.bulk(operations=operations)
        if not response.get("errors"):
            return []

        failed_items: list[Mapping[str, Any]] = []
        for item in response.get("items", []):
            if not isinstance(item, Mapping):
                continue
            index_item = item.get("index")
            if not isinstance(index_item, Mapping):
                continue
            status = index_item.get("status", 418)  # 418 is a placeholder for unknown status
            try:
                status_code = int(status)
            except (TypeError, ValueError):
                status_code = 500
            if status_code >= 400:
                failed_items.append(index_item)
        return failed_items
