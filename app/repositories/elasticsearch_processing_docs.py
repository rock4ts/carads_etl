"""Repository for processing-service writes into Elasticsearch."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

from app.shared.clients.elasticsearch_http import ElasticsearchHttpClient
from app.shared.schemas.processed import CaradDocData

logger = logging.getLogger(__name__)


class ElasticsearchProcessingDocsRepository:
    """Encapsulates processing-stage document writes to Elasticsearch."""

    _protected_fields = {
        "successor_id",
        "predecessor_id",
    }

    def __init__(self, *, client: ElasticsearchHttpClient, index_name: str) -> None:
        self._client = client
        self._index_name = index_name

    @staticmethod
    def _build_doc_id(doc: CaradDocData) -> str:
        return f"[{doc.site_name}]{doc.parapi_unique_id}"

    async def bulk_index_processed_docs(self, docs: Sequence[CaradDocData]) -> list[Mapping[str, Any]]:
        operations: list[dict[str, Any]] = []
        for doc in docs:
            doc_id = self._build_doc_id(doc)
            doc_data = doc.model_dump(mode="json")
            doc_data["updated_at"] = datetime.now(tz=timezone.utc).isoformat()

            update_doc_data = {key: value for key, value in doc_data.items() if key not in self._protected_fields}
            upsert_doc_data = {key: value for key, value in doc_data.items() if key not in self._protected_fields}

            operations.append({"update": {"_index": self._index_name, "_id": doc_id}})
            operations.append(
                {
                    "doc": update_doc_data,
                    "upsert": upsert_doc_data,
                }
            )

        response = await self._client.bulk(operations=operations)
        if not response.get("errors"):
            return []

        failed_items: list[Mapping[str, Any]] = []
        for item in response.get("items", []):
            if not isinstance(item, Mapping):
                continue
            update_item = item.get("update")
            if not isinstance(update_item, Mapping):
                continue
            status = update_item.get("status", 418)  # 418 is a placeholder for unknown status
            try:
                status_code = int(status)
            except (TypeError, ValueError):
                status_code = 500
            if status_code >= 400:
                failed_items.append(update_item)
        return failed_items
