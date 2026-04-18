"""Processed docs storage adapter for Elasticsearch."""

from __future__ import annotations

import logging
from collections.abc import Sequence

from app.repositories.elasticsearch_processing_docs import ElasticsearchProcessingDocsRepository
from app.clients.elasticsearch_http import ElasticsearchHttpClient
from app.schemas.processed import CaradDocData

logger = logging.getLogger(__name__)
PROCESSED_INDEX_NAME = "carads1_local"

async def save_processed_docs(
    docs: Sequence[CaradDocData],
    *,
    elasticsearch_url: str | None,
    elasticsearch_api_key: str | None = None,
    elasticsearch_username: str | None = None,
    elasticsearch_password: str | None = None,
) -> None:
    if not docs:
        logger.info("No processed docs to index")
        return
    if not elasticsearch_url:
        raise RuntimeError("ELASTICSEARCH_URL is not set")

    elasticsearch_client = ElasticsearchHttpClient(
        elasticsearch_url,
        api_key=elasticsearch_api_key,
        username=elasticsearch_username,
        password=elasticsearch_password,
    )
    repository = ElasticsearchProcessingDocsRepository(
        client=elasticsearch_client,
        index_name=PROCESSED_INDEX_NAME,
    )
    failed_items = await repository.bulk_index_processed_docs(docs)
    if not failed_items:
        logger.info("Indexed %s processed docs into %s", len(docs), PROCESSED_INDEX_NAME)
        return

    logger.error("Bulk indexing completed with %s failed items", len(failed_items))
    for failed_item in failed_items:
        logger.error(
            "Failed bulk item _id=%s status=%s error=%s",
            failed_item.get("_id"),
            failed_item.get("status"),
            failed_item.get("error"),
        )
