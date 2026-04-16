"""Ingestion service entrypoint."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from app.services.ingestion_service.core.config import settings
from app.services.processing_service.mapper import map_raw_to_processed
from app.shared.clients.processed_storage import save_processed_docs
from app.shared.clients.raw_storage import save_raw_ads
from app.shared.schemas.raw import RawAd

logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    level = getattr(logging, settings.log_level.strip().upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


async def fetch_raw_ads() -> list[dict[str, Any]]:
    """Temporary parser stub used for local pipeline runs."""
    now = datetime.now(timezone.utc)
    return [
        {
            "id": "stub-avito-1",
            "unique_id": "100001",
            "url": "https://www.avito.ru/stub-avito-1",
            "name": "Toyota Camry",
            "mark": "Toyota",
            "model": "Camry",
            "price": "1200000",
            "year": "2018",
            "run": "95000",
            "checked": now.strftime("%Y-%m-%d %H:%M:%S"),
            "parsed": now.strftime("%Y-%m-%d %H:%M:%S"),
            "added": now.strftime("%Y-%m-%d %H:%M:%S"),
            "actual": "1",
        }
    ]


async def main() -> None:
    raw_payloads = await fetch_raw_ads()
    for payload in raw_payloads:
        raw = RawAd(
            source="avito",
            ingested_at=datetime.now(timezone.utc),
            payload=payload,
        )
        await save_raw_ads(
            [raw],
            mongo_uri=settings.mongo_uri,
            mongo_db=settings.mongo_db,
            raw_collection_name=settings.raw_collection_name,
        )

        try:
            doc = map_raw_to_processed(raw)
        except Exception:
            logger.exception("Mapping failed")
            continue

        await save_processed_docs(
            [doc],
            elasticsearch_url=settings.elasticsearch_url,
            elasticsearch_api_key=settings.elasticsearch_api_key,
            elasticsearch_username=settings.elasticsearch_username,
            elasticsearch_password=settings.elasticsearch_password,
        )
        doc_id = f"[{doc.site_name}]{doc.parapi_unique_id}"
        logger.info("Processed ad %s", doc_id)


if __name__ == "__main__":
    _configure_logging()
    asyncio.run(main())
