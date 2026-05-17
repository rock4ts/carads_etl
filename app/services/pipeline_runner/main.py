"""Pipeline orchestrator for ingestion, matching, and archiving."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from app.clients import ElasticsearchHttpClient
from app.services.archiving_service.main import run_archive
from app.services.ingestion_service.main import run_ingestion
from app.services.matching_service.core.config import settings as matching_settings
from app.services.matching_service.main import run_matcher

logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    level = getattr(logging, matching_settings.log_level.strip().upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def _to_naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


async def _refresh_processed_index() -> None:
    es_client = ElasticsearchHttpClient(
        matching_settings.elasticsearch_url,
        api_key=matching_settings.elasticsearch_api_key,
        username=matching_settings.elasticsearch_username,
        password=matching_settings.elasticsearch_password,
    )
    await es_client.refresh_index(index=matching_settings.processed_index)


async def run_pipeline() -> None:
    started_at = datetime.now(timezone.utc)
    load_till = _to_naive_utc(started_at)
    status = "success"
    logger.info("Pipeline started at %s", started_at.isoformat())
    logger.info("Pipeline load_till fixed at %s", load_till.isoformat())

    try:
        try:
            await run_ingestion(load_till=load_till)
        except Exception:
            status = "failed"
            logger.exception("Pipeline stage failed: ingestion")
            return
        logger.info("Ingestion completed")

        try:
            await _refresh_processed_index()
        except Exception:
            status = "failed"
            logger.exception(
                "Pipeline stage failed: refresh index=%s",
                matching_settings.processed_index,
            )
            return
        logger.info("Elasticsearch refresh completed for index=%s", matching_settings.processed_index)

        try:
            await run_matcher()
        except Exception:
            status = "failed"
            logger.exception("Pipeline stage failed: matcher")
            return
        logger.info("Matcher completed")

        try:
            await run_archive()
        except Exception:
            status = "failed"
            logger.exception("Pipeline stage failed: archive")
            return
        logger.info("Archive completed")
    finally:
        duration_seconds = (datetime.now(timezone.utc) - started_at).total_seconds()
        logger.info("Pipeline finished status=%s duration_seconds=%.3f", status, duration_seconds)


async def main() -> None:
    await run_pipeline()


def cli_main() -> None:
    _configure_logging()
    asyncio.run(main())


if __name__ == "__main__":
    cli_main()
