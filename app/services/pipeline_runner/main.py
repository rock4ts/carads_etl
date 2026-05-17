"""Pipeline orchestrator for ingestion, matching, and archiving."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from app.clients import ElasticsearchHttpClient
from app.services.archiving_service.main import run_archive
from app.services.ingestion_service.main import run_ingestion
from app.services.pipeline_runner.core.config import settings
from app.services.matching_service.main import run_matcher
from app.services.telegram_notifier import TelegramReporter

logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    level = getattr(logging, settings.log_level.strip().upper(), logging.INFO)
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
        settings.elasticsearch_url,
        api_key=settings.elasticsearch_api_key,
        username=settings.elasticsearch_username,
        password=settings.elasticsearch_password,
    )
    _ = await es_client.refresh_index(index=settings.processed_index)


async def run_pipeline() -> None:
    started_at = datetime.now(timezone.utc)
    load_till = _to_naive_utc(started_at)
    status = "success"
    reporter = TelegramReporter.from_settings(
        service_name="pipeline_runner",
        settings=settings,
        logger=logger,
    )

    msg = f"Pipeline started | load_till={load_till.isoformat()}"
    logger.info(msg)
    await reporter.send_progress(msg)

    try:
        try:
            await run_ingestion(load_till=load_till)
        except Exception as error:
            status = "failed"
            msg = f"Ingestion failed: {error}"
            logger.exception(msg)
            await reporter.send_critical(msg)
            return
        msg = "Ingestion completed"
        logger.info(msg)
        await reporter.send_progress(msg)

        try:
            await _refresh_processed_index()
        except Exception:
            status = "failed"
            logger.exception(
                "Pipeline stage failed: refresh index=%s",
                settings.processed_index,
            )
            return
        logger.info("Elasticsearch refreshed")

        try:
            await run_matcher()
        except Exception as error:
            status = "failed"
            msg = f"Matcher failed: {error}"
            logger.exception(msg)
            await reporter.send_critical(msg)
            return
        msg = "Matcher completed"
        logger.info(msg)
        await reporter.send_progress(msg)

        try:
            await run_archive()
        except Exception as error:
            status = "failed"
            msg = f"Archive failed: {error}"
            logger.exception(msg)
            await reporter.send_critical(msg)
            return
        msg = "Archive completed"
        logger.info(msg)
        await reporter.send_progress(msg)

        duration_seconds = (datetime.now(timezone.utc) - started_at).total_seconds()
        msg = (
            "Pipeline finished successfully | "
            f"duration={duration_seconds:.0f}s | load_till={load_till.isoformat()}"
        )
        logger.info(msg)
        await reporter.send_progress(msg)
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
