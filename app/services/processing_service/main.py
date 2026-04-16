"""Processing service entrypoint."""

from __future__ import annotations

import asyncio
import logging

from app.services.processing_service.core.config import settings

logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    level = getattr(logging, settings.log_level.strip().upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


async def main() -> None:
    logger.info("Processing service is reserved for optional future batch processing")


if __name__ == "__main__":
    _configure_logging()
    asyncio.run(main())
