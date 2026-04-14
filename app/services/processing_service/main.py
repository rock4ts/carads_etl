"""Processing service entrypoint."""

from __future__ import annotations

import asyncio
import logging

from app.services.processing_service.mapper import map_raw_to_processed
from app.shared.clients.processed_storage import save_processed_docs
from app.shared.clients.raw_storage import load_raw_ads

logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


async def main() -> None:
    raw_ads = await load_raw_ads()
    processed_docs = []

    for raw in raw_ads:
        try:
            doc = map_raw_to_processed(raw)
            processed_docs.append(doc)
        except Exception:
            logger.exception("Mapping failed for raw ad: %s", raw.model_dump(mode="json"))

    await save_processed_docs(processed_docs)


if __name__ == "__main__":
    _configure_logging()
    asyncio.run(main())
