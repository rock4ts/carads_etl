"""Temporary processed docs storage adapter."""

from __future__ import annotations

import logging
from collections.abc import Sequence

from app.shared.schemas.processed import CaradDocData

logger = logging.getLogger(__name__)


async def save_processed_docs(docs: Sequence[CaradDocData]) -> None:
    logger.info("Processed docs count: %s", len(docs))
