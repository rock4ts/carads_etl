"""MongoDB repository for raw-ad archive reads and deletes."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timezone

from motor.motor_asyncio import AsyncIOMotorCollection

RawArchiveDocument = dict[str, object]


def _build_cutoff_filter(cutoff: datetime) -> dict[str, object]:
    cutoff_utc = (
        cutoff
        if cutoff.tzinfo is None
        else cutoff.astimezone(timezone.utc).replace(tzinfo=None)
    )
    cutoff_strings = {
        cutoff_utc.isoformat(),
        cutoff_utc.isoformat() + "Z",
        cutoff_utc.replace(tzinfo=timezone.utc).isoformat(),
    }
    if cutoff.tzinfo is None:
        cutoff_strings.add(cutoff.isoformat())
    else:
        cutoff_strings.add(cutoff.isoformat())
    return {
        "$or": [
            {"ingested_at": {"$lt": cutoff}},
            *(
                {"ingested_at": {"$lt": cutoff_string}}
                for cutoff_string in sorted(cutoff_strings)
            ),
        ]
    }


class MongoRawArchiveRepository:
    """Stream retention-expired raw ads from MongoDB in bounded chunks."""

    _collection: AsyncIOMotorCollection[RawArchiveDocument]

    def __init__(self, collection: AsyncIOMotorCollection[RawArchiveDocument]) -> None:
        self._collection = collection

    async def iter_expired_batches(
        self,
        *,
        cutoff: datetime,
        batch_size: int,
        cursor_batch_size: int = 1000,
    ) -> AsyncIterator[list[RawArchiveDocument]]:
        cursor = self._collection.find(
            _build_cutoff_filter(cutoff),
            sort=[("ingested_at", 1)],
            batch_size=cursor_batch_size,
        )
        batch: list[RawArchiveDocument] = []
        async for document in cursor:
            if not isinstance(document, dict):
                continue
            batch.append(document)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    async def delete_by_ids(self, document_ids: list[object]) -> int:
        if not document_ids:
            return 0
        result = await self._collection.delete_many({"_id": {"$in": document_ids}})
        return int(result.deleted_count)
