"""MongoDB client wrapper for async service adapters."""

from __future__ import annotations

from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase,
)
from app.repositories.mongo_raw_archive import RawArchiveDocument


class MongoClient:
    """Thin wrapper around Motor client and selected database."""

    def __init__(self, uri: str, db_name: str) -> None:
        self.client: AsyncIOMotorClient[RawArchiveDocument] = AsyncIOMotorClient(host=uri)
        self.db: AsyncIOMotorDatabase[RawArchiveDocument] = self.client[db_name]

    def get_collection(self, name: str) -> AsyncIOMotorCollection[RawArchiveDocument]:
        return self.db[name]
