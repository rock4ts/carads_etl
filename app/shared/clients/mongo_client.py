"""MongoDB client wrapper for async service adapters."""

from __future__ import annotations

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection, AsyncIOMotorDatabase


class MongoClient:
    """Thin wrapper around Motor client and selected database."""

    def __init__(self, uri: str, db_name: str):
        self.client = AsyncIOMotorClient(uri)
        self.db: AsyncIOMotorDatabase = self.client[db_name]

    def get_collection(self, name: str) -> AsyncIOMotorCollection:
        return self.db[name]
