"""Raw ad storage adapter backed by MongoDB."""

from __future__ import annotations

from app.clients.mongo_client import MongoClient
from app.schemas.raw import RawAd


def _build_mongo_client(*, mongo_uri: str | None, mongo_db: str | None) -> MongoClient:
    if not mongo_uri:
        raise RuntimeError("MONGO_URI is not set")
    if not mongo_db:
        raise RuntimeError("MONGO_DB is not set")
    return MongoClient(uri=mongo_uri, db_name=mongo_db)


async def save_raw_ads(
    raw_ads: list[RawAd],
    *,
    mongo_uri: str | None,
    mongo_db: str | None,
    raw_collection_name: str | None,
) -> None:
    if not raw_ads:
        return
    if not raw_collection_name:
        raise RuntimeError("RAW_COLLECTION_NAME is not set")
    mongo_client = _build_mongo_client(mongo_uri=mongo_uri, mongo_db=mongo_db)
    collection = mongo_client.get_collection(raw_collection_name)
    docs = [raw_ad.model_dump(mode="json") for raw_ad in raw_ads]
    await collection.insert_many(docs)


async def load_raw_ads(
    *,
    mongo_uri: str | None,
    mongo_db: str | None,
    raw_collection_name: str | None,
    limit: int = 1000,
) -> list[RawAd]:
    if not raw_collection_name:
        raise RuntimeError("RAW_COLLECTION_NAME is not set")
    mongo_client = _build_mongo_client(mongo_uri=mongo_uri, mongo_db=mongo_db)
    collection = mongo_client.get_collection(raw_collection_name)
    cursor = collection.find().limit(limit)
    return [RawAd(**doc) async for doc in cursor if isinstance(doc, dict)]
