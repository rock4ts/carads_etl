"""External service clients."""

from app.clients.elasticsearch_http import ElasticsearchHttpClient
from app.clients.mongo_client import MongoClient
from app.clients.processed_storage import save_processed_docs
from app.clients.raw_storage import load_raw_ads, save_raw_ads

__all__ = ["ElasticsearchHttpClient", "MongoClient", "load_raw_ads", "save_raw_ads", "save_processed_docs"]
