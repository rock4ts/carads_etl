"""Shared external service clients."""

from app.shared.clients.elasticsearch_http import ElasticsearchHttpClient
from app.shared.clients.mongo_client import MongoClient
from app.shared.clients.processed_storage import save_processed_docs
from app.shared.clients.raw_storage import load_raw_ads, save_raw_ads

__all__ = ["ElasticsearchHttpClient", "MongoClient", "load_raw_ads", "save_raw_ads", "save_processed_docs"]
