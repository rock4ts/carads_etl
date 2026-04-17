from __future__ import annotations

from datetime import datetime

from app.services.ingestion_service.core.config import IngestionServiceSettings

T0 = datetime(2026, 1, 1, 0, 0, 0)
T1 = datetime(2026, 1, 1, 0, 0, 5)
T2 = datetime(2026, 1, 1, 0, 0, 10)
T3 = datetime(2026, 1, 1, 0, 0, 15)
LOAD_TILL = datetime(2026, 1, 1, 0, 5, 0)


def build_settings() -> IngestionServiceSettings:
    return IngestionServiceSettings(
        PARSER_API_URL="https://example.test/parser",
        PARSER_API_KEY="test-key",
        MONGO_URI="mongodb://unused",
        MONGO_DB="etl",
        RAW_COLLECTION_NAME="raw_ads",
        ELASTICSEARCH_URL="http://unused:9200",
        ELASTICSEARCH_API_KEY="api-key",
        ELASTICSEARCH_USERNAME="elastic",
        ELASTICSEARCH_PASSWORD="password",
        POSTGRES_DATABASE_URL="postgresql://unused",
    )


def build_ad(*, checked: datetime, unique_id: int, site_id: int = 2) -> dict[str, object]:
    checked_str = checked.strftime("%Y-%m-%d %H:%M:%S")
    return {
        "checked": checked_str,
        "parsed": checked_str,
        "added": checked_str,
        "unique_id": unique_id,
        "id": f"source-{unique_id}",
        "site_id": site_id,
        "url": f"https://example.test/ads/{unique_id}",
        "name": f"Test car {unique_id}",
        "price": 1000000 + unique_id,
        "actual": 1,
    }
