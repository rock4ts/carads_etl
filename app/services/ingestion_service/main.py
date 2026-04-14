"""Ingestion service entrypoint."""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

from app.shared.clients.raw_storage import save_raw_ads
from app.shared.schemas.raw import RawAd


async def fetch_raw_ads() -> list[dict[str, Any]]:
    """Temporary parser stub used for local pipeline runs."""
    return [
        {
            "id": "stub-avito-1",
            "unique_id": "100001",
            "url": "https://www.avito.ru/stub-avito-1",
            "name": "Toyota Camry",
            "mark": "Toyota",
            "model": "Camry",
            "price": "1200000",
            "year": "2018",
            "run": "95000",
            "checked": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "parsed": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "added": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "actual": "1",
        }
    ]


async def main() -> None:
    raw_payloads = await fetch_raw_ads()
    raw_ads = [
        RawAd(
            source="avito",
            ingested_at=datetime.now(),
            payload=payload,
        )
        for payload in raw_payloads
    ]
    await save_raw_ads(raw_ads)


if __name__ == "__main__":
    asyncio.run(main())
