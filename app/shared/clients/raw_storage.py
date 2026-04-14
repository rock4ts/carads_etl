"""Temporary raw ad storage adapter."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from app.shared.schemas.raw import RawAd

RAW_ADS_PATH = Path("/tmp/raw_ads.json")


def _write_raw_ads_sync(raw_ads: list[RawAd]) -> None:
    RAW_ADS_PATH.parent.mkdir(parents=True, exist_ok=True)
    serialized = [raw_ad.model_dump(mode="json") for raw_ad in raw_ads]
    RAW_ADS_PATH.write_text(
        json.dumps(serialized, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _read_raw_ads_sync() -> list[RawAd]:
    if not RAW_ADS_PATH.exists():
        return []

    payload = json.loads(RAW_ADS_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        return []

    validated_ads: list[RawAd] = []
    for item in payload:
        if isinstance(item, dict):
            validated_ads.append(RawAd.model_validate(item))
    return validated_ads


async def save_raw_ads(raw_ads: list[RawAd]) -> None:
    await asyncio.to_thread(_write_raw_ads_sync, raw_ads)


async def load_raw_ads() -> list[RawAd]:
    return await asyncio.to_thread(_read_raw_ads_sync)
