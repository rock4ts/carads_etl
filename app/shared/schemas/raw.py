"""Raw ingestion schemas for parser API ads."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class RawAd(BaseModel):
    """Raw ad payload enriched only with ingestion metadata.
    Parser API fields are intentionally left untouched and are accepted as
    extra top-level fields so we can preserve the original payload exactly as
    received.
    """

    model_config = ConfigDict(extra="allow")
    source: str
    ingested_at: datetime
    request_params: dict[str, Any] = Field(default_factory=dict)
    payload: dict[str, Any]
