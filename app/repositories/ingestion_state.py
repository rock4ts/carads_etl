"""Repository contract and Postgres implementation for ingestion state storage."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.database.models import UploadTimestamp


class IngestionStateRepository(Protocol):
    """Access upload timestamps used by the ingestion worker."""

    def list_upload_sites(self) -> Sequence[str]:
        """Return site names that have upload timestamps."""
        ...

    def get_upload_timestamp(self, site_name: str) -> datetime | None:
        """Return upload timestamp for a site, if present."""
        ...

    def upsert_upload_timestamp(self, site_name: str, timestamp: datetime) -> None:
        """Insert or update upload timestamp for a site."""
        ...


class PostgresIngestionStateRepository(IngestionStateRepository):
    """SQLAlchemy-backed repository for ingestion state storage."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def list_upload_sites(self) -> Sequence[str]:
        return self._session.scalars(select(UploadTimestamp.site).order_by(UploadTimestamp.site)).all()

    def get_upload_timestamp(self, site_name: str) -> datetime | None:
        row = self._session.get(UploadTimestamp, site_name)
        return row.timestamp if row is not None else None

    def upsert_upload_timestamp(self, site_name: str, timestamp: datetime) -> None:
        row = self._session.get(UploadTimestamp, site_name)
        if row is None:
            self._session.add(UploadTimestamp(site=site_name, timestamp=timestamp))
            return
        row.timestamp = timestamp
        self._session.add(row)
