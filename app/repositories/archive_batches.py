"""Repository contract and Postgres implementation for archive batch metadata."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, cast, override

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from app.database.models import ArchiveBatch, ArchiveBatchStatus

ARCHIVE_WORKER_LOCK_KEY = 830410001


class ArchiveBatchRepository(Protocol):
    """Access archive batch metadata and archive worker locks."""

    def try_acquire_worker_lock(self) -> bool:
        """Acquire the archive worker lock if it is available."""
        ...

    def release_worker_lock(self) -> None:
        """Release the archive worker lock."""
        ...

    def get_last_successful_to_ingested_at(self) -> datetime | None:
        """Return the latest successful archive high-water mark."""
        ...

    def create_in_progress(self, *, batch_id: str, from_ingested_at: datetime, to_ingested_at: datetime) -> None:
        """Insert a metadata record before archive work starts."""
        ...

    def mark_success(
        self,
        *,
        batch_id: str,
        s3_path: str,
        documents_count: int,
        archived_at: datetime,
    ) -> None:
        """Mark an archive batch as successfully uploaded and verified."""
        ...

    def mark_failed(self, *, batch_id: str) -> None:
        """Mark an archive batch as failed."""
        ...


class PostgresArchiveBatchRepository(ArchiveBatchRepository):
    """SQLAlchemy-backed repository for archive batch metadata."""

    def __init__(self, session: Session) -> None:
        self._session: Session = session

    @override
    def try_acquire_worker_lock(self) -> bool:
        result = cast(
            bool | None,
            self._session.execute(
                text("SELECT pg_try_advisory_lock(:lock_key)"),
                {"lock_key": ARCHIVE_WORKER_LOCK_KEY},
            ).scalar_one(),
        )
        return bool(result)

    @override
    def release_worker_lock(self) -> None:
        _: object = self._session.execute(
            text("SELECT pg_advisory_unlock(:lock_key)"),
            {"lock_key": ARCHIVE_WORKER_LOCK_KEY},
        )

    @override
    def get_last_successful_to_ingested_at(self) -> datetime | None:
        return self._session.scalar(
            select(func.max(ArchiveBatch.to_ingested_at)).where(ArchiveBatch.status == ArchiveBatchStatus.SUCCESS)
        )

    @override
    def create_in_progress(self, *, batch_id: str, from_ingested_at: datetime, to_ingested_at: datetime) -> None:
        self._session.add(
            ArchiveBatch(
                id=batch_id,
                from_ingested_at=from_ingested_at,
                to_ingested_at=to_ingested_at,
                archived_at=None,
                s3_path=None,
                documents_count=0,
                status=ArchiveBatchStatus.IN_PROGRESS,
            )
        )

    @override
    def mark_success(
        self,
        *,
        batch_id: str,
        s3_path: str,
        documents_count: int,
        archived_at: datetime,
    ) -> None:
        batch = self._require_batch(batch_id)
        batch.status = ArchiveBatchStatus.SUCCESS
        batch.s3_path = s3_path
        batch.documents_count = documents_count
        batch.archived_at = archived_at
        self._session.add(batch)

    @override
    def mark_failed(self, *, batch_id: str) -> None:
        batch = self._require_batch(batch_id)
        batch.status = ArchiveBatchStatus.FAILED
        self._session.add(batch)

    def _require_batch(self, batch_id: str) -> ArchiveBatch:
        batch = self._session.get(ArchiveBatch, batch_id)
        if batch is None:
            raise RuntimeError(f"Archive batch metadata not found: {batch_id}")
        return batch
