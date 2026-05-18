"""ORM models for ETL domain-state persistence."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from sqlalchemy import Boolean, DateTime, Enum, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class EtlStateBase(DeclarativeBase):
    """Declarative base for matching-worker state tables."""


class MarkerTimestamp(EtlStateBase):
    __tablename__ = "marker_timestamps"

    site: Mapped[str] = mapped_column(String(64), primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(), nullable=False)


class UploadTimestamp(EtlStateBase):
    __tablename__ = "upload_timestamps"

    site: Mapped[str] = mapped_column(String(64), primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(), nullable=False)


class ArchiveBatchStatus(StrEnum):
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"


class ArchiveBatch(EtlStateBase):
    __tablename__ = "archive_batches"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    from_ingested_at: Mapped[datetime] = mapped_column(DateTime(), nullable=False)
    to_ingested_at: Mapped[datetime] = mapped_column(DateTime(), nullable=False)
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(), nullable=True)
    s3_path: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    documents_count: Mapped[int] = mapped_column(Integer(), nullable=False, default=0)
    status: Mapped[ArchiveBatchStatus] = mapped_column(
        Enum(
            ArchiveBatchStatus,
            values_callable=lambda statuses: [status.value for status in statuses],
            name="archive_batch_status",
        ),
        nullable=False,
    )


class BackfillMatcherState(EtlStateBase):
    __tablename__ = "backfill_matcher_states"

    site: Mapped[str] = mapped_column(String(64), primary_key=True)
    next_from: Mapped[datetime] = mapped_column(DateTime(), nullable=False)
    reset_completed: Mapped[bool] = mapped_column(Boolean(), nullable=False, default=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(), nullable=False)
