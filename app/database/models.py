"""ORM models for matching-worker domain-state persistence."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, String
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
