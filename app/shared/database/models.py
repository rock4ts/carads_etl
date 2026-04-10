"""ORM models for matching-worker domain-state persistence."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class MatchingStateBase(DeclarativeBase):
    """Declarative base for matching-worker state tables."""


class MarkerTimestamp(MatchingStateBase):
    __tablename__ = "marker_timestamps"

    site_name: Mapped[str] = mapped_column(String(64), primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(), nullable=False)


class UploadTimestamp(MatchingStateBase):
    __tablename__ = "upload_timestamps"

    site_name: Mapped[str] = mapped_column(String(64), primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(), nullable=False)

