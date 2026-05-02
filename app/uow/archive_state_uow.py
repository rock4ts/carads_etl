"""Unit of Work for archive metadata persistence."""

from __future__ import annotations

from typing import Protocol

from sqlalchemy.orm import Session, sessionmaker

from app.repositories.archive_batches import (
    ArchiveBatchRepository,
    PostgresArchiveBatchRepository,
)


class ArchiveStateUnitOfWork(Protocol):
    """Contract for archive metadata transaction boundaries."""

    archive_batches: ArchiveBatchRepository

    def __enter__(self) -> "ArchiveStateUnitOfWork":
        """Enter a transaction scope."""
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        """Exit transaction scope."""

    def commit(self) -> None:
        """Commit pending changes."""

    def rollback(self) -> None:
        """Rollback pending changes."""


class SqlAlchemyArchiveStateUnitOfWork(ArchiveStateUnitOfWork):
    """SQLAlchemy implementation for Postgres-backed archive metadata."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory
        self._session: Session | None = None
        self.archive_batches: ArchiveBatchRepository

    def __enter__(self) -> "SqlAlchemyArchiveStateUnitOfWork":
        self._session = self._session_factory()
        self.archive_batches = PostgresArchiveBatchRepository(self._session)
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self._session is None:
            return
        if exc_type is not None:
            self._session.rollback()
        self._session.close()
        self._session = None

    def commit(self) -> None:
        if self._session is None:
            raise RuntimeError("Unit of work session is not initialized.")
        self._session.commit()

    def rollback(self) -> None:
        if self._session is None:
            raise RuntimeError("Unit of work session is not initialized.")
        self._session.rollback()
