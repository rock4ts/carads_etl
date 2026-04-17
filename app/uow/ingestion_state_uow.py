"""Unit of Work for ingestion state persistence."""

from __future__ import annotations

from typing import Protocol

from sqlalchemy.orm import Session, sessionmaker

from app.repositories.ingestion_state import IngestionStateRepository, PostgresIngestionStateRepository


class IngestionStateUnitOfWork(Protocol):
    """Contract for ingestion state transaction boundaries."""

    ingestion_states: IngestionStateRepository

    def __enter__(self) -> "IngestionStateUnitOfWork":
        """Enter a transaction scope."""
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        """Exit transaction scope."""

    def commit(self) -> None:
        """Commit pending changes."""

    def rollback(self) -> None:
        """Rollback pending changes."""


class SqlAlchemyIngestionStateUnitOfWork(IngestionStateUnitOfWork):
    """SQLAlchemy implementation for Postgres-backed ingestion state."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory
        self._session: Session | None = None
        self.ingestion_states: IngestionStateRepository

    def __enter__(self) -> "SqlAlchemyIngestionStateUnitOfWork":
        self._session = self._session_factory()
        self.ingestion_states = PostgresIngestionStateRepository(self._session)
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
