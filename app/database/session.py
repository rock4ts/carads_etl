"""Session factory helpers for Postgres persistence."""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.database.models import EtlStateBase


def ensure_etl_state_tables(database_url: str) -> None:
    """Create ETL state tables if they do not exist."""

    engine = create_engine(database_url)
    try:
        EtlStateBase.metadata.create_all(engine)
    finally:
        engine.dispose()


def build_postgres_session_factory(database_url: str) -> sessionmaker[Session]:
    """Create a SQLAlchemy session factory for the given database URL."""

    engine = create_engine(database_url)
    return sessionmaker(bind=engine, expire_on_commit=False)
