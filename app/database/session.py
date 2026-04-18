"""Session factory helpers for Postgres persistence."""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.database.models import EtlStateBase


def build_postgres_session_factory(database_url: str) -> sessionmaker[Session]:
    """Create session factory and ensure state tables exist."""

    engine = create_engine(database_url)
    EtlStateBase.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)
