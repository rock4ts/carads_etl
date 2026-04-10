"""Session factory helpers for shared Postgres persistence."""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.shared.database.models import MatchingStateBase


def build_postgres_session_factory(database_url: str) -> sessionmaker[Session]:
    """Create session factory and ensure state tables exist."""

    engine = create_engine(database_url)
    MatchingStateBase.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)
