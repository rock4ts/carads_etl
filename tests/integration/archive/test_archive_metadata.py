from __future__ import annotations

from collections.abc import Callable

import pytest
from motor.motor_asyncio import AsyncIOMotorCollection
from sqlalchemy.orm import Session, sessionmaker

from app.clients.archive_storage import ArchiveStorage
from app.database.models import ArchiveBatchStatus
from app.repositories.mongo_raw_archive import MongoRawArchiveRepository
from app.services.archiving_service.core.config import ArchivingServiceSettings
from app.services.archiving_service.main import run_archive
from app.uow.archive_state_uow import SqlAlchemyArchiveStateUnitOfWork
from tests.integration.archive.conftest import (
    FROZEN_NOW,
    T1,
    T2,
    build_raw_doc,
    fetch_archive_batches,
    seed_raw_docs,
)


@pytest.mark.asyncio
async def test_archive_metadata_fields_are_consistent(
    archive_settings: ArchivingServiceSettings,
    mongo_collection: AsyncIOMotorCollection,
    raw_archive_repository: MongoRawArchiveRepository,
    archive_storage: ArchiveStorage,
    state_uow_factory: Callable[[], SqlAlchemyArchiveStateUnitOfWork],
    session_factory: sessionmaker[Session],
) -> None:
    await seed_raw_docs(
        mongo_collection,
        [
            build_raw_doc(doc_id="doc-1", ingested_at=T1, payload="old-1"),
            build_raw_doc(doc_id="doc-2", ingested_at=T2, payload="old-2"),
        ],
    )

    await run_archive(
        app_settings=archive_settings,
        raw_archive_repo=raw_archive_repository,
        archive_storage=archive_storage,
        state_uow_factory=state_uow_factory,
    )

    batches = fetch_archive_batches(session_factory)
    assert len(batches) == 1
    batch = batches[0]
    assert batch.status == ArchiveBatchStatus.SUCCESS
    assert batch.from_ingested_at == T1
    assert batch.to_ingested_at == T2
    assert batch.documents_count == 2
    assert batch.archived_at == FROZEN_NOW.replace(tzinfo=None)
    assert batch.s3_path is not None
    assert batch.s3_path.startswith("raw-archive/2026/01/part-")
    assert batch.s3_path.endswith(".jsonl.gz")
