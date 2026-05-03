from __future__ import annotations

from collections.abc import Callable

import pytest
from botocore.client import BaseClient
from motor.motor_asyncio import AsyncIOMotorCollection
from sqlalchemy.orm import Session, sessionmaker

from app.clients.archive_storage import ArchiveStorage
from app.database.models import ArchiveBatchStatus
from app.repositories.mongo_raw_archive import MongoRawArchiveRepository
from app.services.archiving_service.core.config import ArchivingServiceSettings
from app.services.archiving_service.main import run_archive
from app.uow.archive_state_uow import SqlAlchemyArchiveStateUnitOfWork
from tests.integration.archive.conftest import (
    S3_BUCKET,
    S3_PREFIX,
    T1,
    T2,
    T3,
    build_raw_doc,
    fetch_archive_batches,
    list_archive_keys,
    read_archive_jsonl,
    seed_raw_docs,
)


@pytest.mark.asyncio
async def test_archive_writes_separate_objects_per_month_partition(
    archive_settings: ArchivingServiceSettings,
    mongo_collection: AsyncIOMotorCollection,
    raw_archive_repository: MongoRawArchiveRepository,
    archive_storage: ArchiveStorage,
    state_uow_factory: Callable[[], SqlAlchemyArchiveStateUnitOfWork],
    session_factory: sessionmaker[Session],
    s3_client: BaseClient,
) -> None:
    await seed_raw_docs(
        mongo_collection,
        [
            build_raw_doc(doc_id="doc-jan-1", ingested_at=T1, payload="jan-1"),
            build_raw_doc(doc_id="doc-jan-2", ingested_at=T2, payload="jan-2"),
            build_raw_doc(doc_id="doc-feb-1", ingested_at=T3, payload="feb-1"),
        ],
    )

    await run_archive(
        app_settings=archive_settings,
        raw_archive_repo=raw_archive_repository,
        archive_storage=archive_storage,
        state_uow_factory=state_uow_factory,
    )

    jan_keys = list_archive_keys(
        s3_client=s3_client, bucket=S3_BUCKET, prefix=f"{S3_PREFIX}/2026/01/"
    )
    feb_keys = list_archive_keys(
        s3_client=s3_client, bucket=S3_BUCKET, prefix=f"{S3_PREFIX}/2026/02/"
    )
    assert len(jan_keys) == 1
    assert len(feb_keys) == 1

    jan_rows = read_archive_jsonl(s3_client=s3_client, bucket=S3_BUCKET, key=jan_keys[0])
    feb_rows = read_archive_jsonl(s3_client=s3_client, bucket=S3_BUCKET, key=feb_keys[0])
    assert [row["_id"] for row in jan_rows] == ["doc-jan-1", "doc-jan-2"]
    assert [row["_id"] for row in feb_rows] == ["doc-feb-1"]

    batches = fetch_archive_batches(session_factory)
    assert len(batches) == 2
    assert all(batch.status == ArchiveBatchStatus.SUCCESS for batch in batches)
    assert [batch.from_ingested_at for batch in batches] == [T1, T3]
    assert [batch.to_ingested_at for batch in batches] == [T2, T3]
