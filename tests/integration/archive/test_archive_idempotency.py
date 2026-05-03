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
    build_raw_doc,
    fetch_archive_batches,
    list_archive_keys,
    read_archive_jsonl,
    seed_raw_docs,
)


def _seed_docs() -> list[dict[str, object]]:
    return [
        build_raw_doc(doc_id="doc-1", ingested_at=T1, payload="old-1"),
        build_raw_doc(doc_id="doc-2", ingested_at=T2, payload="old-2"),
    ]


@pytest.mark.asyncio
async def test_archive_repeated_runs_on_replayed_data_create_distinct_batches(
    archive_settings: ArchivingServiceSettings,
    mongo_collection: AsyncIOMotorCollection,
    raw_archive_repository: MongoRawArchiveRepository,
    archive_storage: ArchiveStorage,
    state_uow_factory: Callable[[], SqlAlchemyArchiveStateUnitOfWork],
    session_factory: sessionmaker[Session],
    s3_client: BaseClient,
) -> None:
    await seed_raw_docs(mongo_collection, _seed_docs())
    await run_archive(
        app_settings=archive_settings,
        raw_archive_repo=raw_archive_repository,
        archive_storage=archive_storage,
        state_uow_factory=state_uow_factory,
    )
    assert await mongo_collection.count_documents({}) == 0

    await seed_raw_docs(mongo_collection, _seed_docs())
    await run_archive(
        app_settings=archive_settings,
        raw_archive_repo=raw_archive_repository,
        archive_storage=archive_storage,
        state_uow_factory=state_uow_factory,
    )
    assert await mongo_collection.count_documents({}) == 0

    keys = list_archive_keys(
        s3_client=s3_client, bucket=S3_BUCKET, prefix=f"{S3_PREFIX}/2026/01/"
    )
    assert len(keys) == 2
    assert keys[0] != keys[1]
    for key in keys:
        rows = read_archive_jsonl(s3_client=s3_client, bucket=S3_BUCKET, key=key)
        assert [row["_id"] for row in rows] == ["doc-1", "doc-2"]

    batches = fetch_archive_batches(session_factory)
    assert len(batches) == 2
    assert all(batch.status == ArchiveBatchStatus.SUCCESS for batch in batches)
    assert [batch.documents_count for batch in batches] == [2, 2]
    assert [batch.from_ingested_at for batch in batches] == [T1, T1]
    assert [batch.to_ingested_at for batch in batches] == [T2, T2]
