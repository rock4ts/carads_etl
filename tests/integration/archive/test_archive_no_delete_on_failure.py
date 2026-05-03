from __future__ import annotations

from collections.abc import Callable

import pytest
from botocore.client import BaseClient
from motor.motor_asyncio import AsyncIOMotorCollection
from sqlalchemy.orm import Session, sessionmaker

from app.clients.archive_storage import YandexObjectStorageArchiveStorage
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
    seed_raw_docs,
)


@pytest.mark.asyncio
async def test_archive_failure_does_not_delete_mongo_and_marks_batch_failed(
    monkeypatch: pytest.MonkeyPatch,
    archive_settings: ArchivingServiceSettings,
    mongo_collection: AsyncIOMotorCollection,
    raw_archive_repository: MongoRawArchiveRepository,
    archive_storage: YandexObjectStorageArchiveStorage,
    state_uow_factory: Callable[[], SqlAlchemyArchiveStateUnitOfWork],
    session_factory: sessionmaker[Session],
    s3_client: BaseClient,
) -> None:
    await seed_raw_docs(
        mongo_collection,
        [
            build_raw_doc(doc_id="doc-1", ingested_at=T1, payload="old-1"),
            build_raw_doc(doc_id="doc-2", ingested_at=T2, payload="old-2"),
        ],
    )

    def _always_fail_upload_and_verify(*, local_path: object, object_key: str) -> None:
        del local_path, object_key
        raise RuntimeError("simulated upload failure")

    monkeypatch.setattr(
        archive_storage,
        "upload_and_verify",
        _always_fail_upload_and_verify,
    )

    with pytest.raises(RuntimeError, match="simulated upload failure"):
        await run_archive(
            app_settings=archive_settings,
            raw_archive_repo=raw_archive_repository,
            archive_storage=archive_storage,
            state_uow_factory=state_uow_factory,
        )

    remaining_ids = sorted([doc["_id"] async for doc in mongo_collection.find({})])
    assert remaining_ids == ["doc-1", "doc-2"]

    keys = list_archive_keys(
        s3_client=s3_client, bucket=S3_BUCKET, prefix=f"{S3_PREFIX}/2026/01/"
    )
    assert keys == []

    batches = fetch_archive_batches(session_factory)
    assert len(batches) == 1
    assert batches[0].status == ArchiveBatchStatus.FAILED
    assert batches[0].documents_count == 0
    assert batches[0].s3_path is None
    assert batches[0].archived_at is None
