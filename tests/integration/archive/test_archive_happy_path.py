from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from datetime import datetime

import pytest
from botocore.client import BaseClient
from motor.motor_asyncio import AsyncIOMotorCollection
from sqlalchemy.orm import Session, sessionmaker

from app.clients.archive_storage import ArchiveStorage
from app.database.models import ArchiveBatchStatus
from app.repositories.mongo_raw_archive import MongoRawArchiveRepository, RawArchiveDocument
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
    object_exists,
    read_archive_jsonl,
    seed_raw_docs,
)


class OrderAssertingArchiveRepository:
    def __init__(
        self,
        delegate: MongoRawArchiveRepository,
        *,
        session_factory: sessionmaker[Session],
        s3_client: BaseClient,
    ) -> None:
        self._delegate = delegate
        self._session_factory = session_factory
        self._s3_client = s3_client
        self.assertion_executed = False

    async def iter_expired_batches(
        self,
        *,
        cutoff: datetime,
        batch_size: int,
        cursor_batch_size: int = 1000,
    ) -> AsyncIterator[list[RawArchiveDocument]]:
        async for batch in self._delegate.iter_expired_batches(
            cutoff=cutoff, batch_size=batch_size, cursor_batch_size=cursor_batch_size
        ):
            yield batch

    async def delete_by_ids(self, document_ids: list[object]) -> int:
        batches = fetch_archive_batches(self._session_factory)
        assert len(batches) == 1
        batch = batches[0]
        assert batch.status == ArchiveBatchStatus.SUCCESS
        assert batch.s3_path is not None
        assert object_exists(
            s3_client=self._s3_client, bucket=S3_BUCKET, key=batch.s3_path
        )
        self.assertion_executed = True
        return await self._delegate.delete_by_ids(document_ids)


@pytest.mark.asyncio
async def test_archive_happy_path_removes_expired_docs_and_persists_metadata(
    archive_settings: ArchivingServiceSettings,
    mongo_collection: AsyncIOMotorCollection,
    raw_archive_repository: MongoRawArchiveRepository,
    archive_storage: ArchiveStorage,
    state_uow_factory: Callable[[], SqlAlchemyArchiveStateUnitOfWork],
    session_factory: sessionmaker[Session],
    s3_client: BaseClient,
) -> None:
    docs = [
        build_raw_doc(doc_id="doc-1", ingested_at=T1, payload="old-1"),
        build_raw_doc(doc_id="doc-2", ingested_at=T2, payload="old-2"),
        build_raw_doc(
            doc_id="doc-fresh",
            ingested_at=datetime(2026, 5, 3, 11, 0, 0),
            payload="fresh",
        ),
    ]
    await seed_raw_docs(mongo_collection, docs)

    repo_with_order_check = OrderAssertingArchiveRepository(
        raw_archive_repository, session_factory=session_factory, s3_client=s3_client
    )
    await run_archive(
        app_settings=archive_settings,
        raw_archive_repo=repo_with_order_check,
        archive_storage=archive_storage,
        state_uow_factory=state_uow_factory,
    )

    assert repo_with_order_check.assertion_executed is True

    remaining_docs = [doc async for doc in mongo_collection.find({})]
    assert len(remaining_docs) == 1
    assert remaining_docs[0]["_id"] == "doc-fresh"

    keys = list_archive_keys(
        s3_client=s3_client, bucket=S3_BUCKET, prefix=f"{S3_PREFIX}/2026/01/"
    )
    assert len(keys) == 1
    assert keys[0].startswith("raw-archive/2026/01/part-")
    assert keys[0].endswith(".jsonl.gz")

    archived_docs = read_archive_jsonl(s3_client=s3_client, bucket=S3_BUCKET, key=keys[0])
    assert [doc["_id"] for doc in archived_docs] == ["doc-1", "doc-2"]

    batches = fetch_archive_batches(session_factory)
    assert len(batches) == 1
    assert batches[0].status == ArchiveBatchStatus.SUCCESS
    assert batches[0].from_ingested_at == T1
    assert batches[0].to_ingested_at == T2
    assert batches[0].documents_count == 2
    assert batches[0].s3_path == keys[0]
