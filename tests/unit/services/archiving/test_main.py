from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime
from pathlib import Path
from typing import cast

import pytest

from app.clients.archive_storage import ArchiveStorage
from app.repositories.mongo_raw_archive import (
    MongoRawArchiveRepository,
    RawArchiveDocument,
)
from app.services.archiving_service.core.config import ArchivingServiceSettings
import app.services.archiving_service.main as archiving_main
from app.services.archiving_service.main import ArchiveStateUowFactory, run_archive


def _build_settings() -> ArchivingServiceSettings:
    return ArchivingServiceSettings(
        MONGO_URI="mongodb://unused",
        MONGO_DB="etl",
        RAW_COLLECTION_NAME="raw_ads",
        POSTGRES_DATABASE_URL="postgresql://unused",
        ARCHIVE_BATCH_SIZE=1000,
        S3_BUCKET="archive-bucket",
        S3_PREFIX="raw-archive/",
        S3_ENDPOINT="https://storage.yandexcloud.net",
        AWS_ACCESS_KEY_ID="access-key",
        AWS_SECRET_ACCESS_KEY="secret-key",
        AWS_REGION="ru-central1",
        TELEGRAM_REPORTING_ENABLED=False,
    )


class _Reporter:
    instances: list["_Reporter"] = []

    def __init__(self) -> None:
        self.progress_messages: list[str] = []
        self.critical_messages: list[str] = []
        _Reporter.instances.append(self)

    @classmethod
    def from_settings(
        cls,
        *,
        service_name: str,
        settings: ArchivingServiceSettings,
        logger: object,
    ) -> "_Reporter":
        _ = (service_name, settings, logger)
        return cls()

    async def send_progress(self, message: str) -> None:
        self.progress_messages.append(message)

    async def send_critical(self, message: str) -> None:
        self.critical_messages.append(message)


class _FakeArchiveBatches:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.lock_released = False

    def try_acquire_worker_lock(self) -> bool:
        self.events.append("lock")
        return True

    def release_worker_lock(self) -> None:
        self.lock_released = True
        self.events.append("unlock")

    def get_last_successful_to_ingested_at(self) -> datetime | None:
        self.events.append("last-success")
        return None

    def create_in_progress(
        self, *, batch_id: str, from_ingested_at: datetime, to_ingested_at: datetime
    ) -> None:
        self.events.append(f"in-progress:{batch_id}:{from_ingested_at:%Y-%m}")

    def mark_success(
        self,
        *,
        batch_id: str,
        s3_path: str,
        documents_count: int,
        archived_at: datetime,
    ) -> None:
        self.events.append(f"success:{batch_id}:{documents_count}:{s3_path}")

    def mark_failed(self, *, batch_id: str) -> None:
        self.events.append(f"failed:{batch_id}")


class _FakeUow:
    def __init__(self, archive_batches: _FakeArchiveBatches, events: list[str]) -> None:
        self.archive_batches = archive_batches
        self._events = events

    def __enter__(self) -> "_FakeUow":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return

    def commit(self) -> None:
        self._events.append("commit")

    def rollback(self) -> None:
        self._events.append("rollback")


class _FakeRawArchiveRepository:
    def __init__(self, documents: list[RawArchiveDocument], events: list[str]) -> None:
        self._documents = documents
        self._events = events
        self.deleted_ids: list[object] = []

    async def iter_expired_batches(
        self,
        *,
        cutoff: datetime,
        batch_size: int,
        cursor_batch_size: int = 1000,
    ) -> AsyncIterator[list[RawArchiveDocument]]:
        yield self._documents

    async def delete_by_ids(self, document_ids: list[object]) -> int:
        self._events.append(
            f"delete:{','.join(str(document_id) for document_id in document_ids)}"
        )
        self.deleted_ids.extend(document_ids)
        return len(document_ids)


class _FakeArchiveStorage:
    def __init__(self, events: list[str], *, should_fail: bool = False) -> None:
        self._events = events
        self._should_fail = should_fail
        self.uploaded_keys: list[str] = []

    def upload_and_verify(self, *, local_path: Path, object_key: str) -> None:
        assert local_path.exists()
        self._events.append(f"upload:{object_key}")
        self.uploaded_keys.append(object_key)
        if self._should_fail:
            raise RuntimeError("upload verification failed")


def test_archive_batch_succeeds_before_deleting_mongo_documents() -> None:
    events: list[str] = []
    archive_batches = _FakeArchiveBatches(events)
    raw_repo = _FakeRawArchiveRepository(
        [
            {
                "_id": "id-1",
                "ingested_at": datetime(2026, 4, 1, 0, 0),
                "payload": {"id": 1},
            },
            {
                "_id": "id-2",
                "ingested_at": datetime(2026, 4, 2, 0, 0),
                "payload": {"id": 2},
            },
        ],
        events,
    )
    storage = _FakeArchiveStorage(events)
    _Reporter.instances.clear()

    def _state_uow_factory() -> _FakeUow:
        return _FakeUow(archive_batches, events)

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(archiving_main, "TelegramReporter", _Reporter)
    asyncio.run(
        run_archive(
            app_settings=_build_settings(),
            raw_archive_repo=cast(MongoRawArchiveRepository, raw_repo),
            archive_storage=cast(ArchiveStorage, storage),
            state_uow_factory=cast(ArchiveStateUowFactory, _state_uow_factory),
        )
    )
    monkeypatch.undo()

    assert raw_repo.deleted_ids == ["id-1", "id-2"]
    assert len(storage.uploaded_keys) == 1
    assert storage.uploaded_keys[0].startswith("raw-archive/2026/04/part-")
    assert storage.uploaded_keys[0].endswith(".jsonl.gz")
    assert events.index("upload:" + storage.uploaded_keys[0]) < events.index(
        "delete:id-1,id-2"
    )
    assert any(event.startswith("in-progress:") for event in events)
    assert any(event.startswith("success:") for event in events)
    assert len(_Reporter.instances) == 1
    reporter = _Reporter.instances[0]
    assert len(reporter.progress_messages) == 2
    assert reporter.progress_messages[0] == "Archive started | retention_days=60"
    assert reporter.progress_messages[1].startswith("Archive completed | docs=2 | batches=1 | cutoff=")
    assert reporter.critical_messages == []


def test_archive_batches_are_partitioned_by_month() -> None:
    events: list[str] = []
    archive_batches = _FakeArchiveBatches(events)
    raw_repo = _FakeRawArchiveRepository(
        [
            {
                "_id": "id-1",
                "ingested_at": datetime(2026, 4, 30, 23, 0),
                "payload": {"id": 1},
            },
            {
                "_id": "id-2",
                "ingested_at": datetime(2026, 5, 1, 0, 0),
                "payload": {"id": 2},
            },
        ],
        events,
    )
    storage = _FakeArchiveStorage(events)
    _Reporter.instances.clear()

    def _state_uow_factory() -> _FakeUow:
        return _FakeUow(archive_batches, events)

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(archiving_main, "TelegramReporter", _Reporter)
    asyncio.run(
        run_archive(
            app_settings=_build_settings(),
            raw_archive_repo=cast(MongoRawArchiveRepository, raw_repo),
            archive_storage=cast(ArchiveStorage, storage),
            state_uow_factory=cast(ArchiveStateUowFactory, _state_uow_factory),
        )
    )
    monkeypatch.undo()

    assert len(storage.uploaded_keys) == 2
    assert storage.uploaded_keys[0].startswith("raw-archive/2026/04/part-")
    assert storage.uploaded_keys[1].startswith("raw-archive/2026/05/part-")
    assert raw_repo.deleted_ids == ["id-1", "id-2"]
    assert len(_Reporter.instances) == 1
    reporter = _Reporter.instances[0]
    assert len(reporter.progress_messages) == 2
    assert reporter.critical_messages == []


def test_archive_failure_marks_metadata_failed_and_keeps_mongo_documents() -> None:
    events: list[str] = []
    archive_batches = _FakeArchiveBatches(events)
    raw_repo = _FakeRawArchiveRepository(
        [
            {
                "_id": "id-1",
                "ingested_at": datetime(2026, 4, 1, 0, 0),
                "payload": {"id": 1},
            }
        ],
        events,
    )
    storage = _FakeArchiveStorage(events, should_fail=True)
    _Reporter.instances.clear()

    def _state_uow_factory() -> _FakeUow:
        return _FakeUow(archive_batches, events)

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(archiving_main, "TelegramReporter", _Reporter)
    with pytest.raises(RuntimeError, match="upload verification failed"):
        asyncio.run(
            run_archive(
                app_settings=_build_settings(),
                raw_archive_repo=cast(MongoRawArchiveRepository, raw_repo),
                archive_storage=cast(ArchiveStorage, storage),
                state_uow_factory=cast(ArchiveStateUowFactory, _state_uow_factory),
            )
        )
    monkeypatch.undo()

    assert raw_repo.deleted_ids == []
    assert any(event.startswith("in-progress:") for event in events)
    assert any(event.startswith("failed:") for event in events)
    assert not any(event.startswith("delete:") for event in events)
    assert len(_Reporter.instances) == 1
    reporter = _Reporter.instances[0]
    assert len(reporter.progress_messages) == 1
    assert reporter.progress_messages[0] == "Archive started | retention_days=60"
    assert len(reporter.critical_messages) == 1
    assert reporter.critical_messages[0].startswith("Archive failed: upload verification failed")


def test_archive_batch_size_is_clamped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ARCHIVE_BATCH_SIZE", "9999")

    cfg = ArchivingServiceSettings()

    assert cfg.archive_batch_size == 5000
