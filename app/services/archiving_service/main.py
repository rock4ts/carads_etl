"""Mongo raw-ad archiving service entrypoint."""

from __future__ import annotations

import asyncio
import gzip
import logging
import shutil
import tempfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from bson.json_util import RELAXED_JSON_OPTIONS, dumps as bson_json_dumps

from app.clients.archive_storage import (
    ArchiveStorage,
    YandexObjectStorageArchiveStorage,
)
from app.clients.mongo_client import MongoClient
from app.database.session import build_postgres_session_factory
from app.repositories.mongo_raw_archive import (
    MongoRawArchiveRepository,
    RawArchiveDocument,
)
from app.services.archiving_service.core.config import (
    ArchivingServiceSettings,
    settings,
)
from app.uow.archive_state_uow import (
    ArchiveStateUnitOfWork,
    SqlAlchemyArchiveStateUnitOfWork,
)

logger = logging.getLogger(__name__)

ArchiveStateUowFactory = Callable[[], ArchiveStateUnitOfWork]


@dataclass(frozen=True)
class ArchiveBatchPayload:
    batch_id: str
    documents: list[RawArchiveDocument]
    document_ids: list[object]
    from_ingested_at: datetime
    to_ingested_at: datetime
    year: int
    month: int
    object_key: str


@dataclass(frozen=True)
class ArchiveBatchResult:
    documents_count: int
    deleted_count: int


def _configure_logging(app_settings: ArchivingServiceSettings = settings) -> None:
    level = getattr(logging, app_settings.log_level.strip().upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def _to_naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _parse_ingested_at(value: object) -> datetime:
    if isinstance(value, datetime):
        return _to_naive_utc(value)
    if isinstance(value, str):
        normalized_value = value.strip().replace("Z", "+00:00")
        try:
            return _to_naive_utc(datetime.fromisoformat(normalized_value))
        except ValueError as exc:
            raise RuntimeError(
                f"Invalid ingested_at value for archive: {value}"
            ) from exc
    raise RuntimeError(f"Invalid ingested_at type for archive: {type(value).__name__}")


def _split_by_archive_month(
    documents: list[RawArchiveDocument],
) -> Iterator[list[RawArchiveDocument]]:
    current_batch: list[RawArchiveDocument] = []
    current_month: tuple[int, int] | None = None
    for document in documents:
        ingested_at = _parse_ingested_at(document.get("ingested_at"))
        document_month = (ingested_at.year, ingested_at.month)
        if current_batch and document_month != current_month:
            yield current_batch
            current_batch = []
        current_batch.append(document)
        current_month = document_month
    if current_batch:
        yield current_batch


def _build_object_key(*, s3_prefix: str, year: int, month: int, batch_id: str) -> str:
    normalized_prefix = s3_prefix.strip("/")
    path = f"{year}/{month:02d}/part-{batch_id}.jsonl.gz"
    if not normalized_prefix:
        return path
    return f"{normalized_prefix}/{path}"


def _prepare_archive_batch(
    *, documents: list[RawArchiveDocument], s3_prefix: str
) -> ArchiveBatchPayload:
    if not documents:
        raise RuntimeError("Cannot archive an empty batch")
    ingested_values = [
        _parse_ingested_at(document.get("ingested_at")) for document in documents
    ]
    document_ids = [document["_id"] for document in documents]
    batch_id = str(uuid4())
    from_ingested_at = ingested_values[0]
    to_ingested_at = ingested_values[-1]
    year = from_ingested_at.year
    month = from_ingested_at.month
    return ArchiveBatchPayload(
        batch_id=batch_id,
        documents=documents,
        document_ids=document_ids,
        from_ingested_at=from_ingested_at,
        to_ingested_at=to_ingested_at,
        year=year,
        month=month,
        object_key=_build_object_key(
            s3_prefix=s3_prefix, year=year, month=month, batch_id=batch_id
        ),
    )


def _write_jsonl_file(batch: ArchiveBatchPayload) -> Path:
    jsonl_path = (
        Path(tempfile.gettempdir())
        / f"archive_{batch.year}_{batch.month:02d}_{batch.batch_id}.jsonl"
    )
    with jsonl_path.open("w", encoding="utf-8") as archive_file:
        for document in batch.documents:
            archive_file.write(
                bson_json_dumps(document, json_options=RELAXED_JSON_OPTIONS)
            )
            archive_file.write("\n")
    return jsonl_path


def _compress_jsonl_file(jsonl_path: Path) -> Path:
    gzip_path = jsonl_path.with_suffix(".jsonl.gz")
    with jsonl_path.open("rb") as source_file:
        with gzip.open(gzip_path, "wb") as archive_file:
            shutil.copyfileobj(source_file, archive_file)
    return gzip_path


def _write_compressed_archive_file(batch: ArchiveBatchPayload) -> tuple[Path, Path]:
    jsonl_path = _write_jsonl_file(batch)
    return jsonl_path, _compress_jsonl_file(jsonl_path)


def _remove_temp_file(path: Path | None) -> None:
    if path is None:
        return
    try:
        path.unlink(missing_ok=True)
    except OSError:
        logger.warning(
            "Failed to remove archive temp file path=%s", path, exc_info=True
        )


async def _mark_batch_failed(
    *, batch_id: str, state_uow_factory: ArchiveStateUowFactory
) -> None:
    with state_uow_factory() as uow:
        uow.archive_batches.mark_failed(batch_id=batch_id)
        uow.commit()


async def _archive_batch(
    *,
    batch: ArchiveBatchPayload,
    raw_archive_repo: MongoRawArchiveRepository,
    archive_storage: ArchiveStorage,
    state_uow_factory: ArchiveStateUowFactory,
) -> ArchiveBatchResult:
    jsonl_path: Path | None = None
    gzip_path: Path | None = None
    with state_uow_factory() as uow:
        uow.archive_batches.create_in_progress(
            batch_id=batch.batch_id,
            from_ingested_at=batch.from_ingested_at,
            to_ingested_at=batch.to_ingested_at,
        )
        uow.commit()
    try:
        jsonl_path, gzip_path = await asyncio.to_thread(
            _write_compressed_archive_file, batch
        )
        await asyncio.to_thread(
            archive_storage.upload_and_verify,
            local_path=gzip_path,
            object_key=batch.object_key,
        )
        archived_at = _to_naive_utc(datetime.now(timezone.utc))
        with state_uow_factory() as uow:
            uow.archive_batches.mark_success(
                batch_id=batch.batch_id,
                s3_path=batch.object_key,
                documents_count=len(batch.documents),
                archived_at=archived_at,
            )
            uow.commit()
        deleted_count = await raw_archive_repo.delete_by_ids(batch.document_ids)
        return ArchiveBatchResult(
            documents_count=len(batch.documents), deleted_count=deleted_count
        )
    except Exception:
        logger.exception(
            "Archive batch failed batch_id=%s from=%s to=%s s3_path=%s",
            batch.batch_id,
            batch.from_ingested_at,
            batch.to_ingested_at,
            batch.object_key,
        )
        try:
            await _mark_batch_failed(
                batch_id=batch.batch_id, state_uow_factory=state_uow_factory
            )
        except Exception:
            logger.exception(
                "Failed to mark archive batch failed batch_id=%s", batch.batch_id
            )
        raise
    finally:
        _remove_temp_file(jsonl_path)
        _remove_temp_file(gzip_path)


def _build_state_uow_factory(
    app_settings: ArchivingServiceSettings,
) -> ArchiveStateUowFactory:
    session_factory = build_postgres_session_factory(app_settings.postgres_database_url)

    def _state_uow_factory() -> SqlAlchemyArchiveStateUnitOfWork:
        return SqlAlchemyArchiveStateUnitOfWork(session_factory)

    return _state_uow_factory


def _build_raw_archive_repository(
    app_settings: ArchivingServiceSettings,
) -> MongoRawArchiveRepository:
    mongo_client = MongoClient(
        uri=app_settings.mongo_uri, db_name=app_settings.mongo_db
    )
    return MongoRawArchiveRepository(
        mongo_client.get_collection(app_settings.raw_collection_name)
    )


def _build_archive_storage(app_settings: ArchivingServiceSettings) -> ArchiveStorage:
    return YandexObjectStorageArchiveStorage(
        bucket=app_settings.s3_bucket,
        endpoint_url=app_settings.s3_endpoint,
        access_key_id=app_settings.aws_access_key_id,
        secret_access_key=app_settings.aws_secret_access_key,
        region_name=app_settings.aws_region,
    )


async def run_archive(
    *,
    app_settings: ArchivingServiceSettings = settings,
    raw_archive_repo: MongoRawArchiveRepository | None = None,
    archive_storage: ArchiveStorage | None = None,
    state_uow_factory: ArchiveStateUowFactory | None = None,
) -> None:
    raw_archive_repo = raw_archive_repo or _build_raw_archive_repository(app_settings)
    archive_storage = archive_storage or _build_archive_storage(app_settings)
    state_uow_factory = state_uow_factory or _build_state_uow_factory(app_settings)
    cutoff = _to_naive_utc(
        datetime.now(timezone.utc) - timedelta(days=app_settings.archive_retention_days)
    )
    processed_total = 0
    deleted_total = 0
    batch_total = 0

    with state_uow_factory() as lock_uow:
        if not lock_uow.archive_batches.try_acquire_worker_lock():
            logger.warning(
                "Archive job skipped because another archive worker is running"
            )
            return
        try:
            last_archived_at = (
                lock_uow.archive_batches.get_last_successful_to_ingested_at()
            )
            logger.info(
                "Archive job started cutoff=%s last_successful_to_ingested_at=%s",
                cutoff,
                last_archived_at,
            )
            async for raw_batch in raw_archive_repo.iter_expired_batches(
                cutoff=cutoff,
                batch_size=app_settings.archive_batch_size,
            ):
                for monthly_batch in _split_by_archive_month(raw_batch):
                    archive_batch = _prepare_archive_batch(
                        documents=monthly_batch,
                        s3_prefix=app_settings.normalized_s3_prefix,
                    )
                    result = await _archive_batch(
                        batch=archive_batch,
                        raw_archive_repo=raw_archive_repo,
                        archive_storage=archive_storage,
                        state_uow_factory=state_uow_factory,
                    )
                    processed_total += result.documents_count
                    deleted_total += result.deleted_count
                    batch_total += 1
                    logger.info(
                        "Archive batch succeeded batch_id=%s size=%s deleted=%s from=%s to=%s s3_path=%s",
                        archive_batch.batch_id,
                        result.documents_count,
                        result.deleted_count,
                        archive_batch.from_ingested_at,
                        archive_batch.to_ingested_at,
                        archive_batch.object_key,
                    )
            logger.info(
                "Archive job finished batches=%s documents=%s deleted=%s cutoff=%s",
                batch_total,
                processed_total,
                deleted_total,
                cutoff,
            )
        finally:
            lock_uow.archive_batches.release_worker_lock()
            lock_uow.commit()


async def main() -> None:
    await run_archive()


def cli_main() -> None:
    _configure_logging()
    asyncio.run(main())


if __name__ == "__main__":
    cli_main()
