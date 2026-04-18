from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime

from app.repositories.ingestion_state import IngestionStateRepository
from app.schemas.raw import RawAd
from app.uow.ingestion_state_uow import IngestionStateUnitOfWork


class FakeParserClient:
    def __init__(self, responses: Iterable[list[dict[str, object]]]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, object]] = []

    async def fetch(
        self,
        *,
        parser_api_url: str,
        parser_api_key: str,
        site_name: str,
        current_from: datetime,
    ) -> list[dict[str, object]]:
        self.calls.append(
            {
                "parser_api_url": parser_api_url,
                "parser_api_key": parser_api_key,
                "site_name": site_name,
                "current_from": current_from,
            }
        )
        if not self._responses:
            raise AssertionError("FakeParserClient exhausted: no response left for call")
        return self._responses.pop(0)


class FakeMongo:
    def __init__(self) -> None:
        self.saved_raw_ads: list[RawAd] = []

    async def save(self, raw_ads: list[RawAd], **kwargs: object) -> None:
        self.saved_raw_ads.extend(raw_ads)


class FakeES:
    def __init__(self) -> None:
        self.indexed_docs: list[object] = []
        self.docs_by_id: dict[str, object] = {}

    async def save(self, docs: list[object], **kwargs: object) -> None:
        for doc in docs:
            self.indexed_docs.append(doc)
            site_name = getattr(doc, "site_name", "unknown")
            parapi_unique_id = getattr(doc, "parapi_unique_id", None)
            doc_id = f"[{site_name}]{parapi_unique_id}"
            self.docs_by_id[doc_id] = doc


class FakeUploadTimestampRepo:
    """In-memory stand-in for IngestionStateRepository."""

    def __init__(self, initial_timestamps: dict[str, datetime] | None = None) -> None:
        self.timestamps = dict(initial_timestamps or {})
        self.upserts: list[tuple[str, datetime]] = []

    def list_upload_sites(self) -> Sequence[str]:
        return sorted(self.timestamps.keys())

    def get_upload_timestamp(self, site_name: str) -> datetime | None:
        return self.timestamps.get(site_name)

    def upsert_upload_timestamp(self, site_name: str, timestamp: datetime) -> None:
        self.timestamps[site_name] = timestamp
        self.upserts.append((site_name, timestamp))


class FakeStateUow:
    def __init__(self, ingestion_states: IngestionStateRepository, commit_log: list[int]) -> None:
        self.ingestion_states: IngestionStateRepository = ingestion_states
        self._commit_log = commit_log

    def __enter__(self) -> IngestionStateUnitOfWork:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return

    def commit(self) -> None:
        self._commit_log.append(1)

    def rollback(self) -> None:
        return


class FakeStateUowFactory:
    def __init__(self, ingestion_states: IngestionStateRepository) -> None:
        self._states = ingestion_states
        self.commit_log: list[int] = []

    def __call__(self) -> IngestionStateUnitOfWork:
        return FakeStateUow(self._states, self.commit_log)
