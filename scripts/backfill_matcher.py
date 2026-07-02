"""One-time duplicate-chain backfill over existing Elasticsearch data."""

# pyright: reportAny=false, reportUnusedCallResult=false

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pydantic import ValidationError

from app.clients import ElasticsearchHttpClient
from app.database.models import BackfillMatcherState
from app.database.session import build_postgres_session_factory, ensure_etl_state_tables
from app.repositories.elasticsearch_processed_ads import ElasticsearchProcessedAdsRepository
from app.schemas.processed import CaradDocData
from app.services.matching_service.core.config import settings
from app.services.matching_service.main import MATCHING_MAPPING_FIELDS
from app.services.matching_service.matcher import configure_matcher, find_best_duplicate
from app.services.telegram_notifier import TelegramReporter

logger = logging.getLogger(__name__)

BACKFILL_START = datetime(2024, 7, 10, 0, 0, 0, tzinfo=timezone.utc)
VALIDATION_SOURCE_FIELDS = ["predecessor_id", "successor_id"]


@dataclass(slots=True)
class SiteStats:
    processed: int = 0
    matches_found: int = 0
    claim_conflicts: int = 0
    linked: int = 0


@dataclass(frozen=True, slots=True)
class SiteProgressState:
    site: str
    next_from: datetime
    reset_completed: bool


def _configure_logging(log_level: str) -> None:
    level = getattr(logging, log_level.strip().upper(), logging.INFO)
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s - %(message)s")


def _extract_hit_id(hit: dict[str, Any]) -> str | None:
    raw_value = hit.get("_id")
    if raw_value is None:
        return None
    return str(raw_value)


def _extract_hit_source(hit: dict[str, Any]) -> dict[str, Any] | None:
    source = hit.get("_source")
    if isinstance(source, dict):
        return source
    return None


def _extract_hit_sort(hit: dict[str, Any]) -> list[Any] | None:
    sort_values = hit.get("sort")
    if isinstance(sort_values, list) and sort_values:
        return list(sort_values)
    return None


def _build_sites_query() -> dict[str, Any]:
    return {"bool": {"filter": [{"exists": {"field": "site_name.keyword"}}]}}


def _build_site_query(*, site_name: str) -> dict[str, Any]:
    return {"bool": {"filter": [{"term": {"site_name.keyword": site_name}}]}}


def _build_site_candidates_query(
    *, site_name: str, lower_bound: datetime, upper_bound: datetime
) -> dict[str, Any]:
    return {
        "bool": {
            "filter": [
                {"term": {"site_name.keyword": site_name}},
                {
                    "range": {
                        "offer_start": {
                            "gte": lower_bound.isoformat(),
                            "lt": upper_bound.isoformat(),
                        }
                    }
                },
            ]
        }
    }


def _build_candidate_from_source(source: dict[str, Any], doc_id: str) -> CaradDocData:
    now = datetime.now(timezone.utc)
    payload = dict(source)
    if payload.get("original_id") in (None, ""):
        payload["original_id"] = doc_id
    if payload.get("url") is None:
        payload["url"] = ""
    if payload.get("site_name") is None:
        payload["site_name"] = "unknown"
    if payload.get("seller_type") is None:
        payload["seller_type"] = "unknown"
    if payload.get("name") is None:
        payload["name"] = ""
    if payload.get("last_checked") is None:
        payload["last_checked"] = now
    if payload.get("parsed_at") is None:
        payload["parsed_at"] = now
    if payload.get("offer_start") is None:
        payload["offer_start"] = payload.get("offer_end") or payload.get("last_checked") or now
    if payload.get("trans_history") is None:
        payload["trans_history"] = []
    latest_price = payload.get("latest_price")
    if payload.get("initial_price") is None:
        payload["initial_price"] = latest_price if latest_price is not None else 0.0
    if payload.get("latest_price") is None:
        payload["latest_price"] = payload["initial_price"]
    if payload.get("is_new") is None:
        payload["is_new"] = False
    if payload.get("brand") is None:
        payload["brand"] = ""
    if payload.get("model") is None:
        payload["model"] = ""
    if "predecessor_id" not in payload:
        payload["predecessor_id"] = None
    if "successor_id" not in payload:
        payload["successor_id"] = None
    return CaradDocData.model_validate(payload)


def _build_reset_operations(*, index_name: str, doc_ids: list[str]) -> list[dict[str, Any]]:
    operations: list[dict[str, Any]] = []
    for doc_id in doc_ids:
        operations.append({"update": {"_index": index_name, "_id": doc_id}})
        operations.append(
            {
                "script": {
                    "source": """
                        ctx._source.predecessor_id = null;
                        ctx._source.successor_id = null;
                        ctx._source.is_duplicate = null;
                    """
                }
            }
        )
    return operations


def _build_predecessor_operations(
    *, index_name: str, links: list[tuple[str, str]]
) -> list[dict[str, Any]]:
    operations: list[dict[str, Any]] = []
    for candidate_id, duplicate_id in links:
        operations.append({"update": {"_index": index_name, "_id": candidate_id}})
        operations.append(
            {
                "script": {
                    "source": """
                        if (ctx._source.predecessor_id == null) {
                            ctx._source.predecessor_id = params.predecessor_id;
                        } else if (ctx._source.predecessor_id == params.predecessor_id) {
                            ctx.op = 'none';
                        } else {
                            ctx.op = 'none';
                        }
                    """,
                    "params": {"predecessor_id": duplicate_id},
                }
            }
        )
    return operations


def _parse_bulk_update_outcome(
    *,
    response: dict[str, Any],
    links: list[tuple[str, str]],
) -> tuple[int, list[tuple[str, str]]]:
    items = response.get("items", [])
    if not isinstance(items, list):
        raise RuntimeError("Bulk update response is malformed: `items` is not a list.")
    if len(items) != len(links):
        raise RuntimeError(
            "Bulk update response size mismatch: " f"expected={len(links)} actual={len(items)}."
        )

    updated = 0
    conflicts: list[tuple[str, str]] = []
    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            conflicts.append(links[idx])
            continue
        action = item.get("update")
        if not isinstance(action, dict):
            conflicts.append(links[idx])
            continue
        if "error" in action:
            conflicts.append(links[idx])
            continue
        result = action.get("result")
        if result == "updated":
            updated += 1
            continue
        conflicts.append(links[idx])
    return updated, conflicts


def _next_month_boundary(value: datetime) -> datetime:
    if value.month == 12:
        return datetime(value.year + 1, 1, 1, tzinfo=value.tzinfo)
    return datetime(value.year, value.month + 1, 1, tzinfo=value.tzinfo)


def _month_window_end(month_start: datetime) -> datetime:
    month_boundary = _next_month_boundary(month_start)
    return month_boundary


def _current_month_start(now_utc: datetime) -> datetime:
    return datetime(now_utc.year, now_utc.month, 1, tzinfo=now_utc.tzinfo)


class BackfillStateStore:
    def __init__(self, *, session_factory: Any) -> None:
        self._session_factory = session_factory

    def get_or_create(self, *, site_name: str) -> SiteProgressState:
        with self._session_factory() as session:
            row = session.get(BackfillMatcherState, site_name)
            if row is None:
                now = datetime.now(timezone.utc)
                row = BackfillMatcherState(
                    site=site_name,
                    next_from=BACKFILL_START,
                    reset_completed=False,
                    updated_at=now,
                )
                session.add(row)
                session.commit()
            return SiteProgressState(
                site=row.site,
                next_from=row.next_from,
                reset_completed=bool(row.reset_completed),
            )

    def mark_reset_completed(self, *, site_name: str) -> SiteProgressState:
        with self._session_factory() as session:
            row = session.get(BackfillMatcherState, site_name)
            if row is None:
                row = BackfillMatcherState(
                    site=site_name,
                    next_from=BACKFILL_START,
                    reset_completed=True,
                    updated_at=datetime.now(timezone.utc),
                )
            else:
                row.reset_completed = True
                row.updated_at = datetime.now(timezone.utc)
            session.add(row)
            session.commit()
            return SiteProgressState(
                site=row.site,
                next_from=row.next_from,
                reset_completed=bool(row.reset_completed),
            )

    def mark_month_finished(self, *, site_name: str, next_from: datetime) -> SiteProgressState:
        with self._session_factory() as session:
            row = session.get(BackfillMatcherState, site_name)
            if row is None:
                row = BackfillMatcherState(
                    site=site_name,
                    next_from=next_from,
                    reset_completed=True,
                    updated_at=datetime.now(timezone.utc),
                )
            else:
                row.next_from = next_from
                row.reset_completed = True
                row.updated_at = datetime.now(timezone.utc)
            session.add(row)
            session.commit()
            return SiteProgressState(
                site=row.site,
                next_from=row.next_from,
                reset_completed=bool(row.reset_completed),
            )


async def _iter_site_names(
    *,
    client: ElasticsearchHttpClient,
    index_name: str,
    batch_size: int,
) -> list[str]:
    discovered_sites: set[str] = set()
    search_after: list[Any] | None = None

    while True:
        response = await client.search(
            index=index_name,
            query=_build_sites_query(),
            size=batch_size,
            sort=[
                {"site_name.keyword": {"order": "asc"}},
                {"_id": {"order": "asc"}},
            ],
            source=["site_name"],
            search_after=search_after,
        )
        hits = response.get("hits", {}).get("hits", [])
        if not isinstance(hits, list) or not hits:
            break

        for raw_hit in hits:
            if not isinstance(raw_hit, dict):
                continue
            source = _extract_hit_source(raw_hit)
            if source is None:
                continue
            site_name = source.get("site_name")
            if isinstance(site_name, str) and site_name.strip():
                discovered_sites.add(site_name.strip())

        last_hit = hits[-1]
        if not isinstance(last_hit, dict):
            break
        search_after = _extract_hit_sort(last_hit)
        if search_after is None:
            break

    return sorted(discovered_sites)


async def _reset_matching_state_for_site(
    *,
    client: ElasticsearchHttpClient,
    index_name: str,
    site_name: str,
    batch_size: int,
) -> int:
    logger.info("Resetting matching fields for site=%s", site_name)
    reset_count = 0
    search_after: list[Any] | None = None

    while True:
        response = await client.search(
            index=index_name,
            query=_build_site_query(site_name=site_name),
            size=batch_size,
            sort=[{"_id": {"order": "asc"}}],
            source=False,
            search_after=search_after,
        )
        hits = response.get("hits", {}).get("hits", [])
        if not isinstance(hits, list) or not hits:
            break

        doc_ids: list[str] = []
        for raw_hit in hits:
            if not isinstance(raw_hit, dict):
                continue
            hit_id = _extract_hit_id(raw_hit)
            if hit_id is not None:
                doc_ids.append(hit_id)

        if doc_ids:
            operations = _build_reset_operations(index_name=index_name, doc_ids=doc_ids)
            bulk_response = await client.bulk(operations=operations, refresh=False)
            if bulk_response.get("errors"):
                raise RuntimeError(f"Failed to reset matching state for site={site_name}.")
            reset_count += len(doc_ids)

        last_hit = hits[-1]
        if not isinstance(last_hit, dict):
            break
        search_after = _extract_hit_sort(last_hit)
        if search_after is None:
            break

    logger.info("Reset complete for site=%s docs=%s", site_name, reset_count)
    return reset_count


async def _rollback_successor_claim(
    *,
    client: ElasticsearchHttpClient,
    index_name: str,
    duplicate_id: str,
    candidate_id: str,
) -> None:
    await client.update(
        index=index_name,
        doc_id=duplicate_id,
        body={
            "script": {
                "source": """
                    if (ctx._source.successor_id == params.candidate_id) {
                        ctx._source.successor_id = null;
                    } else {
                        ctx.op = 'none';
                    }
                """,
                "params": {"candidate_id": candidate_id},
            }
        },
        refresh=True,
    )


async def _flush_predecessor_links(
    *,
    client: ElasticsearchHttpClient,
    index_name: str,
    links: list[tuple[str, str]],
) -> tuple[int, int]:
    if not links:
        return 0, 0

    operations = _build_predecessor_operations(index_name=index_name, links=links)
    response = await client.bulk(operations=operations, refresh=True)
    linked_count, conflicts = _parse_bulk_update_outcome(response=response, links=links)

    for candidate_id, duplicate_id in conflicts:
        await _rollback_successor_claim(
            client=client,
            index_name=index_name,
            duplicate_id=duplicate_id,
            candidate_id=candidate_id,
        )
    return linked_count, len(conflicts)


async def _process_site(
    *,
    site_name: str,
    processed_ads_repo: ElasticsearchProcessedAdsRepository,
    client: ElasticsearchHttpClient,
    index_name: str,
    batch_size: int,
    month_start: datetime,
    month_end: datetime,
) -> SiteStats:
    logger.info(
        "Backfill processing started for site=%s month_start=%s month_end=%s",
        site_name,
        month_start.isoformat(),
        month_end.isoformat(),
    )
    stats = SiteStats()
    search_after: list[Any] | None = None
    pending_links: list[tuple[str, str]] = []

    while True:
        hits = await processed_ads_repo.search_window(
            query=_build_site_candidates_query(
                site_name=site_name,
                lower_bound=month_start,
                upper_bound=month_end,
            ),
            batch_size=batch_size,
            search_after=search_after,
        )
        if not hits:
            break

        for raw_hit in hits:
            hit = dict(raw_hit)
            doc_id = _extract_hit_id(hit)
            source = _extract_hit_source(hit)
            if doc_id is None or source is None:
                continue

            stats.processed += 1

            try:
                candidate = _build_candidate_from_source(source, doc_id)
            except ValidationError as exc:
                logger.warning("Skipping invalid candidate doc_id=%s: %s", doc_id, exc)
                continue

            duplicate_id, score, duplicate_meta = await find_best_duplicate(
                candidate, candidate_id=doc_id
            )
            if duplicate_id is None or score < settings.matching_min_score:
                continue

            if duplicate_id == doc_id:
                logger.warning("Skipping self match doc_id=%s", doc_id)
                continue

            duplicate_offer_start = duplicate_meta.get("offer_start")
            if not isinstance(duplicate_offer_start, datetime):
                continue
            if duplicate_offer_start >= candidate.offer_start:
                continue

            stats.matches_found += 1
            claim_succeeded = await processed_ads_repo.claim_duplicate(
                duplicate_id=duplicate_id,
                candidate_id=doc_id,
            )
            if not claim_succeeded:
                stats.claim_conflicts += 1
                continue

            pending_links.append((doc_id, duplicate_id))
            if len(pending_links) >= batch_size:
                linked_count, conflicts = await _flush_predecessor_links(
                    client=client,
                    index_name=index_name,
                    links=pending_links,
                )
                stats.linked += linked_count
                stats.claim_conflicts += conflicts
                pending_links = []

        search_after = _extract_hit_sort(dict(hits[-1]))
        if search_after is None:
            logger.warning("Stopping site=%s due to missing search_after values", site_name)
            break

    if pending_links:
        linked_count, conflicts = await _flush_predecessor_links(
            client=client,
            index_name=index_name,
            links=pending_links,
        )
        stats.linked += linked_count
        stats.claim_conflicts += conflicts

    logger.info(
        "Backfill processing finished for site=%s processed=%s matches=%s linked=%s conflicts=%s",
        site_name,
        stats.processed,
        stats.matches_found,
        stats.linked,
        stats.claim_conflicts,
    )
    return stats


def _extract_string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        trimmed = value.strip()
        return trimmed or None
    return None


def _detect_cycles(successor_by_doc: dict[str, str | None]) -> list[str]:
    cycles: list[str] = []
    state: dict[str, int] = {doc_id: 0 for doc_id in successor_by_doc}

    for start_id in successor_by_doc:
        if state[start_id] != 0:
            continue
        path_index: dict[str, int] = {}
        trail: list[str] = []
        current_id: str | None = start_id

        while current_id is not None:
            current_state = state.get(current_id, 2)
            if current_state == 2:
                break
            if current_state == 1:
                cycle_start = path_index.get(current_id, 0)
                cycle_nodes = trail[cycle_start:]
                cycles.append(" -> ".join(cycle_nodes + [current_id]))
                break

            state[current_id] = 1
            path_index[current_id] = len(trail)
            trail.append(current_id)
            current_id = successor_by_doc.get(current_id)

        for doc_id in trail:
            state[doc_id] = 2

    return cycles


async def _validate_chains(
    *,
    client: ElasticsearchHttpClient,
    index_name: str,
    batch_size: int,
) -> None:
    logger.info("Validation started")
    predecessor_by_doc: dict[str, str | None] = {}
    successor_by_doc: dict[str, str | None] = {}
    validation_errors: list[str] = []
    search_after: list[Any] | None = None

    while True:
        response = await client.search(
            index=index_name,
            query={"match_all": {}},
            size=batch_size,
            sort=[{"_id": {"order": "asc"}}],
            source=VALIDATION_SOURCE_FIELDS,
            search_after=search_after,
        )
        hits = response.get("hits", {}).get("hits", [])
        if not isinstance(hits, list) or not hits:
            break

        for raw_hit in hits:
            if not isinstance(raw_hit, dict):
                continue
            doc_id = _extract_hit_id(raw_hit)
            source = _extract_hit_source(raw_hit)
            if doc_id is None or source is None:
                continue

            predecessor_value = source.get("predecessor_id")
            successor_value = source.get("successor_id")
            predecessor_id = _extract_string_or_none(predecessor_value)
            successor_id = _extract_string_or_none(successor_value)

            if predecessor_value is not None and predecessor_id is None:
                validation_errors.append(f"doc_id={doc_id} has invalid predecessor_id type")
            if successor_value is not None and successor_id is None:
                validation_errors.append(f"doc_id={doc_id} has invalid successor_id type")

            predecessor_by_doc[doc_id] = predecessor_id
            successor_by_doc[doc_id] = successor_id

        last_hit = hits[-1]
        if not isinstance(last_hit, dict):
            break
        search_after = _extract_hit_sort(last_hit)
        if search_after is None:
            break

    for doc_id, successor_id in successor_by_doc.items():
        if successor_id is None:
            continue
        linked_predecessor = predecessor_by_doc.get(successor_id)
        if linked_predecessor != doc_id:
            validation_errors.append(
                f"Inconsistent successor link: {doc_id}.successor_id={successor_id}, "
                f"but {successor_id}.predecessor_id={linked_predecessor!r}"
            )

    for doc_id, predecessor_id in predecessor_by_doc.items():
        if predecessor_id is None:
            continue
        linked_successor = successor_by_doc.get(predecessor_id)
        if linked_successor != doc_id:
            validation_errors.append(
                f"Inconsistent predecessor link: {doc_id}.predecessor_id={predecessor_id}, "
                f"but {predecessor_id}.successor_id={linked_successor!r}"
            )

    cycles = _detect_cycles(successor_by_doc)
    if cycles:
        validation_errors.extend(f"Cycle detected: {cycle}" for cycle in cycles)

    if validation_errors:
        sample = "; ".join(validation_errors[:10])
        raise RuntimeError(
            f"Backfill validation failed with {len(validation_errors)} error(s): {sample}"
        )
    logger.info("Validation passed")


async def run_backfill() -> None:
    _configure_logging(settings.log_level)
    reporter = TelegramReporter.from_settings(
        service_name="backfill_matcher", settings=settings, logger=logger
    )
    ensure_etl_state_tables(settings.postgres_database_url)
    session_factory = build_postgres_session_factory(settings.postgres_database_url)
    state_store = BackfillStateStore(session_factory=session_factory)

    client = ElasticsearchHttpClient(
        settings.elasticsearch_url,
        api_key=settings.elasticsearch_api_key,
        username=settings.elasticsearch_username,
        password=settings.elasticsearch_password,
    )
    processed_ads_repo = ElasticsearchProcessedAdsRepository(
        client=client,
        index_name=settings.processed_index,
    )
    configure_matcher(client=processed_ads_repo, index_name=settings.processed_index)
    await processed_ads_repo.ensure_mapping(body=MATCHING_MAPPING_FIELDS)

    configured_sites = settings.matching_sites
    if configured_sites:
        sites = sorted({site.strip() for site in configured_sites if site.strip()})
    else:
        sites = await _iter_site_names(
            client=client,
            index_name=settings.processed_index,
            batch_size=settings.matching_batch_size,
        )
    if not sites:
        logger.info("No sites found for backfill")
        return

    logger.info(
        "Backfill started index=%s sites=%s start_from=%s",
        settings.processed_index,
        len(sites),
        BACKFILL_START.isoformat(),
    )

    total_reset = 0
    for site_name in sites:
        site_state = state_store.get_or_create(site_name=site_name)
        if not site_state.reset_completed:
            total_reset += await _reset_matching_state_for_site(
                client=client,
                index_name=settings.processed_index,
                site_name=site_name,
                batch_size=settings.matching_batch_size,
            )
            state_store.mark_reset_completed(site_name=site_name)
    _ = await client.refresh_index(index=settings.processed_index)
    logger.info("Reset completed across all sites docs=%s", total_reset)

    aggregate = SiteStats()
    run_until_exclusive = _current_month_start(datetime.now(timezone.utc))
    for site_name in sites:
        site_state = state_store.get_or_create(site_name=site_name)
        month_start = site_state.next_from

        while month_start < run_until_exclusive:
            month_end = _month_window_end(month_start)
            site_stats = await _process_site(
                site_name=site_name,
                processed_ads_repo=processed_ads_repo,
                client=client,
                index_name=settings.processed_index,
                batch_size=settings.matching_batch_size,
                month_start=month_start,
                month_end=month_end,
            )
            aggregate.processed += site_stats.processed
            aggregate.matches_found += site_stats.matches_found
            aggregate.claim_conflicts += site_stats.claim_conflicts
            aggregate.linked += site_stats.linked

            checkpoint = state_store.mark_month_finished(site_name=site_name, next_from=month_end)
            await reporter.send_progress(
                (
                    f"site={site_name} status=month_finished from={month_start.isoformat()} "
                    f"to={month_end.isoformat()} next_from={checkpoint.next_from.isoformat()} "
                    f"processed={site_stats.processed} matches={site_stats.matches_found} "
                    f"linked={site_stats.linked} conflicts={site_stats.claim_conflicts}"
                )
            )
            month_start = month_end

    _ = await client.refresh_index(index=settings.processed_index)
    await _validate_chains(
        client=client,
        index_name=settings.processed_index,
        batch_size=settings.matching_batch_size,
    )

    logger.info(
        "Backfill completed processed=%s matches=%s linked=%s conflicts=%s",
        aggregate.processed,
        aggregate.matches_found,
        aggregate.linked,
        aggregate.claim_conflicts,
    )


async def main() -> None:
    await run_backfill()


if __name__ == "__main__":
    asyncio.run(main())
