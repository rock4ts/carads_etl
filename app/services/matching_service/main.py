"""Standalone duplicate-linking worker for processed car ads."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import datetime
from typing import Any

from pydantic import ValidationError

from app.repositories.elasticsearch_processed_ads import ElasticsearchProcessedAdsRepository
from app.services.matching_service.core.config import settings
from app.services.matching_service.matcher import configure_matcher, find_best_duplicate
from app.shared.clients import ElasticsearchHttpClient
from app.shared.database.session import build_postgres_session_factory
from app.shared.schemas.processed import CaradDocData
from app.uow.matching_state_uow import (
    MatchingStateUnitOfWork,
    SqlAlchemyMatchingStateUnitOfWork,
)

logger = logging.getLogger(__name__)
MATCHING_MAPPING_FIELDS = {
    "properties": {
        "predecessor_id": {"type": "keyword"},
        "successor_id": {"type": "keyword"},
    }
}

StateUowFactory = Callable[[], MatchingStateUnitOfWork]


def _configure_logging(log_level: str) -> None:
    level = getattr(logging, log_level.strip().upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def _build_candidate(source: Mapping[str, Any], doc_id: str) -> CaradDocData:
    now = datetime.now()
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


def _build_new_docs_query(site_name: str, lower_bound: datetime, upper_bound: datetime) -> dict[str, Any]:
    return {
        "bool": {
            "filter": [
                {"term": {"site_name.keyword": site_name}},
                {"bool": {"must_not": [{"exists": {"field": "predecessor_id"}}]}},
                {
                    "range": {
                        "offer_start": {
                            "gte": lower_bound.isoformat(),
                            "lte": upper_bound.isoformat(),
                        }
                    }
                },
            ],
        }
    }


def _extract_hit_id(hit: Mapping[str, Any]) -> str | None:
    hit_id = hit.get("_id")
    if hit_id is None:
        return None
    return str(hit_id)


def _extract_hit_source(hit: Mapping[str, Any]) -> dict[str, Any] | None:
    source = hit.get("_source")
    if isinstance(source, dict):
        return source
    return None


def _extract_hit_sort(hit: Mapping[str, Any]) -> list[Any] | None:
    sort_values = hit.get("sort")
    if isinstance(sort_values, list) and sort_values:
        return list(sort_values)
    return None


def _extract_offer_start(hit: Mapping[str, Any], source: Mapping[str, Any]) -> datetime | None:
    sort_values = hit.get("sort")
    if isinstance(sort_values, list) and sort_values:
        parsed = _parse_datetime(sort_values[0])
        if parsed is not None:
            return parsed
    return _parse_datetime(source.get("offer_start"))


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


async def _process_site(
    *,
    site_name: str,
    state_uow_factory: StateUowFactory,
    processed_ads_repo: ElasticsearchProcessedAdsRepository,
    batch_size: int,
) -> None:
    with state_uow_factory() as uow:
        upload_timestamp = uow.matching_states.get_upload_timestamp(site_name)
        if upload_timestamp is None:
            logger.info("Skipping site %s: missing upload timestamp", site_name)
            return

        marker_timestamp = uow.matching_states.get_marker_timestamp(site_name)
        if marker_timestamp is None:
            uow.matching_states.upsert_marker_timestamp(site_name, upload_timestamp)
            uow.commit()
            logger.info(
                "Initialized marker timestamp for %s at %s; nothing to process until next upload window",
                site_name,
                upload_timestamp.isoformat(),
            )
            return

        lower_bound = marker_timestamp
        upper_bound = upload_timestamp
        if lower_bound >= upper_bound:
            logger.info(
                "Skipping site %s: marker %s is already at or ahead of upload %s",
                site_name,
                lower_bound.isoformat(),
                upper_bound.isoformat(),
            )
            return

        processed_docs = 0
        matches_found = 0
        claim_successes = 0
        claim_failures = 0
        linked_docs = 0
        search_after: list[Any] | None = None

        logger.info(
            "Processing site %s in window [%s, %s]",
            site_name,
            lower_bound.isoformat(),
            upper_bound.isoformat(),
        )

        while True:
            hits = await processed_ads_repo.search_window(
                query=_build_new_docs_query(site_name, lower_bound, upper_bound),
                batch_size=batch_size,
                search_after=search_after,
            )
            if not hits:
                break

            last_processed_offer_start = marker_timestamp
            pending_links: list[tuple[str, str]] = []
            for hit in hits:
                doc_id = _extract_hit_id(hit)
                source = _extract_hit_source(hit)
                if doc_id is None or source is None:
                    continue

                processed_docs += 1
                offer_start = _extract_offer_start(hit, source)
                if offer_start is not None:
                    last_processed_offer_start = offer_start

                try:
                    candidate = _build_candidate(source, doc_id)
                except ValidationError as exc:
                    logger.warning("Skipping invalid document %s: %s", doc_id, exc)
                    continue

                if candidate.predecessor_id is not None:
                    logger.debug("Skip %s: reason=already_linked predecessor_id=%s", doc_id, candidate.predecessor_id)
                    continue

                if candidate.offer_start is None:
                    logger.warning("Skip %s: reason=missing_offer_start source=candidate", doc_id)
                    continue

                duplicate_id, score, duplicate_meta = await find_best_duplicate(candidate, candidate_id=doc_id)
                if duplicate_id is None or score < settings.matching_min_score:
                    logger.debug("Skip %s: reason=low_score score=%.6f", doc_id, score)
                    continue

                if duplicate_id == doc_id:
                    logger.warning("Skip %s: reason=self_match duplicate_id=%s", doc_id, duplicate_id)
                    continue

                duplicate_offer_start = duplicate_meta.get("offer_start")
                if duplicate_offer_start is None:
                    logger.debug("Skip %s: reason=missing_offer_start source=duplicate duplicate_id=%s", doc_id, duplicate_id)
                    continue

                if duplicate_offer_start >= candidate.offer_start:
                    logger.warning(
                        "Skip %s: reason=temporal_violation dup=%s cand=%s duplicate_id=%s",
                        doc_id,
                        duplicate_offer_start,
                        candidate.offer_start,
                        duplicate_id,
                    )
                    continue

                matches_found += 1
                logger.info(
                    "Match found for %s -> %s (score=%.6f)",
                    doc_id,
                    duplicate_id,
                    score,
                )

                claim_succeeded = await processed_ads_repo.claim_duplicate(
                    duplicate_id=duplicate_id,
                    candidate_id=doc_id,
                )
                if not claim_succeeded:
                    claim_failures += 1
                    logger.info("Claim failed for duplicate %s -> %s", duplicate_id, doc_id)
                    continue

                claim_successes += 1
                pending_links.append((doc_id, duplicate_id))
                logger.info("Claim succeeded for duplicate %s -> %s", duplicate_id, doc_id)

            if pending_links:
                linked_now = await processed_ads_repo.link_predecessors(links=pending_links)
                linked_docs += linked_now
                logger.info("Bulk-linked predecessor_id for %s documents", linked_now)

            marker_timestamp = last_processed_offer_start
            uow.matching_states.upsert_marker_timestamp(site_name, marker_timestamp)
            uow.commit()
            logger.info(
                "Committed marker for %s at %s after batch: processed=%s matches=%s claims_ok=%s claims_failed=%s linked=%s",
                site_name,
                marker_timestamp.isoformat(),
                processed_docs,
                matches_found,
                claim_successes,
                claim_failures,
                linked_docs,
            )

            search_after = _extract_hit_sort(hits[-1])
            if search_after is None:
                logger.warning("Stopping site %s: last hit is missing search_after sort values", site_name)
                break

        logger.info(
            "Finished site %s: processed=%s matches=%s claims_ok=%s claims_failed=%s linked=%s",
            site_name,
            processed_docs,
            matches_found,
            claim_successes,
            claim_failures,
            linked_docs,
        )


def _iter_sites(upload_sites: Sequence[str], requested_sites: Iterable[str] | None) -> list[str]:
    upload_sites_list = list(upload_sites)
    if requested_sites is None:
        return upload_sites_list
    requested = {site.strip() for site in requested_sites if site.strip()}
    return [site for site in upload_sites_list if site in requested]


async def _run() -> None:
    session_factory = build_postgres_session_factory(settings.postgres_database_url)

    client = ElasticsearchHttpClient(
        settings.elasticsearch_url,
        api_key=settings.elasticsearch_api_key,
        username=settings.elasticsearch_username,
        password=settings.elasticsearch_password,
    )
    processed_ads_repo = ElasticsearchProcessedAdsRepository(client=client, index_name=settings.processed_index)
    configure_matcher(client=processed_ads_repo, index_name=settings.processed_index)

    await processed_ads_repo.ensure_mapping(body=MATCHING_MAPPING_FIELDS)
    logger.info("Ensured ES mapping fields exist for %s", settings.processed_index)

    def _state_uow_factory() -> SqlAlchemyMatchingStateUnitOfWork:
        return SqlAlchemyMatchingStateUnitOfWork(session_factory)

    with _state_uow_factory() as uow:
        sites = _iter_sites(uow.matching_states.list_upload_sites(), settings.matching_sites)
    if not sites:
        logger.info("No sites available for matching")
        return

    for site_name in sites:
        await _process_site(
            site_name=site_name,
            state_uow_factory=_state_uow_factory,
            processed_ads_repo=processed_ads_repo,
            batch_size=settings.matching_batch_size,
        )


def main() -> None:
    _configure_logging(settings.log_level)
    asyncio.run(_run())


if __name__ == "__main__":
    main()
