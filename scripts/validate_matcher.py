"""Validate duplicate matcher quality on real Elasticsearch data.

This script is read-only: it only uses search/mget APIs and does not modify data.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib import error, request

from pydantic import ValidationError

from app.services.matching_service.matcher import DuplicateMatcher
from app.schemas.processed import CaradDocData

DEFAULT_INDEX = "carads1_local"
DEFAULT_SAMPLE_SIZE = 1000
DEFAULT_RECENT_DAYS = 2
# Matcher score is:
#   (3.0 * offer_end_proximity + 2.0 * price_proximity + 1.5 * mileage_proximity) / 6.5
# 0.65 is a practical "strong match" cutoff for manual inspection.
HIGH_SCORE_THRESHOLD = 0.65
MAX_CANDIDATES_TO_SCAN = 100000
MAX_SAMPLE_FETCH = 1000
DEFAULT_OUTPUT_FILE = "matcher_validation_results.json"
MIN_DUPLICATES_FOUND = 5
MAX_SEARCH_PAGES = 25
SCROLL_TTL = "2m"


def _env_int(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        return int(raw_value)
    except ValueError:
        return default


class ElasticsearchHttpClient:
    """Minimal async-compatible Elasticsearch client for read-only calls."""

    def __init__(self, base_url: str) -> None:
        self._base_url = base_url.rstrip("/")

    async def search(self, **kwargs: Any) -> dict[str, Any]:
        index = kwargs.get("index")
        if not index:
            raise ValueError("search() requires 'index'.")

        body: dict[str, Any] = {}
        for key in ("query", "size", "sort", "collapse"):
            if key in kwargs and kwargs[key] is not None:
                body[key] = kwargs[key]
        if "from_" in kwargs and kwargs["from_"] is not None:
            body["from"] = kwargs["from_"]
        if "from" in kwargs and kwargs["from"] is not None:
            body["from"] = kwargs["from"]
        if "source" in kwargs and kwargs["source"] is not None:
            # Elasticsearch search API expects `_source`, while matcher passes `source`.
            body["_source"] = kwargs["source"]
        if "_source" in kwargs and kwargs["_source"] is not None:
            body["_source"] = kwargs["_source"]
        if "track_total_hits" in kwargs:
            body["track_total_hits"] = kwargs["track_total_hits"]

        return await self._post_json(f"/{index}/_search", body)

    async def mget(
        self,
        *,
        index: str,
        ids: list[str],
        source_includes: list[str] | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any]
        if source_includes:
            body = {
                "docs": [
                    {
                        "_id": doc_id,
                        "_source": source_includes,
                    }
                    for doc_id in ids
                ]
            }
        else:
            body = {"ids": ids}
        return await self._post_json(f"/{index}/_mget", body)

    async def start_scroll(
        self,
        *,
        index: str,
        scroll_ttl: str,
        size: int,
        query: dict[str, Any],
        sort: list[dict[str, Any]],
    ) -> dict[str, Any]:
        body = {"size": size, "query": query, "sort": sort}
        return await self._post_json(f"/{index}/_search?scroll={scroll_ttl}", body)

    async def continue_scroll(self, *, scroll_id: str, scroll_ttl: str) -> dict[str, Any]:
        body = {
            "scroll": scroll_ttl,
            "scroll_id": scroll_id,
        }
        return await self._post_json("/_search/scroll", body)

    async def clear_scroll(self, *, scroll_id: str) -> None:
        await self._post_json("/_search/scroll", {"scroll_id": [scroll_id]})

    async def _post_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        return await asyncio.to_thread(self._post_json_sync, path, payload)

    def _post_json_sync(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        url = f"{self._base_url}{path}"
        data = json.dumps(payload).encode("utf-8")
        req = request.Request(
            url=url,
            data=data,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with request.urlopen(req, timeout=30) as response:
                response_body = response.read().decode("utf-8")
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Elasticsearch HTTP {exc.code} at {url}: {detail}") from exc
        except error.URLError as exc:
            raise RuntimeError(f"Failed to reach Elasticsearch at {url}: {exc}") from exc

        try:
            return json.loads(response_body)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid JSON response from Elasticsearch at {url}") from exc


def _build_candidate(source: dict[str, Any]) -> CaradDocData:
    now = datetime.now(tz=UTC)
    offer_start = source.get("offer_start") or source.get("offer_end") or source.get("last_checked") or now
    latest_price = source.get("latest_price")
    initial_price = source.get("initial_price", latest_price if latest_price is not None else 0.0)

    payload = dict(source)
    payload.setdefault("url", "")
    payload.setdefault("site_name", "unknown")
    payload.setdefault("seller_type", "unknown")
    payload.setdefault("name", "")
    payload.setdefault("last_checked", now)
    payload.setdefault("parsed_at", now)
    payload.setdefault("offer_start", offer_start)
    payload.setdefault("trans_history", [])
    payload.setdefault("initial_price", initial_price if initial_price is not None else 0.0)
    payload.setdefault("latest_price", latest_price if latest_price is not None else payload["initial_price"])
    payload.setdefault("is_new", False)
    payload.setdefault("brand", payload.get("brand") or "")
    payload.setdefault("model", payload.get("model") or "")

    return CaradDocData.model_validate(payload)


def _get_hit_source(hit: dict[str, Any]) -> dict[str, Any] | None:
    source = hit.get("_source")
    if isinstance(source, dict):
        return source
    return None


def _get_hit_id(hit: dict[str, Any]) -> str | None:
    hit_id = hit.get("_id")
    if hit_id is None:
        return None
    return str(hit_id)


async def _fetch_sample_hits(
    client: ElasticsearchHttpClient,
    *,
    index: str,
    sample_size: int,
    recent_days: int,
    scroll_id: str | None = None,
) -> tuple[list[dict[str, Any]], str | None]:
    window_start = (
        datetime.fromisoformat("2026-04-05 21:00:18").replace(tzinfo=UTC) - timedelta(days=recent_days)
    ).isoformat()
    fetch_size = min(MAX_SAMPLE_FETCH, max(sample_size * 4, 400))

    # Matcher now heavily relies on exact/missing-field clauses.
    # Sampling candidates with the core fields present yields more realistic validation.
    required_candidate_fields = [
        "brand",
        "model",
        "site_name",
        "seller_type",
        "name",
        "offer_end",
        "latest_price",
        "is_new",
    ]

    query = {
        "bool": {
            "filter": [{"exists": {"field": field_name}} for field_name in required_candidate_fields]
            + [{"range": {"offer_end": {"gte": window_start}}}]
        }
    }
    sort = [
        {"offer_end": {"order": "desc"}},
        {"_doc": {"order": "asc"}},
    ]

    if scroll_id is None:
        response = await client.start_scroll(
            index=index,
            scroll_ttl=SCROLL_TTL,
            size=fetch_size,
            query=query,
            sort=sort,
        )
    else:
        response = await client.continue_scroll(scroll_id=scroll_id, scroll_ttl=SCROLL_TTL)

    hits = response.get("hits", {}).get("hits", [])
    if not isinstance(hits, list):
        return [], None
    next_scroll_id = response.get("_scroll_id")
    if not isinstance(next_scroll_id, str) or not next_scroll_id.strip():
        next_scroll_id = None

    per_group_cap = 8
    grouped_counts: dict[tuple[str, str], int] = defaultdict(int)
    selected: list[dict[str, Any]] = []
    overflow: list[dict[str, Any]] = []

    for hit in hits:
        if not isinstance(hit, dict):
            continue
        source = _get_hit_source(hit)
        if source is None:
            continue
        brand = str(source.get("brand") or "").strip().lower()
        model = str(source.get("model") or "").strip().lower()
        if not brand or not model:
            continue

        group_key = (brand, model)
        if grouped_counts[group_key] < per_group_cap and len(selected) < sample_size:
            grouped_counts[group_key] += 1
            selected.append(hit)
        else:
            overflow.append(hit)

        if len(selected) >= sample_size:
            break

    if len(selected) < sample_size:
        for hit in overflow:
            if len(selected) >= sample_size:
                break
            selected.append(hit)

    return selected, next_scroll_id


def _pick_doc_fields(doc: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(doc, dict):
        return {}
    return {
        "brand": doc.get("brand"),
        "model": doc.get("model"),
        "year": doc.get("build_year"),
        "price": doc.get("latest_price"),
        "mileage": doc.get("mileage"),
        "name": doc.get("name"),
    }


async def _fetch_docs_for_ids(
    client: ElasticsearchHttpClient,
    *,
    index: str,
    ids: list[str],
) -> dict[str, dict[str, Any]]:
    if not ids:
        return {}

    response = await client.mget(
        index=index,
        ids=ids,
        source_includes=[
            "brand",
            "model",
            "build_year",
            "latest_price",
            "mileage",
            "name",
        ],
    )
    docs = response.get("docs", [])
    if not isinstance(docs, list):
        return {}

    results: dict[str, dict[str, Any]] = {}
    for item in docs:
        if not isinstance(item, dict):
            continue
        doc_id = item.get("_id")
        source = item.get("_source")
        if doc_id is None or not isinstance(source, dict):
            continue
        results[str(doc_id)] = source
    return results


def _resolve_output_path() -> Path:
    output_value = os.getenv("VALIDATE_MATCHER_OUTPUT", DEFAULT_OUTPUT_FILE).strip()
    if not output_value:
        output_value = DEFAULT_OUTPUT_FILE
    output_path = Path(output_value)
    if not output_path.is_absolute():
        output_path = Path.cwd() / output_path
    return output_path


def _write_results_file(
    *,
    output_path: Path,
    rows: list[dict[str, Any]],
    high_score_rows: list[dict[str, Any]],
    docs_by_id: dict[str, dict[str, Any]],
) -> None:
    high_score_inspection: list[dict[str, Any]] = []
    for row in high_score_rows:
        candidate_id = row["candidate_id"]
        duplicate_id = row["duplicate_id"]
        high_score_inspection.append(
            {
                "candidate_id": candidate_id,
                "duplicate_id": duplicate_id,
                "score": row["score"],
                "candidate": _pick_doc_fields(docs_by_id.get(candidate_id)),
                "duplicate": _pick_doc_fields(docs_by_id.get(duplicate_id)),
            }
        )

    payload = {
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "rows": rows,
        "high_score_threshold": HIGH_SCORE_THRESHOLD,
        "high_score_rows": high_score_rows,
        "high_score_inspection": high_score_inspection,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


async def run() -> None:
    es_url = os.getenv("ELASTICSEARCH_URL", "http://localhost:19200")
    index_name = os.getenv("PROCESSED_INDEX", DEFAULT_INDEX)
    sample_size = max(100, min(_env_int("SAMPLE_SIZE", DEFAULT_SAMPLE_SIZE), 3000))
    recent_days = max(1, _env_int("RECENT_DAYS", DEFAULT_RECENT_DAYS))
    max_results = max(1, _env_int("MATCHER_MAX_RESULTS", 200))
    min_duplicates_found = max(1, _env_int("MIN_DUPLICATES_FOUND", MIN_DUPLICATES_FOUND))
    max_candidates_to_scan = max(
        sample_size,
        _env_int("MAX_CANDIDATES_TO_SCAN", sample_size * 5),
    )
    output_path = _resolve_output_path()

    client = ElasticsearchHttpClient(es_url)
    matcher = DuplicateMatcher(client=client, index_name=index_name, max_results=max_results)

    rows: list[dict[str, Any]] = []
    high_score_rows: list[dict[str, Any]] = []
    duplicate_matches_found = 0
    search_pages = 0
    scroll_id: str | None = None

    print(
        f"Searching candidates in index '{index_name}' "
        f"(recent_days={recent_days}, es_url={es_url}, min_duplicates={min_duplicates_found})."
    )

    while duplicate_matches_found < min_duplicates_found and len(rows) < max_candidates_to_scan:
        remaining_budget = max_candidates_to_scan - len(rows)
        if remaining_budget <= 0:
            break

        requested_batch_size = min(sample_size, remaining_budget)
        batch_hits, scroll_id = await _fetch_sample_hits(
            client,
            index=index_name,
            sample_size=requested_batch_size,
            recent_days=recent_days,
            scroll_id=scroll_id,
        )

        if not batch_hits:
            if search_pages == 0:
                print("No sample ads were fetched.")
                return
            break

        for hit in batch_hits:
            if not isinstance(hit, dict):
                continue
            candidate_id = _get_hit_id(hit)
            source = _get_hit_source(hit)
            if candidate_id is None or source is None:
                continue

            try:
                candidate = _build_candidate(source)
            except ValidationError as exc:
                print(f"Skipping candidate {candidate_id}: invalid document ({exc.errors()[0]['msg']})")
                continue

            duplicate_id, score, _ = await matcher.find_best_duplicate(candidate, candidate_id)
            row = {
                "candidate_id": candidate_id,
                "duplicate_id": duplicate_id,
                "score": round(float(score), 6),
            }
            rows.append(row)

            if duplicate_id is not None:
                duplicate_matches_found += 1
                if score > HIGH_SCORE_THRESHOLD:
                    high_score_rows.append(row)

            if len(rows) >= max_candidates_to_scan:
                break

        print(f"Scanned {len(rows)} candidates so far; found {duplicate_matches_found} with predicted duplicates.")
        if scroll_id is None:
            break

    if scroll_id is not None:
        try:
            await client.clear_scroll(scroll_id=scroll_id)
        except RuntimeError:
            # Best-effort cleanup; validation output is still valid if this fails.
            pass

    if duplicate_matches_found < min_duplicates_found:
        print(
            f"Only found {duplicate_matches_found} predicted duplicates after scanning "
            f"{len(rows)} candidates (target={min_duplicates_found})."
        )

    print(json.dumps(rows, ensure_ascii=False, indent=2))

    print(f"\n--- Manual inspection (score > {HIGH_SCORE_THRESHOLD}) ---")
    docs_by_id: dict[str, dict[str, Any]] = {}
    if not high_score_rows:
        print("No high-score matches found.")
    else:
        ids_for_lookup = list(
            {
                doc_id
                for row in high_score_rows
                for doc_id in (row["candidate_id"], row["duplicate_id"])
                if isinstance(doc_id, str) and doc_id
            }
        )
        docs_by_id = await _fetch_docs_for_ids(client, index=index_name, ids=ids_for_lookup)

        for row in high_score_rows:
            candidate_id = row["candidate_id"]
            duplicate_id = row["duplicate_id"]
            score = row["score"]
            candidate_doc = docs_by_id.get(candidate_id)
            duplicate_doc = docs_by_id.get(duplicate_id)

            print(f"\nscore={score} | candidate_id={candidate_id} | duplicate_id={duplicate_id}")
            print(f"candidate: {_pick_doc_fields(candidate_doc)}")
            print(f"duplicate: {_pick_doc_fields(duplicate_doc)}")

    _write_results_file(
        output_path=output_path,
        rows=rows,
        high_score_rows=high_score_rows,
        docs_by_id=docs_by_id,
    )
    print(f"\nWrote validation results to {output_path}")


if __name__ == "__main__":
    asyncio.run(run())
