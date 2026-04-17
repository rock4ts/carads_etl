"""Run matching worker on a safe copy of a production-like index.

This script is additive-only and does not modify existing repository files.
It automates:
1) target index creation from source mappings/settings
2) full data copy (reindex)
3) link-field reset (predecessor_id/successor_id)
4) one-site hour-based window selection
5) marker/upload timestamp seeding
6) worker run + metrics collection
7) sampling + chain checks + idempotency rerun
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib import error, request

from sqlalchemy import create_engine, text

SOURCE_INDEX_DEFAULT = "carads1_local"
TARGET_INDEX_DEFAULT = "carads1_local_test"
ES_URL_DEFAULT = "http://localhost:19200"
POSTGRES_URL_DEFAULT = "postgresql+psycopg://postgres:postgres@localhost:5432/car_intel"
WORKER_MODULE_DEFAULT = "app.services.matching_service.main"
WINDOW_HOURS_DEFAULT = 48
SAMPLE_SIZE_DEFAULT = 20
SCROLL_TTL = "2m"
SCROLL_BATCH_SIZE = 2000

UNSAFE_INDEX_SETTING_KEYS = {
    "creation_date",
    "history_uuid",
    "provided_name",
    "resize",
    "routing",
    "uuid",
    "version",
    "verified_before_close",
}

FINISHED_SITE_RE = re.compile(
    r"Finished site (?P<site>.+?): "
    r"processed=(?P<processed>\d+) "
    r"matches=(?P<matches>\d+) "
    r"claims_ok=(?P<claims_ok>\d+) "
    r"claims_failed=(?P<claims_failed>\d+) "
    r"linked=(?P<linked>\d+)"
)
COMMITTED_BATCH_RE = re.compile(
    r"Committed marker for (?P<site>.+?) at .*? after batch: "
    r"processed=(?P<processed>\d+) "
    r"matches=(?P<matches>\d+) "
    r"claims_ok=(?P<claims_ok>\d+) "
    r"claims_failed=(?P<claims_failed>\d+) "
    r"linked=(?P<linked>\d+)"
)


@dataclass(frozen=True)
class WorkerMetrics:
    site_name: str
    total_docs_processed: int
    matches_found: int
    claim_success: int
    claim_failed: int
    linked_docs: int


class ElasticsearchHttpClient:
    """Minimal Elasticsearch HTTP client for dry-run orchestration."""

    def __init__(self, base_url: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._headers: dict[str, str] = {"Content-Type": "application/json"}

        api_key = os.getenv("ELASTICSEARCH_API_KEY")
        username = os.getenv("ELASTICSEARCH_USERNAME")
        password = os.getenv("ELASTICSEARCH_PASSWORD")
        if api_key:
            self._headers["Authorization"] = f"ApiKey {api_key}"
        elif username and password:
            auth = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
            self._headers["Authorization"] = f"Basic {auth}"

    def get_json(self, path: str) -> dict[str, Any]:
        return self._request_json("GET", path)

    def put_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request_json("PUT", path, payload=payload)

    def post_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request_json("POST", path, payload=payload)

    def delete(
        self,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        ignore_statuses: set[int] | None = None,
    ) -> dict[str, Any]:
        return self._request_json("DELETE", path, payload=payload, ignore_statuses=ignore_statuses)

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        ignore_statuses: set[int] | None = None,
    ) -> dict[str, Any]:
        url = f"{self._base_url}{path}"
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        req = request.Request(url=url, data=data, method=method, headers=self._headers)
        try:
            with request.urlopen(req, timeout=120) as response:
                raw = response.read().decode("utf-8")
        except error.HTTPError as exc:
            if ignore_statuses and exc.code in ignore_statuses:
                return {}
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Elasticsearch HTTP {exc.code} at {url}: {detail}") from exc
        except error.URLError as exc:
            raise RuntimeError(f"Failed to reach Elasticsearch at {url}: {exc}") from exc

        if not raw:
            return {}
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid JSON response from Elasticsearch at {url}") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--es-url", default=os.getenv("ELASTICSEARCH_URL", ES_URL_DEFAULT))
    parser.add_argument("--source-index", default=SOURCE_INDEX_DEFAULT)
    parser.add_argument("--target-index", default=TARGET_INDEX_DEFAULT)
    parser.add_argument(
        "--postgres-url",
        default=os.getenv("POSTGRES_DATABASE_URL", os.getenv("DATABASE_URL", POSTGRES_URL_DEFAULT)),
    )
    parser.add_argument("--site-name", default=None, help="Optional site override; auto-picks if omitted.")
    parser.add_argument("--window-hours", type=int, default=WINDOW_HOURS_DEFAULT)
    parser.add_argument("--sample-size", type=int, default=SAMPLE_SIZE_DEFAULT)
    parser.add_argument("--worker-module", default=WORKER_MODULE_DEFAULT)
    parser.add_argument(
        "--reindex-timeout-seconds",
        type=int,
        default=7200,
        help="Maximum time to wait for _reindex task completion.",
    )
    parser.add_argument(
        "--reindex-poll-seconds",
        type=int,
        default=5,
        help="Polling interval while waiting for _reindex task completion.",
    )
    parser.add_argument(
        "--reset-timeout-seconds",
        type=int,
        default=7200,
        help="Maximum time to wait for _update_by_query reset completion.",
    )
    parser.add_argument(
        "--reset-poll-seconds",
        type=int,
        default=5,
        help="Polling interval while waiting for reset task completion.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional output directory for artifacts. Defaults to artifacts/matching_dry_run/<timestamp>.",
    )
    return parser.parse_args()


def validate_safety(args: argparse.Namespace) -> None:
    if args.source_index == args.target_index:
        raise ValueError("Source and target indexes must be different.")
    if not str(args.target_index).endswith("_test"):
        raise ValueError("Target index must end with '_test' for safety.")


def validate_elasticsearch_connection(client: ElasticsearchHttpClient, *, es_url: str) -> None:
    try:
        client.get_json("/_cluster/health")
    except RuntimeError as exc:
        raise RuntimeError(
            "Elasticsearch is not reachable. "
            f"Tried '{es_url}'. "
            "Set ELASTICSEARCH_URL or pass --es-url (for this repo, localhost:19200 is typical)."
        ) from exc


def get_index_doc_count(client: ElasticsearchHttpClient, *, index_name: str) -> int:
    response = client.post_json(f"/{index_name}/_count", {"query": {"match_all": {}}})
    count = response.get("count")
    if not isinstance(count, int):
        raise RuntimeError(f"Unexpected _count response for index '{index_name}': {response}")
    return count


def should_skip_copy_for_existing_target(
    client: ElasticsearchHttpClient,
    *,
    target_index: str,
) -> bool:
    try:
        target_count = get_index_doc_count(client, index_name=target_index)
    except RuntimeError as exc:
        if "HTTP 404" in str(exc):
            return False
        raise
    return target_count > 0


def count_docs_with_link_fields(client: ElasticsearchHttpClient, *, index_name: str) -> int:
    response = client.post_json(
        f"/{index_name}/_count",
        {
            "query": {
                "bool": {
                    "should": [
                        {"exists": {"field": "predecessor_id"}},
                        {"exists": {"field": "successor_id"}},
                    ],
                    "minimum_should_match": 1,
                }
            }
        },
    )
    count = response.get("count")
    if not isinstance(count, int):
        raise RuntimeError(f"Unexpected link-fields _count response for index '{index_name}': {response}")
    return count


def clean_index_settings(index_settings: dict[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for key, value in index_settings.items():
        if key in UNSAFE_INDEX_SETTING_KEYS:
            continue
        cleaned[key] = value
    return cleaned


def recreate_target_index_from_source(
    client: ElasticsearchHttpClient,
    *,
    source_index: str,
    target_index: str,
) -> dict[str, Any]:
    source_def_payload = client.get_json(f"/{source_index}")
    source_def = source_def_payload.get(source_index)
    if not isinstance(source_def, dict):
        raise RuntimeError(f"Could not read source index definition for '{source_index}'.")

    source_settings = source_def.get("settings", {}).get("index", {})
    if not isinstance(source_settings, dict):
        source_settings = {}

    create_body: dict[str, Any] = {
        "mappings": source_def.get("mappings", {}),
    }
    cleaned = clean_index_settings(source_settings)
    if cleaned:
        create_body["settings"] = {"index": cleaned}

    client.delete(f"/{target_index}", ignore_statuses={404})
    client.put_json(f"/{target_index}", create_body)

    mapping_response = client.put_json(
        f"/{target_index}/_mapping",
        {
            "properties": {
                "predecessor_id": {"type": "keyword"},
                "successor_id": {"type": "keyword"},
            }
        },
    )
    return mapping_response


def run_reindex_copy(
    client: ElasticsearchHttpClient,
    *,
    source_index: str,
    target_index: str,
    timeout_seconds: int,
    poll_seconds: int,
) -> dict[str, Any]:
    if timeout_seconds <= 0:
        raise ValueError("--reindex-timeout-seconds must be positive.")
    if poll_seconds <= 0:
        raise ValueError("--reindex-poll-seconds must be positive.")

    start_response = client.post_json(
        "/_reindex?wait_for_completion=false&refresh=true",
        {
            "source": {"index": source_index},
            "dest": {"index": target_index},
        },
    )
    task_id = start_response.get("task")
    if not isinstance(task_id, str) or not task_id.strip():
        raise RuntimeError(f"Could not get _reindex task id: {start_response}")

    deadline = time.monotonic() + timeout_seconds
    while True:
        task_response = client.get_json(f"/_tasks/{task_id}")
        if bool(task_response.get("completed")):
            response = task_response.get("response")
            if not isinstance(response, dict):
                raise RuntimeError(f"_reindex task finished without valid response: {task_response}")
            break

        task_error = task_response.get("error")
        if task_error is not None:
            raise RuntimeError(f"_reindex task failed: {json.dumps(task_error, ensure_ascii=False)}")

        if time.monotonic() >= deadline:
            raise RuntimeError(f"_reindex did not finish within {timeout_seconds}s. Task is still running: {task_id}")
        time.sleep(poll_seconds)

    failures = response.get("failures")
    if isinstance(failures, list) and failures:
        raise RuntimeError(f"Reindex reported failures: {json.dumps(failures[:3], ensure_ascii=False)}")
    return response


def reset_matching_fields(
    client: ElasticsearchHttpClient,
    *,
    target_index: str,
    timeout_seconds: int,
    poll_seconds: int,
) -> tuple[dict[str, Any], bool]:
    if timeout_seconds <= 0:
        raise ValueError("--reset-timeout-seconds must be positive.")
    if poll_seconds <= 0:
        raise ValueError("--reset-poll-seconds must be positive.")

    linked_docs_count = count_docs_with_link_fields(client, index_name=target_index)
    if linked_docs_count == 0:
        return {
            "total": 0,
            "updated": 0,
            "version_conflicts": 0,
            "reason": "no_docs_with_both_predecessor_and_successor",
        }, True

    start_response = client.post_json(
        f"/{target_index}/_update_by_query?wait_for_completion=false&refresh=true",
        {
            "query": {
                "bool": {
                    "should": [{"exists": {"field": "predecessor_id"}}, {"exists": {"field": "successor_id"}}],
                    "minimum_should_match": 1,
                }
            },
            "script": {"source": ("ctx._source.remove('predecessor_id');ctx._source.remove('successor_id');")},
        },
    )
    task_id = start_response.get("task")
    if not isinstance(task_id, str) or not task_id.strip():
        raise RuntimeError(f"Could not get reset task id: {start_response}")

    deadline = time.monotonic() + timeout_seconds
    while True:
        task_response = client.get_json(f"/_tasks/{task_id}")
        if bool(task_response.get("completed")):
            response = task_response.get("response")
            if not isinstance(response, dict):
                raise RuntimeError(f"Reset task finished without valid response: {task_response}")
            break

        task_error = task_response.get("error")
        if task_error is not None:
            raise RuntimeError(f"Reset task failed: {json.dumps(task_error, ensure_ascii=False)}")

        if time.monotonic() >= deadline:
            raise RuntimeError(f"Reset did not finish within {timeout_seconds}s. Task is still running: {task_id}")
        time.sleep(poll_seconds)

    failures = response.get("failures")
    if isinstance(failures, list) and failures:
        raise RuntimeError(f"Reset step reported failures: {json.dumps(failures[:3], ensure_ascii=False)}")
    return response, False


def site_filter_clause(site_name: str) -> dict[str, Any]:
    return {
        "bool": {
            "should": [
                {"term": {"site_name.keyword": site_name}},
                {"term": {"site_name": site_name}},
            ],
            "minimum_should_match": 1,
        }
    }


def pick_site_name(client: ElasticsearchHttpClient, *, target_index: str, site_override: str | None) -> str:
    if site_override:
        return site_override.strip()

    response = client.post_json(
        f"/{target_index}/_search",
        {
            "size": 0,
            "aggs": {
                "sites": {
                    "terms": {"field": "site_name.keyword", "size": 20},
                }
            },
        },
    )
    buckets = response.get("aggregations", {}).get("sites", {}).get("buckets", [])
    if not isinstance(buckets, list) or not buckets:
        raise RuntimeError("No site_name buckets found in target index.")
    best = buckets[0]
    key = best.get("key")
    if not isinstance(key, str) or not key.strip():
        raise RuntimeError("Could not resolve site_name from aggregation.")
    return key


def parse_dt(value: Any) -> datetime:
    if not isinstance(value, str):
        raise RuntimeError(f"Expected datetime string, got: {value!r}")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def compute_window(
    client: ElasticsearchHttpClient,
    *,
    target_index: str,
    site_name: str,
    window_hours: int,
) -> tuple[datetime, datetime]:
    response = client.post_json(
        f"/{target_index}/_search",
        {
            "size": 0,
            "query": {"bool": {"filter": [site_filter_clause(site_name)]}},
            "aggs": {
                "min_offer_start": {"min": {"field": "offer_start"}},
                "max_offer_start": {"max": {"field": "offer_start"}},
            },
        },
    )

    min_as_string = response.get("aggregations", {}).get("min_offer_start", {}).get("value_as_string")
    max_as_string = response.get("aggregations", {}).get("max_offer_start", {}).get("value_as_string")
    if not min_as_string or not max_as_string:
        raise RuntimeError(f"No offer_start range found for site '{site_name}'.")

    earliest = parse_dt(min_as_string)
    latest = parse_dt(max_as_string)

    lower = latest - timedelta(hours=max(1, window_hours))
    if lower < earliest:
        lower = earliest
    if lower >= latest:
        lower = latest - timedelta(seconds=1)
    return lower, latest


def seed_postgres_window(
    *,
    postgres_url: str,
    site_name: str,
    lower_bound: datetime,
    upper_bound: datetime,
) -> None:
    lower_naive = lower_bound.astimezone(UTC).replace(tzinfo=None)
    upper_naive = upper_bound.astimezone(UTC).replace(tzinfo=None)

    engine = create_engine(postgres_url, future=True)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS upload_timestamps (
                    site VARCHAR(64) PRIMARY KEY,
                    "timestamp" TIMESTAMP NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS marker_timestamps (
                    site VARCHAR(64) PRIMARY KEY,
                    "timestamp" TIMESTAMP NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO upload_timestamps (site, "timestamp")
                VALUES (:site, :timestamp)
                ON CONFLICT (site)
                DO UPDATE SET "timestamp" = EXCLUDED."timestamp"
                """
            ),
            {"site": site_name, "timestamp": upper_naive},
        )
        conn.execute(
            text(
                """
                INSERT INTO marker_timestamps (site, "timestamp")
                VALUES (:site, :timestamp)
                ON CONFLICT (site)
                DO UPDATE SET "timestamp" = EXCLUDED."timestamp"
                """
            ),
            {"site": site_name, "timestamp": lower_naive},
        )


def run_worker(
    *,
    worker_module: str,
    es_url: str,
    target_index: str,
    postgres_url: str,
    site_name: str,
) -> tuple[str, WorkerMetrics]:
    env = os.environ.copy()
    env["ELASTICSEARCH_URL"] = es_url
    env["PROCESSED_INDEX"] = target_index
    env["POSTGRES_DATABASE_URL"] = postgres_url
    env["MATCHING_SITES"] = site_name
    env.setdefault("LOG_LEVEL", "INFO")

    process = subprocess.run(
        [sys.executable, "-m", worker_module],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    output = f"{process.stdout}\n{process.stderr}"
    if process.returncode != 0:
        raise RuntimeError(f"Worker failed with code={process.returncode}\n{output}")

    metrics = extract_worker_metrics(output, expected_site_name=site_name)
    return output, metrics


def _metrics_from_match(match: re.Match[str]) -> WorkerMetrics:
    groups = match.groupdict()
    return WorkerMetrics(
        site_name=groups["site"].strip(),
        total_docs_processed=int(groups["processed"]),
        matches_found=int(groups["matches"]),
        claim_success=int(groups["claims_ok"]),
        claim_failed=int(groups["claims_failed"]),
        linked_docs=int(groups["linked"]),
    )


def extract_worker_metrics(output: str, *, expected_site_name: str) -> WorkerMetrics:
    last_match: re.Match[str] | None = None
    for match in FINISHED_SITE_RE.finditer(output):
        last_match = match
    if last_match is not None:
        return _metrics_from_match(last_match)

    last_committed_match: re.Match[str] | None = None
    for match in COMMITTED_BATCH_RE.finditer(output):
        last_committed_match = match
    if last_committed_match is not None:
        return _metrics_from_match(last_committed_match)

    no_op_markers = (
        f"Skipping site {expected_site_name}:",
        "No sites available for matching",
        f"Initialized marker timestamp for {expected_site_name}",
    )
    if any(marker in output for marker in no_op_markers):
        return WorkerMetrics(
            site_name=expected_site_name,
            total_docs_processed=0,
            matches_found=0,
            claim_success=0,
            claim_failed=0,
            linked_docs=0,
        )

    raise RuntimeError("Could not parse worker metrics from output.")


def window_query(site_name: str, lower_bound: datetime, upper_bound: datetime) -> dict[str, Any]:
    return {
        "bool": {
            "filter": [
                site_filter_clause(site_name),
                {
                    "range": {
                        "offer_start": {
                            "gte": lower_bound.isoformat(),
                            "lte": upper_bound.isoformat(),
                        }
                    }
                },
            ]
        }
    }


def fetch_window_docs(
    client: ElasticsearchHttpClient,
    *,
    target_index: str,
    site_name: str,
    lower_bound: datetime,
    upper_bound: datetime,
) -> dict[str, dict[str, Any]]:
    response = client.post_json(
        f"/{target_index}/_search?scroll={SCROLL_TTL}",
        {
            "size": SCROLL_BATCH_SIZE,
            "sort": ["_doc"],
            "query": window_query(site_name, lower_bound, upper_bound),
        },
    )

    docs: dict[str, dict[str, Any]] = {}
    scroll_id = response.get("_scroll_id")
    while True:
        hits = response.get("hits", {}).get("hits", [])
        if not isinstance(hits, list) or not hits:
            break
        for hit in hits:
            if not isinstance(hit, dict):
                continue
            doc_id = hit.get("_id")
            source = hit.get("_source")
            if isinstance(doc_id, str) and isinstance(source, dict):
                docs[doc_id] = source
        if not isinstance(scroll_id, str) or not scroll_id:
            break
        response = client.post_json(
            "/_search/scroll",
            {"scroll": SCROLL_TTL, "scroll_id": scroll_id},
        )
        scroll_id = response.get("_scroll_id")

    if isinstance(scroll_id, str) and scroll_id:
        client.delete(
            "/_search/scroll",
            payload={"scroll_id": [scroll_id]},
            ignore_statuses={404},
        )
    return docs


def fetch_docs_by_ids(
    client: ElasticsearchHttpClient,
    *,
    target_index: str,
    ids: list[str],
) -> dict[str, dict[str, Any]]:
    if not ids:
        return {}
    response = client.post_json(f"/{target_index}/_mget", {"ids": ids})
    docs = response.get("docs", [])
    result: dict[str, dict[str, Any]] = {}
    if not isinstance(docs, list):
        return result
    for item in docs:
        if not isinstance(item, dict):
            continue
        doc_id = item.get("_id")
        source = item.get("_source")
        if isinstance(doc_id, str) and isinstance(source, dict):
            result[doc_id] = source
    return result


def summarize_doc(doc_id: str, source: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": doc_id,
        "site_name": source.get("site_name"),
        "name": source.get("name"),
        "brand": source.get("brand"),
        "model": source.get("model"),
        "offer_start": source.get("offer_start"),
        "offer_end": source.get("offer_end"),
        "latest_price": source.get("latest_price"),
        "mileage": source.get("mileage"),
        "predecessor_id": source.get("predecessor_id"),
        "successor_id": source.get("successor_id"),
    }


def build_samples(
    *,
    docs_by_id: dict[str, dict[str, Any]],
    client: ElasticsearchHttpClient,
    target_index: str,
    sample_size: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    matched_candidates: list[tuple[str, str]] = []
    unmatched_doc_ids: list[str] = []
    predecessor_lookup_needed: set[str] = set()

    for doc_id, source in docs_by_id.items():
        predecessor = source.get("predecessor_id")
        successor = source.get("successor_id")
        predecessor_id = predecessor if isinstance(predecessor, str) and predecessor else None
        successor_id = successor if isinstance(successor, str) and successor else None
        if predecessor_id:
            matched_candidates.append((doc_id, predecessor_id))
            if predecessor_id not in docs_by_id:
                predecessor_lookup_needed.add(predecessor_id)
        if predecessor_id is None and successor_id is None:
            unmatched_doc_ids.append(doc_id)

    extra_docs = fetch_docs_by_ids(
        client,
        target_index=target_index,
        ids=sorted(predecessor_lookup_needed),
    )

    matched_pairs: list[dict[str, Any]] = []
    for candidate_id, predecessor_id in matched_candidates[: max(1, sample_size)]:
        candidate_source = docs_by_id.get(candidate_id)
        predecessor_source = docs_by_id.get(predecessor_id) or extra_docs.get(predecessor_id)
        if candidate_source is None:
            continue
        matched_pairs.append(
            {
                "candidate": summarize_doc(candidate_id, candidate_source),
                "predecessor": summarize_doc(predecessor_id, predecessor_source or {}),
            }
        )

    unmatched_docs: list[dict[str, Any]] = []
    for doc_id in unmatched_doc_ids[: max(1, sample_size)]:
        source = docs_by_id.get(doc_id)
        if source is None:
            continue
        unmatched_docs.append(summarize_doc(doc_id, source))
    return matched_pairs, unmatched_docs


def link_snapshot(docs_by_id: dict[str, dict[str, Any]]) -> dict[str, dict[str, str | None]]:
    snapshot: dict[str, dict[str, str | None]] = {}
    for doc_id, source in docs_by_id.items():
        predecessor = source.get("predecessor_id")
        successor = source.get("successor_id")
        snapshot[doc_id] = {
            "predecessor_id": predecessor if isinstance(predecessor, str) else None,
            "successor_id": successor if isinstance(successor, str) else None,
        }
    return snapshot


def verify_chains(
    docs_by_id: dict[str, dict[str, Any]],
    *,
    client: ElasticsearchHttpClient,
    target_index: str,
) -> dict[str, Any]:
    resolved_docs_by_id: dict[str, dict[str, Any]] = dict(docs_by_id)
    referenced_ids: set[str] = set()
    for source in docs_by_id.values():
        predecessor = source.get("predecessor_id")
        successor = source.get("successor_id")
        if isinstance(predecessor, str) and predecessor:
            referenced_ids.add(predecessor)
        if isinstance(successor, str) and successor:
            referenced_ids.add(successor)

    unresolved_ids = sorted(referenced_ids - set(resolved_docs_by_id))
    if unresolved_ids:
        resolved_docs_by_id.update(
            fetch_docs_by_ids(
                client,
                target_index=target_index,
                ids=unresolved_ids,
            )
        )

    missing_predecessors: list[dict[str, str]] = []
    missing_successors: list[dict[str, str]] = []
    reverse_mismatches: list[dict[str, str | None]] = []
    predecessor_to_successors: dict[str, list[str]] = {}

    for doc_id, source in docs_by_id.items():
        predecessor = source.get("predecessor_id")
        successor = source.get("successor_id")
        predecessor_id = predecessor if isinstance(predecessor, str) and predecessor else None
        successor_id = successor if isinstance(successor, str) and successor else None

        if predecessor_id is not None:
            predecessor_to_successors.setdefault(predecessor_id, []).append(doc_id)
            predecessor_source = resolved_docs_by_id.get(predecessor_id)
            if predecessor_source is None:
                missing_predecessors.append({"doc_id": doc_id, "predecessor_id": predecessor_id})
            else:
                reverse = predecessor_source.get("successor_id")
                reverse_id = reverse if isinstance(reverse, str) else None
                if reverse_id != doc_id:
                    reverse_mismatches.append(
                        {
                            "doc_id": doc_id,
                            "predecessor_id": predecessor_id,
                            "predecessor_successor_id": reverse_id,
                        }
                    )

        if successor_id is not None:
            successor_source = resolved_docs_by_id.get(successor_id)
            if successor_source is None:
                missing_successors.append({"doc_id": doc_id, "successor_id": successor_id})
            else:
                reverse = successor_source.get("predecessor_id")
                reverse_id = reverse if isinstance(reverse, str) else None
                if reverse_id != doc_id:
                    reverse_mismatches.append(
                        {
                            "doc_id": doc_id,
                            "successor_id": successor_id,
                            "successor_predecessor_id": reverse_id,
                        }
                    )

    predecessor_conflicts = {
        predecessor_id: sorted(successors)
        for predecessor_id, successors in predecessor_to_successors.items()
        if len(successors) > 1
    }

    cycles: list[list[str]] = []
    for start_id in docs_by_id.keys():
        visited: set[str] = set()
        path: list[str] = [start_id]
        current = start_id
        while True:
            successor_raw = docs_by_id.get(current, {}).get("successor_id")
            successor = successor_raw if isinstance(successor_raw, str) and successor_raw else None
            if successor is None or successor not in docs_by_id:
                break
            if successor == start_id:
                cycles.append(path + [successor])
                break
            if successor in visited:
                cycles.append(path + [successor])
                break
            visited.add(successor)
            path.append(successor)
            current = successor

    is_valid = not (missing_predecessors or missing_successors or reverse_mismatches or predecessor_conflicts or cycles)
    return {
        "is_valid": is_valid,
        "checked_docs": len(docs_by_id),
        "resolved_docs": len(resolved_docs_by_id),
        "resolved_external_refs": len(resolved_docs_by_id) - len(docs_by_id),
        "missing_predecessors": missing_predecessors,
        "missing_successors": missing_successors,
        "reverse_mismatches": reverse_mismatches,
        "predecessor_conflicts": predecessor_conflicts,
        "cycles": cycles,
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def build_failure_details(
    *,
    chain_report: dict[str, Any],
    idempotent: bool,
    first_only_doc_ids: list[str],
    second_only_doc_ids: list[str],
) -> dict[str, Any]:
    reasons: list[dict[str, Any]] = []
    if not idempotent:
        reasons.append(
            {
                "type": "idempotency_failed",
                "message": "Link snapshot changed after second run.",
                "first_only_count": len(first_only_doc_ids),
                "second_only_count": len(second_only_doc_ids),
                "first_only_examples": first_only_doc_ids[:20],
                "second_only_examples": second_only_doc_ids[:20],
            }
        )

    if not chain_report.get("is_valid"):
        missing_predecessors = chain_report.get("missing_predecessors", [])
        missing_successors = chain_report.get("missing_successors", [])
        reverse_mismatches = chain_report.get("reverse_mismatches", [])
        predecessor_conflicts = chain_report.get("predecessor_conflicts", {})
        cycles = chain_report.get("cycles", [])
        reasons.append(
            {
                "type": "chain_verification_failed",
                "message": "Chain verification reported invalid links or graph conflicts.",
                "checked_docs": chain_report.get("checked_docs"),
                "missing_predecessors_count": len(missing_predecessors),
                "missing_successors_count": len(missing_successors),
                "reverse_mismatches_count": len(reverse_mismatches),
                "predecessor_conflicts_count": (
                    len(predecessor_conflicts) if isinstance(predecessor_conflicts, dict) else 0
                ),
                "cycles_count": len(cycles),
                "missing_predecessors_examples": (
                    missing_predecessors[:20] if isinstance(missing_predecessors, list) else []
                ),
                "missing_successors_examples": (
                    missing_successors[:20] if isinstance(missing_successors, list) else []
                ),
                "reverse_mismatches_examples": (
                    reverse_mismatches[:20] if isinstance(reverse_mismatches, list) else []
                ),
                "predecessor_conflicts_examples": (
                    dict(list(predecessor_conflicts.items())[:20]) if isinstance(predecessor_conflicts, dict) else {}
                ),
                "cycles_examples": cycles[:20] if isinstance(cycles, list) else [],
            }
        )

    return {
        "has_failure": bool(reasons),
        "reasons": reasons,
    }


def build_output_dir(output_dir_arg: str | None) -> Path:
    if output_dir_arg:
        return Path(output_dir_arg).resolve()
    timestamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    return (Path.cwd() / "artifacts" / "matching_dry_run" / timestamp).resolve()


def main() -> None:
    args = parse_args()
    validate_safety(args)

    client = ElasticsearchHttpClient(args.es_url)
    validate_elasticsearch_connection(client, es_url=args.es_url)
    output_dir = build_output_dir(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Steps 0-3: create target, copy docs, reset matching fields.
    skipped_copy = should_skip_copy_for_existing_target(
        client,
        target_index=args.target_index,
    )
    mapping_response: dict[str, Any] = {}
    reindex_response: dict[str, Any] = {}
    if skipped_copy:
        print(f"Skipping recreate/reindex because target '{args.target_index}' already exists and has documents.")
    else:
        mapping_response = recreate_target_index_from_source(
            client,
            source_index=args.source_index,
            target_index=args.target_index,
        )
        reindex_response = run_reindex_copy(
            client,
            source_index=args.source_index,
            target_index=args.target_index,
            timeout_seconds=args.reindex_timeout_seconds,
            poll_seconds=args.reindex_poll_seconds,
        )
    reset_response, skipped_reset = reset_matching_fields(
        client,
        target_index=args.target_index,
        timeout_seconds=args.reset_timeout_seconds,
        poll_seconds=args.reset_poll_seconds,
    )
    if skipped_reset:
        print(f"Skipping reset for '{args.target_index}' because no docs have predecessor_id or successor_id set.")

    # Steps 5-6: choose site and set marker/upload timestamps for an hour-based window.
    site_name = pick_site_name(client, target_index=args.target_index, site_override=args.site_name)
    lower_bound, upper_bound = compute_window(
        client,
        target_index=args.target_index,
        site_name=site_name,
        window_hours=args.window_hours,
    )
    seed_postgres_window(
        postgres_url=args.postgres_url,
        site_name=site_name,
        lower_bound=lower_bound,
        upper_bound=upper_bound,
    )

    # Step 7-8: run worker and collect metrics.
    first_output, first_metrics = run_worker(
        worker_module=args.worker_module,
        es_url=args.es_url,
        target_index=args.target_index,
        postgres_url=args.postgres_url,
        site_name=site_name,
    )
    (output_dir / "worker_first_run.log").write_text(first_output, encoding="utf-8")

    docs_after_first = fetch_window_docs(
        client,
        target_index=args.target_index,
        site_name=site_name,
        lower_bound=lower_bound,
        upper_bound=upper_bound,
    )
    first_snapshot = link_snapshot(docs_after_first)

    # Steps 9-10: samples + chain verification.
    matched_pairs, unmatched_docs = build_samples(
        docs_by_id=docs_after_first,
        client=client,
        target_index=args.target_index,
        sample_size=args.sample_size,
    )
    chain_report = verify_chains(
        docs_after_first,
        client=client,
        target_index=args.target_index,
    )

    # Step 11: idempotency rerun and compare.
    second_output, second_metrics = run_worker(
        worker_module=args.worker_module,
        es_url=args.es_url,
        target_index=args.target_index,
        postgres_url=args.postgres_url,
        site_name=site_name,
    )
    (output_dir / "worker_second_run.log").write_text(second_output, encoding="utf-8")

    docs_after_second = fetch_window_docs(
        client,
        target_index=args.target_index,
        site_name=site_name,
        lower_bound=lower_bound,
        upper_bound=upper_bound,
    )
    second_snapshot = link_snapshot(docs_after_second)
    idempotent = first_snapshot == second_snapshot

    metrics_payload = {
        "index_name": args.target_index,
        "site_name": site_name,
        "window_hours": args.window_hours,
        "lower_bound": lower_bound.isoformat(),
        "upper_bound": upper_bound.isoformat(),
        "total_docs_processed": first_metrics.total_docs_processed,
        "matches_found": first_metrics.matches_found,
        "claim_success": first_metrics.claim_success,
        "claim_failed": first_metrics.claim_failed,
        "linked_docs": first_metrics.linked_docs,
        "second_run": {
            "total_docs_processed": second_metrics.total_docs_processed,
            "matches_found": second_metrics.matches_found,
            "claim_success": second_metrics.claim_success,
            "claim_failed": second_metrics.claim_failed,
            "linked_docs": second_metrics.linked_docs,
        },
    }
    write_json(output_dir / "metrics.json", metrics_payload)
    write_json(
        output_dir / "matched_pairs_sample.json",
        {"count": len(matched_pairs), "items": matched_pairs},
    )
    write_json(
        output_dir / "unmatched_docs_sample.json",
        {"count": len(unmatched_docs), "items": unmatched_docs},
    )
    write_json(output_dir / "chain_verification.json", chain_report)
    first_only_doc_ids = sorted(set(first_snapshot) - set(second_snapshot))
    second_only_doc_ids = sorted(set(second_snapshot) - set(first_snapshot))
    write_json(
        output_dir / "idempotency.json",
        {
            "idempotent": idempotent,
            "docs_compared": len(first_snapshot),
            "first_only_doc_ids": first_only_doc_ids,
            "second_only_doc_ids": second_only_doc_ids,
        },
    )

    failure_details = build_failure_details(
        chain_report=chain_report,
        idempotent=idempotent,
        first_only_doc_ids=first_only_doc_ids,
        second_only_doc_ids=second_only_doc_ids,
    )
    failure_details_file = output_dir / "failure_details.json"
    if failure_details["has_failure"]:
        write_json(failure_details_file, failure_details)
    write_json(
        output_dir / "run_summary.json",
        {
            "source_index": args.source_index,
            "target_index": args.target_index,
            "site_name": site_name,
            "output_dir": str(output_dir),
            "skipped_recreate_and_reindex": skipped_copy,
            "skipped_reset": skipped_reset,
            "mapping_response": mapping_response,
            "reindex_response": reindex_response,
            "reset_response": {
                "total": reset_response.get("total"),
                "updated": reset_response.get("updated"),
                "version_conflicts": reset_response.get("version_conflicts"),
            },
            "metrics_file": str(output_dir / "metrics.json"),
            "matched_pairs_file": str(output_dir / "matched_pairs_sample.json"),
            "unmatched_docs_file": str(output_dir / "unmatched_docs_sample.json"),
            "chain_verification_file": str(output_dir / "chain_verification.json"),
            "idempotency_file": str(output_dir / "idempotency.json"),
            "idempotent": idempotent,
            "failure_details_file": str(failure_details_file) if failure_details["has_failure"] else None,
        },
    )

    if failure_details["has_failure"]:
        raise RuntimeError(f"Dry-run validation failed. See failure details: {failure_details_file}")
    print(f"Dry-run completed. Artifacts written to: {output_dir}")


if __name__ == "__main__":
    main()
