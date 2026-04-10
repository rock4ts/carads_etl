"""Run a deterministic end-to-end scenario for the matching worker.

The script performs the full flow required by the controlled test plan:
1. Create/reset the test index from ``index_example.json``.
2. Seed a dedicated set of documents for chain/competing/no-match/mismatch cases.
3. Seed ``marker_timestamps`` and ``upload_timestamps`` in Postgres.
4. Run ``python -m app.services.matching_service.main``.
5. Verify expected predecessor/successor links and graph invariants.
6. Run the worker again and verify idempotency.
7. Validate logs include expected counters, including competing-case claim failures.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib import error, request

from sqlalchemy import create_engine, text

from app.shared.database.models import MatchingStateBase

DEFAULT_ELASTICSEARCH_URL = "http://localhost:19200"
DEFAULT_POSTGRES_DATABASE_URL = "postgresql+psycopg://postgres:postgres@localhost:5432/car_intel"
DEFAULT_INDEX = "carads1_local"
DEFAULT_SITE = "matching_controlled_test"
DEFAULT_WORKER_MODULE = "app.services.matching_service.main"

INDEX_DEFINITION_FILE = Path("index_example.json")

LOGGER = logging.getLogger("controlled_matching_scenario")


@dataclass(frozen=True)
class ScenarioDoc:
    doc_id: str
    case_name: str
    source: dict[str, Any]


@dataclass(frozen=True)
class ScenarioExpectation:
    predecessor_by_doc: dict[str, str | None]
    successor_by_doc: dict[str, str | None]
    competing_group_docs: tuple[str, str, str]


class ElasticsearchHttpClient:
    """Minimal Elasticsearch client for deterministic scenario setup and checks."""

    def __init__(self, base_url: str) -> None:
        self._base_url = base_url.rstrip("/")

    def get_json(self, path: str) -> dict[str, Any]:
        return self._request_json("GET", path)

    def put_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request_json("PUT", path, payload=payload)

    def post_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request_json("POST", path, payload=payload)

    def post_ndjson(self, path: str, lines: list[dict[str, Any]]) -> dict[str, Any]:
        payload = "".join(f"{json.dumps(line, separators=(',', ':'))}\n" for line in lines).encode("utf-8")
        headers = {"Content-Type": "application/x-ndjson"}
        return self._request_json("POST", path, data=payload, headers=headers)

    def delete(self, path: str, *, ignore_statuses: set[int] | None = None) -> dict[str, Any]:
        return self._request_json("DELETE", path, ignore_statuses=ignore_statuses)

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        data: bytes | None = None,
        headers: dict[str, str] | None = None,
        ignore_statuses: set[int] | None = None,
    ) -> dict[str, Any]:
        url = f"{self._base_url}{path}"
        body = data
        request_headers = {"Content-Type": "application/json"}
        if headers is not None:
            request_headers = headers
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
        req = request.Request(url=url, data=body, method=method, headers=request_headers)
        try:
            with request.urlopen(req, timeout=30) as response:
                raw_body = response.read().decode("utf-8")
        except error.HTTPError as exc:
            if ignore_statuses and exc.code in ignore_statuses:
                return {}
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Elasticsearch HTTP {exc.code} at {url}: {detail}") from exc
        except error.URLError as exc:
            raise RuntimeError(f"Failed to reach Elasticsearch at {url}: {exc}") from exc
        if not raw_body:
            return {}
        try:
            return json.loads(raw_body)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid JSON response from Elasticsearch at {url}") from exc


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--es-url", default=os.getenv("ELASTICSEARCH_URL", DEFAULT_ELASTICSEARCH_URL))
    parser.add_argument("--index", default=os.getenv("PROCESSED_INDEX", DEFAULT_INDEX))
    parser.add_argument(
        "--postgres-url",
        default=os.getenv("POSTGRES_DATABASE_URL", os.getenv("DATABASE_URL", DEFAULT_POSTGRES_DATABASE_URL)),
    )
    parser.add_argument("--site-name", default=os.getenv("MATCHING_SCENARIO_SITE", DEFAULT_SITE))
    parser.add_argument(
        "--worker-module",
        default=DEFAULT_WORKER_MODULE,
        help="Python module path to execute via `python -m`.",
    )
    parser.add_argument(
        "--index-definition",
        default=str(INDEX_DEFINITION_FILE),
        help="Path to index definition JSON (expects top-level object keyed by index name).",
    )
    parser.add_argument(
        "--keep-index",
        action="store_true",
        help="Do not delete and recreate the index before seeding.",
    )
    parser.add_argument(
        "--wait-seconds",
        type=int,
        default=30,
        help="Maximum seconds to wait for Elasticsearch health.",
    )
    return parser.parse_args()


def _wait_for_elasticsearch(client: ElasticsearchHttpClient, timeout_seconds: int) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            health = client.get_json("/_cluster/health")
            status = health.get("status")
            if isinstance(status, str):
                LOGGER.info("Elasticsearch is reachable: cluster status=%s", status)
                return
        except RuntimeError:
            pass
        time.sleep(1.0)
    raise RuntimeError(f"Elasticsearch did not become ready within {timeout_seconds} seconds.")


def _read_index_definition(index_definition_path: Path, index_name: str) -> dict[str, Any]:
    raw_text = index_definition_path.read_text(encoding="utf-8")
    payload = json.loads(raw_text)
    index_body = payload.get(index_name)
    if not isinstance(index_body, dict):
        available = ", ".join(sorted(str(key) for key in payload.keys()))
        raise RuntimeError(
            f"Index definition for '{index_name}' not found in {index_definition_path}. Available keys: {available}"
        )
    return index_body


def _ensure_reset_index(
    *,
    client: ElasticsearchHttpClient,
    index_name: str,
    index_definition_path: Path,
    keep_index: bool,
) -> None:
    if not keep_index:
        client.delete(f"/{index_name}", ignore_statuses={404})
        LOGGER.info("Deleted index '%s' (if existed).", index_name)
        index_body = _read_index_definition(index_definition_path, index_name)
        client.put_json(f"/{index_name}", index_body)
        LOGGER.info("Created index '%s' from %s.", index_name, index_definition_path)
    client.put_json(
        f"/{index_name}/_mapping",
        {
            "properties": {
                "predecessor_id": {"type": "keyword"},
                "successor_id": {"type": "keyword"},
            }
        },
    )
    LOGGER.info("Ensured predecessor/successor mapping for '%s'.", index_name)


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).isoformat()


def _base_doc(
    *,
    site_name: str,
    name: str,
    offer_start: datetime,
    offer_end: datetime | None,
    latest_price: float,
    mileage: int | None,
    brand: str = "Toyota",
    model: str = "Camry",
    generation: str = "XV70",
    engine_power: float = 181.0,
    engine_volume_liters: float = 2.5,
    gear_box: str = "automatic",
    gear_type: str = "fwd",
) -> dict[str, Any]:
    parsed_at = offer_start + timedelta(minutes=5)
    last_checked = offer_start + timedelta(minutes=10)
    source: dict[str, Any] = {
        "original_id": f"{site_name}:{name}",
        "url": f"https://example.test/{site_name}/{name}",
        "site_name": site_name,
        "seller_type": "dealer",
        "name": name,
        "last_checked": _iso(last_checked),
        "parsed_at": _iso(parsed_at),
        "offer_start": _iso(offer_start),
        "trans_history": [],
        "initial_price": latest_price,
        "latest_price": latest_price,
        "is_new": False,
        "brand": brand,
        "model": model,
        "generation": generation,
        "modification": "2.5 AT",
        "complectation": "base",
        "build_year": 2020,
        "vin": None,
        "engine_power": engine_power,
        "engine_volume_liters": engine_volume_liters,
        "fuel": "petrol",
        "gear_type": gear_type,
        "gear_box": gear_box,
        "steering_position": "left",
        "body_type": "sedan",
        "body_color": "white",
        "doors_num": "4",
        "count_owner": 1,
        "condition": "used",
        "mileage": mileage,
        "mileage_units": "km",
        "views_total": 120,
        "place": "city-a",
        "region": "region-a",
        "predecessor_id": None,
        "successor_id": None,
        "offer_end": _iso(offer_end) if offer_end is not None else None,
        "images": [],
    }
    return source


def _build_scenario_docs(site_name: str) -> tuple[list[ScenarioDoc], ScenarioExpectation, datetime, datetime]:
    base_time = datetime(2026, 4, 10, 10, 0, tzinfo=UTC)
    docs: list[ScenarioDoc] = []
    chain_seller_name = "AutoHub Chain Seller"
    competing_seller_name = "AutoHub Competing Seller"
    mismatch_seller_name = "AutoHub Mismatch Seller"
    # Use 4-day gaps between generations:
    # - old candidate cannot match newer docs (matcher upper bound is +3 days)
    # - newer candidate can still match old docs (lower bound is -5 days)
    chain_c_start = base_time
    chain_a_start = base_time + timedelta(days=4)
    chain_b_start = base_time + timedelta(days=8)
    competing_c_start = base_time + timedelta(days=20)
    competing_a_start = base_time + timedelta(days=24)
    competing_b_start = base_time + timedelta(days=24, minutes=1)

    # Case A: perfect chain C(old) -> A(new) -> B(newer)
    chain_c_id = "caseA-chain-c"
    chain_a_id = "caseA-chain-a"
    chain_b_id = "caseA-chain-b"
    docs.append(
        ScenarioDoc(
            doc_id=chain_c_id,
            case_name="A_perfect_chain",
            source=_base_doc(
                site_name=site_name,
                name=chain_seller_name,
                offer_start=chain_c_start,
                offer_end=chain_c_start + timedelta(hours=1),
                latest_price=20000.0,
                mileage=60000,
            ),
        )
    )
    docs.append(
        ScenarioDoc(
            doc_id=chain_a_id,
            case_name="A_perfect_chain",
            source=_base_doc(
                site_name=site_name,
                name=chain_seller_name,
                offer_start=chain_a_start,
                offer_end=chain_a_start + timedelta(hours=1),
                latest_price=19950.0,
                mileage=60200,
            ),
        )
    )
    docs.append(
        ScenarioDoc(
            doc_id=chain_b_id,
            case_name="A_perfect_chain",
            source=_base_doc(
                site_name=site_name,
                name=chain_seller_name,
                offer_start=chain_b_start,
                offer_end=chain_b_start + timedelta(hours=1),
                latest_price=19800.0,
                mileage=60500,
            ),
        )
    )

    # Case B: competing candidates; only one can claim old predecessor.
    comp_c_id = "caseB-competing-c"
    comp_a_id = "caseB-competing-a"
    comp_b_id = "caseB-competing-b"
    docs.append(
        ScenarioDoc(
            doc_id=comp_c_id,
            case_name="B_competing",
            source=_base_doc(
                site_name=site_name,
                name=competing_seller_name,
                offer_start=competing_c_start,
                offer_end=competing_c_start + timedelta(hours=1),
                latest_price=15000.0,
                mileage=70000,
                brand="Hyundai",
                model="Elantra",
                generation="AD",
            ),
        )
    )
    docs.append(
        ScenarioDoc(
            doc_id=comp_a_id,
            case_name="B_competing",
            source=_base_doc(
                site_name=site_name,
                name=competing_seller_name,
                offer_start=competing_a_start,
                offer_end=None,
                latest_price=14900.0,
                mileage=70200,
                brand="Hyundai",
                model="Elantra",
                generation="AD",
            ),
        )
    )
    docs.append(
        ScenarioDoc(
            doc_id=comp_b_id,
            case_name="B_competing",
            source=_base_doc(
                site_name=site_name,
                name=competing_seller_name,
                offer_start=competing_b_start,
                offer_end=None,
                latest_price=14920.0,
                mileage=70150,
                brand="Hyundai",
                model="Elantra",
                generation="AD",
            ),
        )
    )

    # Case C: no match.
    no_match_id = "caseC-no-match-d"
    docs.append(
        ScenarioDoc(
            doc_id=no_match_id,
            case_name="C_no_match",
            source=_base_doc(
                site_name=site_name,
                name="case-c-no-match-d",
                offer_start=base_time + timedelta(hours=10),
                offer_end=base_time + timedelta(hours=11),
                latest_price=32000.0,
                mileage=35000,
                brand="BMW",
                model="X5",
                generation="G05",
            ),
        )
    )

    # Case D: edge mismatch (same brand/model, different engine/gearbox -> no match).
    mismatch_old_id = "caseD-mismatch-old"
    mismatch_new_id = "caseD-mismatch-new"
    docs.append(
        ScenarioDoc(
            doc_id=mismatch_old_id,
            case_name="D_edge_mismatch",
            source=_base_doc(
                site_name=site_name,
                name=mismatch_seller_name,
                offer_start=base_time + timedelta(hours=12),
                offer_end=base_time + timedelta(hours=13),
                latest_price=24000.0,
                mileage=50000,
                brand="Kia",
                model="Sportage",
                generation="QL",
                engine_power=177.0,
                engine_volume_liters=1.6,
                gear_box="automatic",
                gear_type="awd",
            ),
        )
    )
    docs.append(
        ScenarioDoc(
            doc_id=mismatch_new_id,
            case_name="D_edge_mismatch",
            source=_base_doc(
                site_name=site_name,
                name=mismatch_seller_name,
                offer_start=base_time + timedelta(hours=14),
                offer_end=base_time + timedelta(hours=15),
                latest_price=23900.0,
                mileage=50100,
                brand="Kia",
                model="Sportage",
                generation="QL",
                engine_power=150.0,
                engine_volume_liters=2.0,
                gear_box="manual",
                gear_type="fwd",
            ),
        )
    )

    # Fillers (within same site but intentionally unique fields to avoid accidental matching).
    for index in range(1, 17):
        filler_start = base_time + timedelta(hours=16 + index)
        docs.append(
            ScenarioDoc(
                doc_id=f"filler-{index:02d}",
                case_name="filler",
                source=_base_doc(
                    site_name=site_name,
                    name=f"filler-{index:02d}",
                    offer_start=filler_start,
                    offer_end=filler_start + timedelta(hours=1),
                    latest_price=10000.0 + (index * 200),
                    mileage=80000 + (index * 1200),
                    brand=f"Brand{index:02d}",
                    model=f"Model{index:02d}",
                    generation=f"Gen{index:02d}",
                ),
            )
        )

    expected = ScenarioExpectation(
        predecessor_by_doc={
            chain_a_id: chain_c_id,
            chain_b_id: chain_a_id,
            comp_a_id: None,  # one of A/B will be set dynamically by competing assertion
            comp_b_id: None,
            no_match_id: None,
            mismatch_old_id: None,
            mismatch_new_id: None,
        },
        successor_by_doc={
            chain_c_id: chain_a_id,
            chain_a_id: chain_b_id,
            chain_b_id: None,
            comp_c_id: None,  # winner resolved dynamically
            no_match_id: None,
            mismatch_old_id: None,
            mismatch_new_id: None,
        },
        competing_group_docs=(comp_c_id, comp_a_id, comp_b_id),
    )
    offer_starts = [datetime.fromisoformat(str(doc.source["offer_start"])) for doc in docs]
    earliest = min(offer_starts)
    latest = max(offer_starts)
    return docs, expected, earliest, latest


def _bulk_seed_documents(client: ElasticsearchHttpClient, *, index_name: str, docs: list[ScenarioDoc]) -> None:
    bulk_lines: list[dict[str, Any]] = []
    for scenario_doc in docs:
        bulk_lines.append({"index": {"_index": index_name, "_id": scenario_doc.doc_id}})
        bulk_lines.append(scenario_doc.source)
    response = client.post_ndjson("/_bulk?refresh=true", bulk_lines)
    if response.get("errors"):
        items = response.get("items", [])
        if isinstance(items, list):
            for item in items:
                if not isinstance(item, dict):
                    continue
                action = item.get("index")
                if isinstance(action, dict) and "error" in action:
                    raise RuntimeError(f"Bulk insert error: {json.dumps(action['error'], ensure_ascii=False)}")
        raise RuntimeError("Bulk insert failed with unknown errors.")
    LOGGER.info("Seeded %s scenario documents into '%s'.", len(docs), index_name)


def _seed_postgres_window(*, postgres_url: str, site_name: str, earliest: datetime, latest: datetime) -> None:
    engine = create_engine(postgres_url, future=True)
    # Ensure worker state tables exist for clean local/dev databases.
    MatchingStateBase.metadata.create_all(engine)
    earliest_naive = earliest.astimezone(UTC).replace(tzinfo=None)
    latest_naive = latest.astimezone(UTC).replace(tzinfo=None)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO upload_timestamps (site_name, "timestamp")
                VALUES (:site_name, :timestamp)
                ON CONFLICT (site_name)
                DO UPDATE SET "timestamp" = EXCLUDED."timestamp"
                """
            ),
            {"site_name": site_name, "timestamp": latest_naive},
        )
        conn.execute(
            text(
                """
                INSERT INTO marker_timestamps (site_name, "timestamp")
                VALUES (:site_name, :timestamp)
                ON CONFLICT (site_name)
                DO UPDATE SET "timestamp" = EXCLUDED."timestamp"
                """
            ),
            {"site_name": site_name, "timestamp": earliest_naive},
        )
    LOGGER.info(
        "Seeded Postgres window for site='%s': marker=%s upload=%s",
        site_name,
        earliest.isoformat(),
        latest.isoformat(),
    )


def _fetch_docs_by_ids(
    client: ElasticsearchHttpClient, *, index_name: str, doc_ids: list[str]
) -> dict[str, dict[str, Any]]:
    response = client.post_json(f"/{index_name}/_mget", {"ids": doc_ids})
    docs = response.get("docs", [])
    if not isinstance(docs, list):
        raise RuntimeError("Unexpected _mget response payload.")
    resolved: dict[str, dict[str, Any]] = {}
    for entry in docs:
        if not isinstance(entry, dict):
            continue
        entry_id = entry.get("_id")
        source = entry.get("_source")
        found = bool(entry.get("found"))
        if isinstance(entry_id, str) and found and isinstance(source, dict):
            resolved[entry_id] = source
    return resolved


def _extract_link_snapshot(docs_by_id: dict[str, dict[str, Any]]) -> dict[str, tuple[str | None, str | None]]:
    snapshot: dict[str, tuple[str | None, str | None]] = {}
    for doc_id, source in docs_by_id.items():
        predecessor = source.get("predecessor_id")
        successor = source.get("successor_id")
        pred_value = predecessor if isinstance(predecessor, str) else None
        succ_value = successor if isinstance(successor, str) else None
        snapshot[doc_id] = (pred_value, succ_value)
    return snapshot


def _verify_expected_links(
    *,
    docs_by_id: dict[str, dict[str, Any]],
    expectation: ScenarioExpectation,
) -> None:
    predecessor_expectations = dict(expectation.predecessor_by_doc)
    successor_expectations = dict(expectation.successor_by_doc)
    comp_old, comp_a, comp_b = expectation.competing_group_docs

    comp_old_source = docs_by_id.get(comp_old)
    comp_a_source = docs_by_id.get(comp_a)
    comp_b_source = docs_by_id.get(comp_b)
    if comp_old_source is None or comp_a_source is None or comp_b_source is None:
        raise RuntimeError("Competing-case documents are missing from index after worker run.")

    comp_old_successor_raw = comp_old_source.get("successor_id")
    if comp_old_successor_raw not in (comp_a, comp_b):
        raise RuntimeError(
            "Competing case failed: expected old doc successor to be one of "
            f"({comp_a}, {comp_b}), got {comp_old_successor_raw!r}."
        )
    comp_winner = str(comp_old_successor_raw)
    comp_loser = comp_b if comp_winner == comp_a else comp_a

    predecessor_expectations[comp_winner] = comp_old
    predecessor_expectations[comp_loser] = None
    successor_expectations[comp_old] = comp_winner

    for doc_id, expected_predecessor in predecessor_expectations.items():
        source = docs_by_id.get(doc_id)
        if source is None:
            raise RuntimeError(f"Expected doc '{doc_id}' not found for predecessor check.")
        actual = source.get("predecessor_id")
        if actual != expected_predecessor:
            raise RuntimeError(f"Predecessor mismatch for {doc_id}: expected {expected_predecessor!r}, got {actual!r}.")

    for doc_id, expected_successor in successor_expectations.items():
        source = docs_by_id.get(doc_id)
        if source is None:
            raise RuntimeError(f"Expected doc '{doc_id}' not found for successor check.")
        actual = source.get("successor_id")
        if actual != expected_successor:
            raise RuntimeError(f"Successor mismatch for {doc_id}: expected {expected_successor!r}, got {actual!r}.")

    LOGGER.info(
        "Verified directed links for chain, competing winner='%s', no-match, and mismatch groups.",
        comp_winner,
    )


def _verify_invariants(docs_by_id: dict[str, dict[str, Any]]) -> None:
    predecessor_to_docs: dict[str, list[str]] = {}
    for doc_id, source in docs_by_id.items():
        predecessor = source.get("predecessor_id")
        successor = source.get("successor_id")
        if isinstance(predecessor, str):
            predecessor_to_docs.setdefault(predecessor, []).append(doc_id)
        if isinstance(successor, str):
            target = docs_by_id.get(successor)
            if target is None:
                raise RuntimeError(f"Invariant failure: successor '{successor}' referenced by '{doc_id}' is missing.")
            target_predecessor = target.get("predecessor_id")
            if target_predecessor != doc_id:
                raise RuntimeError(
                    "Invariant failure: inconsistent bidirectional link "
                    f"for '{doc_id}' -> '{successor}', reverse predecessor is {target_predecessor!r}."
                )

    conflicting = {pred: refs for pred, refs in predecessor_to_docs.items() if len(refs) > 1}
    if conflicting:
        raise RuntimeError(f"Invariant failure: predecessor has multiple successors: {conflicting}.")

    for start_doc in docs_by_id:
        visited: set[str] = set()
        current = start_doc
        while True:
            source = docs_by_id.get(current)
            if source is None:
                break
            successor = source.get("successor_id")
            if not isinstance(successor, str):
                break
            if successor == start_doc:
                raise RuntimeError(f"Invariant failure: cycle detected starting at '{start_doc}'.")
            if successor in visited:
                raise RuntimeError(f"Invariant failure: cycle detected via '{successor}'.")
            visited.add(successor)
            current = successor

    LOGGER.info("Verified invariants: consistent links, single-successor rule, and no cycles.")


def _run_worker_once(
    *,
    worker_module: str,
    es_url: str,
    index_name: str,
    postgres_url: str,
    site_name: str,
) -> str:
    env = os.environ.copy()
    env["ELASTICSEARCH_URL"] = es_url
    env["PROCESSED_INDEX"] = index_name
    env["POSTGRES_DATABASE_URL"] = postgres_url
    env["MATCHING_SITES"] = site_name
    env.setdefault("LOG_LEVEL", "INFO")

    process = subprocess.run(
        [sys.executable, "-m", worker_module],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    combined_output = f"{process.stdout}\n{process.stderr}"
    if process.returncode != 0:
        raise RuntimeError(
            f"Worker run failed with exit code {process.returncode}.\n--- worker output ---\n{combined_output}"
        )
    return combined_output


def _assert_log_expectations(worker_output: str) -> None:
    required_tokens = [
        "Match found for",
        "Claim succeeded for duplicate",
        "Finished site",
    ]
    for token in required_tokens:
        if token not in worker_output:
            raise RuntimeError(f"Expected log token '{token}' was not found in worker output.")

    claim_failed_count = worker_output.count("Claim failed for duplicate")
    if claim_failed_count <= 0:
        LOGGER.warning(
            "No claim-failed logs were observed. This is expected for a single non-parallel worker run "
            "because candidates are evaluated sequentially and already-claimed predecessors are filtered out."
        )
    else:
        LOGGER.info("Validated worker logs: claim_failures=%s", claim_failed_count)


def _focus_doc_ids_for_verification(docs: list[ScenarioDoc]) -> list[str]:
    return [scenario_doc.doc_id for scenario_doc in docs if scenario_doc.case_name != "filler"]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    args = _parse_args()

    client = ElasticsearchHttpClient(args.es_url)
    _wait_for_elasticsearch(client, timeout_seconds=args.wait_seconds)
    _ensure_reset_index(
        client=client,
        index_name=args.index,
        index_definition_path=Path(args.index_definition),
        keep_index=args.keep_index,
    )

    docs, expectation, earliest, latest = _build_scenario_docs(args.site_name)
    _bulk_seed_documents(client, index_name=args.index, docs=docs)
    _seed_postgres_window(postgres_url=args.postgres_url, site_name=args.site_name, earliest=earliest, latest=latest)

    LOGGER.info("Running worker first pass for controlled scenario.")
    first_output = _run_worker_once(
        worker_module=args.worker_module,
        es_url=args.es_url,
        index_name=args.index,
        postgres_url=args.postgres_url,
        site_name=args.site_name,
    )
    _assert_log_expectations(first_output)

    verify_doc_ids = _focus_doc_ids_for_verification(docs)
    first_docs = _fetch_docs_by_ids(client, index_name=args.index, doc_ids=verify_doc_ids)
    _verify_expected_links(docs_by_id=first_docs, expectation=expectation)
    _verify_invariants(first_docs)
    first_snapshot = _extract_link_snapshot(first_docs)

    LOGGER.info("Running worker second pass for idempotency check.")
    second_output = _run_worker_once(
        worker_module=args.worker_module,
        es_url=args.es_url,
        index_name=args.index,
        postgres_url=args.postgres_url,
        site_name=args.site_name,
    )

    second_docs = _fetch_docs_by_ids(client, index_name=args.index, doc_ids=verify_doc_ids)
    _verify_invariants(second_docs)
    second_snapshot = _extract_link_snapshot(second_docs)

    if first_snapshot != second_snapshot:
        raise RuntimeError("Idempotency check failed: link snapshot changed on second run.")
    LOGGER.info("Idempotency check passed: second run produced no link changes.")

    LOGGER.info("Second run output length=%s characters.", len(second_output))
    LOGGER.info("Controlled matching scenario finished successfully.")


if __name__ == "__main__":
    main()
