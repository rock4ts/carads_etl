"""Copy a recent slice of the production Elasticsearch index for validation."""

from __future__ import annotations

import base64
import json
import os
import time
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any
from urllib import error, parse, request

DEFAULT_SOURCE_INDEX = "carads1"
TARGET_INDEX = "carads1_local"
CUTOFF_DATE = datetime(2026, 4, 1).isoformat()
SCROLL_TTL = "2m"
SEARCH_BATCH_SIZE = 3000
BULK_BATCH_SIZE = 500
REQUEST_TIMEOUT_SECONDS = 60
REQUEST_MAX_RETRIES = 4
REQUEST_BACKOFF_BASE_SECONDS = 1.0
RETRYABLE_HTTP_STATUSES = {408, 429, 500, 502, 503, 504}

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


class ElasticsearchHttpClient:
    """Minimal Elasticsearch HTTP client for index copy operations."""

    def __init__(self, base_url: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._headers = {"Content-Type": "application/json"}

        api_key = os.getenv("ELASTICSEARCH_API_KEY")
        username = os.getenv("ELASTICSEARCH_USERNAME")
        password = os.getenv("ELASTICSEARCH_PASSWORD")

        if api_key:
            self._headers["Authorization"] = f"ApiKey {api_key}"
        elif username and password:
            credentials = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
            self._headers["Authorization"] = f"Basic {credentials}"

    def get_json(self, path: str) -> dict[str, Any]:
        return self._request_json("GET", path)

    def put_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request_json("PUT", path, payload=payload)

    def post_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request_json("POST", path, payload=payload)

    def post(self, path: str) -> dict[str, Any]:
        return self._request_json("POST", path)

    def delete(
        self,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        ignore_statuses: set[int] | None = None,
    ) -> dict[str, Any]:
        return self._request_json("DELETE", path, payload=payload, ignore_statuses=ignore_statuses)

    def post_ndjson(self, path: str, lines: Iterable[dict[str, Any]]) -> dict[str, Any]:
        payload = "".join(f"{json.dumps(line, separators=(',', ':'))}\n" for line in lines).encode("utf-8")
        headers = dict(self._headers)
        headers["Content-Type"] = "application/x-ndjson"
        return self._request_json("POST", path, data=payload, headers=headers)

    @staticmethod
    def _get_retry_delay_seconds(attempt_number: int) -> float:
        return REQUEST_BACKOFF_BASE_SECONDS * (2 ** (attempt_number - 1))

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
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
        request_headers = headers or self._headers
        response_body = ""

        for attempt_number in range(1, REQUEST_MAX_RETRIES + 1):
            req = request.Request(
                url=url,
                data=data,
                method=method,
                headers=request_headers,
            )

            try:
                with request.urlopen(req, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                    response_body = response.read().decode("utf-8")
                    break
            except error.HTTPError as exc:
                if ignore_statuses and exc.code in ignore_statuses:
                    return {}
                detail = exc.read().decode("utf-8", errors="replace")
                is_retryable_status = exc.code in RETRYABLE_HTTP_STATUSES
                has_retries_remaining = attempt_number < REQUEST_MAX_RETRIES
                if is_retryable_status and has_retries_remaining:
                    retry_delay_seconds = self._get_retry_delay_seconds(attempt_number)
                    print(
                        f"[retry] {method} {url} failed with HTTP {exc.code}; "
                        f"attempt {attempt_number}/{REQUEST_MAX_RETRIES}, retrying in {retry_delay_seconds:.1f}s"
                    )
                    time.sleep(retry_delay_seconds)
                    continue
                print(
                    f"[error] {method} {url} failed with HTTP {exc.code} "
                    f"on attempt {attempt_number}/{REQUEST_MAX_RETRIES}: {detail}"
                )
                raise RuntimeError(f"Elasticsearch HTTP {exc.code} at {url}: {detail}") from exc
            except (error.URLError, TimeoutError) as exc:
                if attempt_number < REQUEST_MAX_RETRIES:
                    retry_delay_seconds = self._get_retry_delay_seconds(attempt_number)
                    print(
                        f"[retry] {method} {url} failed with {type(exc).__name__}: {exc}; "
                        f"attempt {attempt_number}/{REQUEST_MAX_RETRIES}, retrying in {retry_delay_seconds:.1f}s"
                    )
                    time.sleep(retry_delay_seconds)
                    continue
                print(
                    f"[error] {method} {url} failed with {type(exc).__name__}: {exc} "
                    f"on attempt {attempt_number}/{REQUEST_MAX_RETRIES}"
                )
                raise RuntimeError(f"Failed to reach Elasticsearch at {url}: {exc}") from exc

        if not response_body:
            return {}

        try:
            return json.loads(response_body)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid JSON response from Elasticsearch at {url}") from exc


def _clean_index_settings(index_settings: dict[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for key, value in index_settings.items():
        if key in UNSAFE_INDEX_SETTING_KEYS:
            continue
        cleaned[key] = value
    return cleaned


def _build_target_index_body(source_index_response: dict[str, Any], source_index: str) -> dict[str, Any]:
    source_definition = source_index_response.get(source_index)
    if not isinstance(source_definition, dict):
        raise RuntimeError(f"Could not load definition for source index '{source_index}'.")

    source_settings = source_definition.get("settings", {}).get("index", {})
    if not isinstance(source_settings, dict):
        source_settings = {}

    body: dict[str, Any] = {
        "mappings": source_definition.get("mappings", {}),
    }

    cleaned_settings = _clean_index_settings(source_settings)
    if cleaned_settings:
        body["settings"] = {"index": cleaned_settings}

    return body


def _build_copy_query() -> dict[str, Any]:
    return {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "should": [
                            {"range": {"offer_start": {"gt": CUTOFF_DATE}}},
                            {"range": {"offer_end": {"gt": CUTOFF_DATE}}},
                        ],
                        "minimum_should_match": 1,
                    }
                }
            ]
        }
    }


def _reset_validation_fields(source: dict[str, Any]) -> dict[str, Any]:
    copied = dict(source)
    copied["successor_id"] = None
    return copied


def _yield_bulk_lines(index_name: str, hits: list[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    for hit in hits:
        doc_id = hit.get("_id")
        source = hit.get("_source")
        if doc_id is None or not isinstance(source, dict):
            continue

        yield {"index": {"_index": index_name, "_id": str(doc_id)}}
        yield _reset_validation_fields(source)


def _bulk_index_hits(
    client: ElasticsearchHttpClient,
    *,
    target_index: str,
    hits: list[dict[str, Any]],
) -> int:
    valid_hits = [
        hit
        for hit in hits
        if isinstance(hit, dict) and hit.get("_id") is not None and isinstance(hit.get("_source"), dict)
    ]
    if not valid_hits:
        return 0

    response = client.post_ndjson("/_bulk", _yield_bulk_lines(target_index, valid_hits))
    if response.get("errors"):
        items = response.get("items", [])
        if isinstance(items, list):
            for item in items:
                if not isinstance(item, dict):
                    continue
                action = item.get("index")
                if isinstance(action, dict) and "error" in action:
                    raise RuntimeError(f"Bulk indexing failed: {json.dumps(action['error'], ensure_ascii=False)}")
        raise RuntimeError("Bulk indexing failed with unknown errors.")

    return len(valid_hits)


def _initial_scroll_search(
    client: ElasticsearchHttpClient,
    *,
    source_index: str,
) -> dict[str, Any]:
    query = parse.urlencode({"scroll": SCROLL_TTL})
    return client.post_json(
        f"/{source_index}/_search?{query}",
        {
            "size": SEARCH_BATCH_SIZE,
            "sort": ["_doc"],
            "query": _build_copy_query(),
        },
    )


def _scroll_next(client: ElasticsearchHttpClient, *, scroll_id: str) -> dict[str, Any]:
    return client.post_json(
        "/_search/scroll",
        {
            "scroll": SCROLL_TTL,
            "scroll_id": scroll_id,
        },
    )


def _clear_scroll(client: ElasticsearchHttpClient, *, scroll_id: str | None) -> None:
    if not scroll_id:
        return
    client.delete("/_search/scroll", payload={"scroll_id": [scroll_id]}, ignore_statuses={404})


def _extract_hits(response: dict[str, Any]) -> list[dict[str, Any]]:
    hits = response.get("hits", {}).get("hits", [])
    if not isinstance(hits, list):
        return []
    return [hit for hit in hits if isinstance(hit, dict)]


def recreate_target_index(
    *,
    source_client: ElasticsearchHttpClient,
    target_client: ElasticsearchHttpClient,
    source_index: str,
    target_index: str,
) -> None:
    if source_index == target_index and source_client is target_client:
        raise ValueError("Source and target indexes must be different when using one cluster.")

    source_index_response = source_client.get_json(f"/{source_index}")
    target_body = _build_target_index_body(source_index_response, source_index)

    print(f"Recreating target index '{target_index}' on local Elasticsearch...")
    target_client.delete(f"/{target_index}", ignore_statuses={404})
    target_client.put_json(f"/{target_index}", target_body)
    print(f"Target index '{target_index}' is ready.")


def copy_validation_slice(
    *,
    source_client: ElasticsearchHttpClient,
    target_client: ElasticsearchHttpClient,
    source_index: str,
    target_index: str,
) -> int:
    copied_docs = 0
    scroll_id: str | None = None
    page_number = 0

    try:
        print(f"Starting copy from source index '{source_index}' to '{target_index}'...")
        try:
            response = _initial_scroll_search(source_client, source_index=source_index)
        except RuntimeError as exc:
            print(f"[error] Initial scroll search failed for index '{source_index}': {exc}")
            raise
        scroll_id = response.get("_scroll_id")

        while True:
            hits = _extract_hits(response)
            if not hits:
                break
            page_number += 1
            page_copied = 0

            for start in range(0, len(hits), BULK_BATCH_SIZE):
                batch_copied = _bulk_index_hits(
                    target_client,
                    target_index=target_index,
                    hits=hits[start : start + BULK_BATCH_SIZE],
                )
                copied_docs += batch_copied
                page_copied += batch_copied

            print(f"Progress: page={page_number} page_docs={page_copied} total_docs={copied_docs}")

            if not scroll_id:
                break

            try:
                response = _scroll_next(source_client, scroll_id=scroll_id)
            except RuntimeError as exc:
                print(
                    f"[error] Subsequent scroll request failed for index '{source_index}' "
                    f"after page={page_number}: {exc}"
                )
                raise
            next_scroll_id = response.get("_scroll_id")
            if isinstance(next_scroll_id, str) and next_scroll_id:
                scroll_id = next_scroll_id
    finally:
        _clear_scroll(source_client, scroll_id=scroll_id)

    target_client.post(f"/{target_index}/_refresh")
    return copied_docs


def run() -> None:
    source_es_url = os.getenv("ELASTICSEARCH_URL", "http://10.10.20.11:9200")
    target_es_url = os.getenv("LOCAL_ELASTICSEARCH_URL", "http://localhost:19200")
    source_index = os.getenv("PROCESSED_INDEX", DEFAULT_SOURCE_INDEX)
    target_index = TARGET_INDEX

    source_client = ElasticsearchHttpClient(source_es_url)
    target_client = ElasticsearchHttpClient(target_es_url)
    print(f"Source Elasticsearch: {source_es_url}")
    print(f"Target Elasticsearch: {target_es_url}")
    print(f"Date filter: offer_start OR offer_end > {CUTOFF_DATE}")
    recreate_target_index(
        source_client=source_client,
        target_client=target_client,
        source_index=source_index,
        target_index=target_index,
    )
    copied_docs = copy_validation_slice(
        source_client=source_client,
        target_client=target_client,
        source_index=source_index,
        target_index=target_index,
    )

    print(copied_docs)


if __name__ == "__main__":
    run()
