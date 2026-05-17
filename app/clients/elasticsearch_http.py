"""Low-level HTTP client for Elasticsearch."""

from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import Sequence
from typing import Any
from urllib import error, parse, request

REQUEST_TIMEOUT_SECONDS = 30
REQUEST_HEADERS = {"Content-Type": "application/json"}


class ElasticsearchHttpClient:
    """Minimal async-friendly Elasticsearch HTTP client."""

    def __init__(
        self,
        base_url: str,
        *,
        api_key: str | None = None,
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._headers = dict(REQUEST_HEADERS)

        if api_key:
            self._headers["Authorization"] = f"ApiKey {api_key}"
        elif username and password:
            credentials = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
            self._headers["Authorization"] = f"Basic {credentials}"

    async def search(self, **kwargs: Any) -> dict[str, Any]:
        index = str(kwargs["index"])
        query = kwargs["query"]
        size = int(kwargs["size"])
        sort = kwargs.get("sort")
        source = kwargs.get("source")
        search_after = kwargs.get("search_after")

        body: dict[str, Any] = {
            "query": query,
            "size": size,
        }
        if sort is not None:
            body["sort"] = sort
        if source is not None:
            body["_source"] = source
        if search_after is not None:
            body["search_after"] = list(search_after)
        return await self._request_json("POST", f"/{index}/_search", payload=body)

    async def update(
        self,
        *,
        index: str,
        doc_id: str,
        body: dict[str, Any],
        refresh: bool = False,
    ) -> dict[str, Any]:
        encoded_doc_id = parse.quote(str(doc_id), safe="")
        path = f"/{index}/_update/{encoded_doc_id}"
        if refresh:
            path = f"{path}?refresh=true"
        return await self._request_json("POST", path, payload=body)

    async def put_mapping(self, *, index: str, body: dict[str, Any]) -> dict[str, Any]:
        return await self._request_json("PUT", f"/{index}/_mapping", payload=body)

    async def refresh_index(self, *, index: str) -> dict[str, Any]:
        return await self._request_json("POST", f"/{index}/_refresh")

    async def bulk(
        self,
        *,
        operations: Sequence[dict[str, Any]],
        refresh: bool = False,
    ) -> dict[str, Any]:
        if not operations:
            return {"errors": False, "items": []}
        path = "/_bulk?refresh=true" if refresh else "/_bulk"
        return await self._post_ndjson(path, operations)

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await asyncio.to_thread(self._request_json_sync, method, path, payload)

    def _request_json_sync(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None,
    ) -> dict[str, Any]:
        url = f"{self._base_url}{path}"
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        req = request.Request(
            url=url,
            data=data,
            method=method,
            headers=self._headers,
        )
        try:
            with request.urlopen(req, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                response_body = response.read().decode("utf-8")
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Elasticsearch HTTP {exc.code} at {url}: {detail}") from exc
        except error.URLError as exc:
            raise RuntimeError(f"Failed to reach Elasticsearch at {url}: {exc}") from exc

        if not response_body:
            return {}

        try:
            return json.loads(response_body)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid JSON response from Elasticsearch at {url}") from exc

    async def _post_ndjson(self, path: str, lines: Sequence[dict[str, Any]]) -> dict[str, Any]:
        return await asyncio.to_thread(self._post_ndjson_sync, path, lines)

    def _post_ndjson_sync(self, path: str, lines: Sequence[dict[str, Any]]) -> dict[str, Any]:
        payload = "".join(f"{json.dumps(line, separators=(',', ':'))}\n" for line in lines).encode("utf-8")
        headers = dict(self._headers)
        headers["Content-Type"] = "application/x-ndjson"
        url = f"{self._base_url}{path}"
        req = request.Request(
            url=url,
            data=payload,
            method="POST",
            headers=headers,
        )
        try:
            with request.urlopen(req, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                response_body = response.read().decode("utf-8")
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Elasticsearch HTTP {exc.code} at {url}: {detail}") from exc
        except error.URLError as exc:
            raise RuntimeError(f"Failed to reach Elasticsearch at {url}: {exc}") from exc

        if not response_body:
            return {}
        try:
            return json.loads(response_body)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid JSON response from Elasticsearch at {url}") from exc
