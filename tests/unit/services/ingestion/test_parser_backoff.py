from __future__ import annotations

import io
from datetime import datetime
from http.client import HTTPMessage
from urllib import error

import pytest

from app.services.ingestion_service import main as ingestion_main


class _Response:
    def __init__(self, body: str) -> None:
        self._body = body.encode("utf-8")

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


@pytest.fixture(autouse=True)
def _disable_backoff_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("backoff._sync.time.sleep", lambda _: None)


def test_fetch_parser_ads_sync_retries_transient_http_and_notifies(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}
    notifications: list[str] = []

    def _fake_urlopen(req: object, timeout: int) -> _Response:
        calls["count"] += 1
        if calls["count"] == 1:
            raise error.HTTPError(
                url="https://parser.example/api",
                code=503,
                msg="service unavailable",
                hdrs=HTTPMessage(),
                fp=io.BytesIO(b"temporary outage"),
            )
        return _Response('[{"checked": "2026-01-01 00:10:00"}]')

    monkeypatch.setattr(ingestion_main.request, "urlopen", _fake_urlopen)
    result = ingestion_main._fetch_parser_ads_sync(
        parser_api_url="https://parser.example/api",
        parser_api_key="secret",
        site_name="avito",
        current_from=datetime(2026, 1, 1, 0, 0, 0),
        on_backoff_notify=notifications.append,
    )

    assert calls["count"] == 2
    assert result == [{"checked": "2026-01-01 00:10:00"}]
    assert len(notifications) == 1
    assert "operation=parser_fetch:avito" in notifications[0]


def test_fetch_parser_ads_sync_does_not_retry_non_retryable_http_error(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}

    def _fake_urlopen(req: object, timeout: int) -> _Response:
        calls["count"] += 1
        raise error.HTTPError(
            url="https://parser.example/api",
            code=400,
            msg="bad request",
            hdrs=HTTPMessage(),
            fp=io.BytesIO(b"bad request"),
        )

    monkeypatch.setattr(ingestion_main.request, "urlopen", _fake_urlopen)

    with pytest.raises(RuntimeError, match="Parser HTTP 400"):
        ingestion_main._fetch_parser_ads_sync(
            parser_api_url="https://parser.example/api",
            parser_api_key="secret",
            site_name="avito",
            current_from=datetime(2026, 1, 1, 0, 0, 0),
        )

    assert calls["count"] == 1
