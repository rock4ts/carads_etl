"""Retry helpers for external HTTP requests."""

from __future__ import annotations

import logging
import socket
from collections.abc import Callable
from typing import Any, TypeVar, cast
from urllib import error

import backoff
from backoff._typing import Details

logger = logging.getLogger(__name__)

_ReturnT = TypeVar("_ReturnT")
BackoffNotifier = Callable[[str], None]

_RETRYABLE_HTTP_STATUSES = {408, 429, 502, 503, 504}
_RETRYABLE_EXCEPTIONS = (
    TimeoutError,
    socket.timeout,
    ConnectionError,
    ConnectionResetError,
    ConnectionAbortedError,
    BrokenPipeError,
    error.HTTPError,
)


def _is_retryable_http_error(exc: BaseException) -> bool:
    if isinstance(exc, error.HTTPError):
        return exc.code in _RETRYABLE_HTTP_STATUSES
    return True


def _format_backoff_message(operation: str, details: dict[str, Any]) -> str:
    tries = details.get("tries", 0)
    wait_seconds = details.get("wait", 0.0)
    exc = details.get("exception")
    error_type = type(exc).__name__ if exc is not None else "UnknownError"
    return (
        f"status=retrying operation={operation} attempt={tries} wait={wait_seconds:.1f}s "
        f"error_type={error_type} error={exc!s}"
    )


def _build_on_backoff(
    *,
    operation: str,
    notify: BackoffNotifier | None,
) -> Callable[[Details], None]:
    def _on_backoff(details: Details) -> None:
        message = _format_backoff_message(operation, cast(dict[str, Any], details))
        logger.warning("External request backoff: %s", message)
        if notify is None:
            return
        try:
            notify(message)
        except Exception:
            logger.exception("Backoff notifier failed for operation=%s", operation)

    return _on_backoff


def with_http_backoff(
    *,
    operation: str,
    notify: BackoffNotifier | None = None,
) -> Callable[[Callable[..., _ReturnT]], Callable[..., _ReturnT]]:
    """Create a backoff decorator for transient external HTTP failures."""
    return backoff.on_exception(
        backoff.expo,
        _RETRYABLE_EXCEPTIONS,
        max_time=3600,
        giveup=lambda exc: not _is_retryable_http_error(exc),
        on_backoff=_build_on_backoff(operation=operation, notify=notify),
    )
