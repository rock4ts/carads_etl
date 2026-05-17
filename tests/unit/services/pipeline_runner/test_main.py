from __future__ import annotations

import asyncio
from datetime import datetime, timezone, tzinfo
from typing import override

import pytest

from app.services.pipeline_runner import main as pipeline_main


def _patch_reporter(
    monkeypatch: pytest.MonkeyPatch,
    report_events: list[tuple[str, str]],
) -> None:
    class _Reporter:
        @classmethod
        def from_settings(
            cls,
            *,
            service_name: str,
            settings: object,
            logger: object | None = None,
        ) -> "_Reporter":
            _ = (settings, logger)
            assert service_name == "pipeline_runner"
            return cls()

        async def send_progress(self, message: str) -> None:
            report_events.append(("progress", message))

        async def send_critical(self, message: str) -> None:
            report_events.append(("critical", message))

    monkeypatch.setattr(pipeline_main, "TelegramReporter", _Reporter)


def test_run_pipeline_executes_stages_in_strict_order(monkeypatch: pytest.MonkeyPatch) -> None:
    started_at = datetime(2026, 5, 17, 12, 0, 0, tzinfo=timezone.utc)
    finished_at = datetime(2026, 5, 17, 12, 0, 5, tzinfo=timezone.utc)
    now_values = iter([started_at, finished_at, finished_at])
    stage_order: list[str] = []
    report_events: list[tuple[str, str]] = []
    captured_load_till: datetime | None = None

    class _FrozenDatetime(datetime):
        @classmethod
        @override
        def now(cls, tz: tzinfo | None = None) -> datetime:
            current = next(now_values)
            if tz is None:
                return current.replace(tzinfo=None)
            return current.astimezone(tz)

    async def _fake_run_ingestion(*, load_till: datetime) -> None:
        nonlocal captured_load_till
        stage_order.append("ingestion")
        captured_load_till = load_till

    async def _fake_refresh() -> None:
        stage_order.append("refresh")

    async def _fake_run_matcher() -> None:
        stage_order.append("matcher")

    async def _fake_run_archive() -> None:
        stage_order.append("archive")

    monkeypatch.setattr(pipeline_main, "datetime", _FrozenDatetime)
    monkeypatch.setattr(pipeline_main, "run_ingestion", _fake_run_ingestion)
    monkeypatch.setattr(pipeline_main, "_refresh_processed_index", _fake_refresh)
    monkeypatch.setattr(pipeline_main, "run_matcher", _fake_run_matcher)
    monkeypatch.setattr(pipeline_main, "run_archive", _fake_run_archive)
    _patch_reporter(monkeypatch, report_events)

    asyncio.run(pipeline_main.run_pipeline())

    assert stage_order == ["ingestion", "refresh", "matcher", "archive"]
    assert captured_load_till == started_at.replace(tzinfo=None)
    assert report_events == [
        ("progress", "Pipeline started | load_till=2026-05-17T12:00:00"),
        ("progress", "Ingestion completed"),
        ("progress", "Matcher completed"),
        ("progress", "Archive completed"),
        (
            "progress",
            "Pipeline finished successfully | duration=5s | load_till=2026-05-17T12:00:00",
        ),
    ]


def test_run_pipeline_stops_when_ingestion_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    stage_order: list[str] = []
    report_events: list[tuple[str, str]] = []

    async def _failing_ingestion(*, load_till: datetime) -> None:
        _ = load_till
        stage_order.append("ingestion")
        raise RuntimeError("ingestion failed")

    async def _fake_refresh() -> None:
        stage_order.append("refresh")

    async def _fake_run_matcher() -> None:
        stage_order.append("matcher")

    async def _fake_run_archive() -> None:
        stage_order.append("archive")

    monkeypatch.setattr(pipeline_main, "run_ingestion", _failing_ingestion)
    monkeypatch.setattr(pipeline_main, "_refresh_processed_index", _fake_refresh)
    monkeypatch.setattr(pipeline_main, "run_matcher", _fake_run_matcher)
    monkeypatch.setattr(pipeline_main, "run_archive", _fake_run_archive)
    _patch_reporter(monkeypatch, report_events)

    asyncio.run(pipeline_main.run_pipeline())

    assert stage_order == ["ingestion"]
    assert report_events[-1] == ("critical", "Ingestion failed: ingestion failed")
    assert all("Pipeline finished successfully" not in message for _, message in report_events)


def test_run_pipeline_stops_when_refresh_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    stage_order: list[str] = []
    report_events: list[tuple[str, str]] = []

    async def _fake_ingestion(*, load_till: datetime) -> None:
        _ = load_till
        stage_order.append("ingestion")

    async def _failing_refresh() -> None:
        stage_order.append("refresh")
        raise RuntimeError("refresh failed")

    async def _fake_run_matcher() -> None:
        stage_order.append("matcher")

    async def _fake_run_archive() -> None:
        stage_order.append("archive")

    monkeypatch.setattr(pipeline_main, "run_ingestion", _fake_ingestion)
    monkeypatch.setattr(pipeline_main, "_refresh_processed_index", _failing_refresh)
    monkeypatch.setattr(pipeline_main, "run_matcher", _fake_run_matcher)
    monkeypatch.setattr(pipeline_main, "run_archive", _fake_run_archive)
    _patch_reporter(monkeypatch, report_events)

    asyncio.run(pipeline_main.run_pipeline())

    assert stage_order == ["ingestion", "refresh"]
    assert report_events[-1] == ("progress", "Ingestion completed")
    assert all(level != "critical" for level, _ in report_events)


def test_run_pipeline_stops_when_matcher_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    stage_order: list[str] = []
    report_events: list[tuple[str, str]] = []

    async def _fake_ingestion(*, load_till: datetime) -> None:
        _ = load_till
        stage_order.append("ingestion")

    async def _fake_refresh() -> None:
        stage_order.append("refresh")

    async def _failing_matcher() -> None:
        stage_order.append("matcher")
        raise RuntimeError("matcher failed")

    async def _fake_run_archive() -> None:
        stage_order.append("archive")

    monkeypatch.setattr(pipeline_main, "run_ingestion", _fake_ingestion)
    monkeypatch.setattr(pipeline_main, "_refresh_processed_index", _fake_refresh)
    monkeypatch.setattr(pipeline_main, "run_matcher", _failing_matcher)
    monkeypatch.setattr(pipeline_main, "run_archive", _fake_run_archive)
    _patch_reporter(monkeypatch, report_events)

    asyncio.run(pipeline_main.run_pipeline())

    assert stage_order == ["ingestion", "refresh", "matcher"]
    assert report_events[-1] == ("critical", "Matcher failed: matcher failed")
    assert all("Pipeline finished successfully" not in message for _, message in report_events)


def test_run_pipeline_logs_and_returns_when_archive_fails(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    stage_order: list[str] = []
    report_events: list[tuple[str, str]] = []

    async def _fake_ingestion(*, load_till: datetime) -> None:
        _ = load_till
        stage_order.append("ingestion")

    async def _fake_refresh() -> None:
        stage_order.append("refresh")

    async def _fake_run_matcher() -> None:
        stage_order.append("matcher")

    async def _failing_archive() -> None:
        stage_order.append("archive")
        raise RuntimeError("archive failed")

    monkeypatch.setattr(pipeline_main, "run_ingestion", _fake_ingestion)
    monkeypatch.setattr(pipeline_main, "_refresh_processed_index", _fake_refresh)
    monkeypatch.setattr(pipeline_main, "run_matcher", _fake_run_matcher)
    monkeypatch.setattr(pipeline_main, "run_archive", _failing_archive)
    _patch_reporter(monkeypatch, report_events)

    with caplog.at_level("ERROR"):
        asyncio.run(pipeline_main.run_pipeline())

    assert stage_order == ["ingestion", "refresh", "matcher", "archive"]
    assert "Archive failed: archive failed" in caplog.text
    assert report_events[-1] == ("critical", "Archive failed: archive failed")
    assert all("Pipeline finished successfully" not in message for _, message in report_events)
