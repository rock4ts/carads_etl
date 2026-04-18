from __future__ import annotations

import logging
from dataclasses import dataclass

from telegram.error import TelegramError

from app.services.telegram_notifier import TelegramReporter


@dataclass
class _Settings:
    telegram_bot_token: str | None
    telegram_channel_id: str | None
    telegram_reporting_enabled: bool


def test_reporter_skips_when_not_configured(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    class _FakeBot:
        def __init__(self, token: str) -> None:
            self._token = token

        async def send_message(self, *, chat_id: str, text: str) -> None:
            calls.append((chat_id, text))

    monkeypatch.setattr("app.services.telegram_notifier.Bot", _FakeBot)
    reporter = TelegramReporter.from_settings(
        service_name="ingestion",
        settings=_Settings(
            telegram_bot_token=None,
            telegram_channel_id="@etl_reports",
            telegram_reporting_enabled=True,
        ),
    )

    import asyncio

    asyncio.run(reporter.send_progress("status=start"))

    assert calls == []


def test_reporter_logs_warning_on_telegram_error(monkeypatch, caplog) -> None:
    class _FailingBot:
        def __init__(self, token: str) -> None:
            self._token = token

        async def send_message(self, *, chat_id: str, text: str) -> None:
            raise TelegramError("send failed")

    monkeypatch.setattr("app.services.telegram_notifier.Bot", _FailingBot)
    reporter = TelegramReporter.from_settings(
        service_name="matching",
        settings=_Settings(
            telegram_bot_token="bot-token",
            telegram_channel_id="@etl_reports",
            telegram_reporting_enabled=True,
        ),
        logger=logging.getLogger("test.telegram"),
    )

    import asyncio

    with caplog.at_level(logging.WARNING):
        asyncio.run(reporter.send_critical("status=failed error=boom"))

    assert "Telegram reporting failed" in caplog.text
