"""Telegram reporting helpers for long-running workers."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Protocol

from telegram import Bot
from telegram.error import TelegramError


class TelegramReportingSettings(Protocol):
    """Minimal settings contract required by TelegramReporter."""

    telegram_bot_token: str | None
    telegram_channel_id: str | None
    telegram_reporting_enabled: bool


@dataclass(frozen=True)
class TelegramReporterConfig:
    """Configuration for TelegramReporter."""

    bot_token: str | None
    channel_id: str | None
    enabled: bool
    service_name: str


class TelegramReporter:
    """Best-effort Telegram reporter that never raises to callers."""

    def __init__(self, config: TelegramReporterConfig, *, logger: logging.Logger | None = None) -> None:
        self._service_name = config.service_name
        self._logger = logger or logging.getLogger(__name__)
        self._channel_id = config.channel_id
        self._enabled = bool(config.enabled and config.bot_token and config.channel_id)
        self._bot = Bot(token=config.bot_token) if self._enabled and config.bot_token is not None else None

    @classmethod
    def from_settings(
        cls,
        *,
        service_name: str,
        settings: TelegramReportingSettings,
        logger: logging.Logger | None = None,
    ) -> "TelegramReporter":
        config = TelegramReporterConfig(
            bot_token=settings.telegram_bot_token,
            channel_id=settings.telegram_channel_id,
            enabled=settings.telegram_reporting_enabled,
            service_name=service_name,
        )
        return cls(config, logger=logger)

    async def send_progress(self, message: str) -> None:
        await self._send(level="PROGRESS", message=message)

    async def send_critical(self, message: str) -> None:
        await self._send(level="CRITICAL", message=message)

    async def _send(self, *, level: str, message: str) -> None:
        if not self._enabled or self._bot is None or self._channel_id is None:
            return
        text = f"[{self._service_name}] [{level}] {message}"
        try:
            await self._bot.send_message(chat_id=self._channel_id, text=text)
        except TelegramError as exc:
            self._logger.warning(
                "Telegram reporting failed for %s level=%s: %s",
                self._service_name,
                level,
                exc,
            )
        except Exception:
            self._logger.exception(
                "Unexpected Telegram reporting error for %s level=%s",
                self._service_name,
                level,
            )
