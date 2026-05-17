"""Settings for the matching service worker."""

from __future__ import annotations

from typing import Any

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_INDEX = "carads1_local"
DEFAULT_POSTGRES_DATABASE_URL = "postgresql+psycopg://postgres:postgres@localhost:5432/car_intel"
DEFAULT_BATCH_SIZE = 500
DEFAULT_MIN_SCORE = 0.7
DEFAULT_TIME_WINDOW_DAYS = 5
DEFAULT_PARSER_LAG_DAYS = 3
DEFAULT_PRICE_TOLERANCE = 0.10
DEFAULT_MILEAGE_TOLERANCE = 0.05
DEFAULT_MAX_RESULTS = 200
DEFAULT_TELEGRAM_PROGRESS_INTERVAL_MINUTES = 30


class MatchingServiceSettings(BaseSettings):
    """Environment-backed settings for the matching worker."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    elasticsearch_url: str = Field(default="http://localhost:9200", alias="ELASTICSEARCH_URL")
    processed_index: str = Field(default=DEFAULT_INDEX, alias="PROCESSED_INDEX")
    postgres_database_url: str = Field(
        default=DEFAULT_POSTGRES_DATABASE_URL,
        validation_alias=AliasChoices("POSTGRES_DATABASE_URL", "DATABASE_URL"),
    )
    matching_batch_size: int = Field(default=DEFAULT_BATCH_SIZE, alias="MATCHING_BATCH_SIZE")
    matching_min_score: float = Field(default=DEFAULT_MIN_SCORE, alias="MATCHING_MIN_SCORE")
    matching_time_window_days: int = Field(default=DEFAULT_TIME_WINDOW_DAYS, alias="MATCHING_TIME_WINDOW_DAYS")
    matching_parser_lag_days: int = Field(default=DEFAULT_PARSER_LAG_DAYS, alias="MATCHING_PARSER_LAG_DAYS")
    matching_price_tolerance: float = Field(default=DEFAULT_PRICE_TOLERANCE, alias="MATCHING_PRICE_TOLERANCE")
    matching_mileage_tolerance: float = Field(default=DEFAULT_MILEAGE_TOLERANCE, alias="MATCHING_MILEAGE_TOLERANCE")
    matching_max_results: int = Field(default=DEFAULT_MAX_RESULTS, alias="MATCHING_MAX_RESULTS")
    matching_sites_raw: str | None = Field(default=None, alias="MATCHING_SITES")
    telegram_bot_token: str | None = Field(default=None, alias="TELEGRAM_BOT_TOKEN")
    telegram_channel_id: str | None = Field(default=None, alias="TELEGRAM_CHANNEL_ID")
    telegram_reporting_enabled: bool = Field(default=True, alias="TELEGRAM_REPORTING_ENABLED")
    telegram_progress_interval_minutes: int = Field(
        default=DEFAULT_TELEGRAM_PROGRESS_INTERVAL_MINUTES,
        alias="TELEGRAM_PROGRESS_INTERVAL_MINUTES",
    )
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    elasticsearch_api_key: str | None = Field(default=None, alias="ELASTICSEARCH_API_KEY")
    elasticsearch_username: str | None = Field(default=None, alias="ELASTICSEARCH_USERNAME")
    elasticsearch_password: str | None = Field(default=None, alias="ELASTICSEARCH_PASSWORD")

    @field_validator(
        "elasticsearch_url",
        "processed_index",
        "postgres_database_url",
        "matching_sites_raw",
        "log_level",
        "elasticsearch_api_key",
        "elasticsearch_username",
        "elasticsearch_password",
        "telegram_bot_token",
        "telegram_channel_id",
        mode="before",
    )
    @classmethod
    def _strip_strings(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip()
        return value

    @field_validator("matching_batch_size", mode="before")
    @classmethod
    def _coerce_batch_size(cls, value: object) -> int:
        raw_value: Any = value
        if isinstance(raw_value, str):
            raw_value = raw_value.strip()
        try:
            parsed = int(raw_value)
        except (TypeError, ValueError):
            parsed = DEFAULT_BATCH_SIZE
        return max(500, min(parsed, 1000))

    @field_validator("matching_max_results", mode="before")
    @classmethod
    def _coerce_max_results(cls, value: object) -> int:
        raw_value: Any = value
        if isinstance(raw_value, str):
            raw_value = raw_value.strip()
        try:
            parsed = int(raw_value)
        except (TypeError, ValueError):
            parsed = DEFAULT_MAX_RESULTS
        return max(1, min(parsed, 500))

    @field_validator("telegram_progress_interval_minutes", mode="before")
    @classmethod
    def _coerce_telegram_progress_interval_minutes(cls, value: object) -> int:
        raw_value: Any = value
        if isinstance(raw_value, str):
            raw_value = raw_value.strip()
        try:
            parsed = int(raw_value)
        except (TypeError, ValueError):
            parsed = DEFAULT_TELEGRAM_PROGRESS_INTERVAL_MINUTES
        return max(1, parsed)

    @property
    def matching_sites(self) -> list[str] | None:
        raw_value = self.matching_sites_raw
        if raw_value is None:
            return None
        sites = [site.strip() for site in raw_value.split(",") if site.strip()]
        return sites or None


settings = MatchingServiceSettings()
