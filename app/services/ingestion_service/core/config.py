"""Settings for ingestion service."""

from __future__ import annotations

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class IngestionServiceSettings(BaseSettings):
    """Environment-backed settings used by ingestion service."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    mongo_uri: str = Field(default="mongodb://localhost:27017", alias="MONGO_URI")
    mongo_db: str = Field(default="etl", alias="MONGO_DB")
    raw_collection_name: str = Field(default="raw_ads", alias="RAW_COLLECTION_NAME")
    postgres_database_url: str = Field(
        default="postgresql+psycopg://postgres:postgres@localhost:5432/car_intel",
        alias="POSTGRES_DATABASE_URL",
    )
    parser_api_url: str = Field(
        default="https://highload.kocherov.net/parser/api/auto_api/",
        alias="PARSER_API_URL",
    )
    parser_api_key: str = Field(default="replace-with-parser-api-key", alias="PARSER_API_KEY")
    elasticsearch_url: str = Field(default="http://localhost:9200", alias="ELASTICSEARCH_URL")
    processed_index: str = Field(default="carads1_local", alias="PROCESSED_INDEX")
    elasticsearch_api_key: str | None = Field(default=None, alias="ELASTICSEARCH_API_KEY")
    elasticsearch_username: str | None = Field(default=None, alias="ELASTICSEARCH_USERNAME")
    elasticsearch_password: str | None = Field(default=None, alias="ELASTICSEARCH_PASSWORD")
    telegram_bot_token: str | None = Field(default=None, alias="TELEGRAM_BOT_TOKEN")
    telegram_channel_id: str | None = Field(default=None, alias="TELEGRAM_CHANNEL_ID")
    telegram_reporting_enabled: bool = Field(default=True, alias="TELEGRAM_REPORTING_ENABLED")
    telegram_progress_interval_minutes: int = Field(default=30, alias="TELEGRAM_PROGRESS_INTERVAL_MINUTES")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    @field_validator(
        "mongo_uri",
        "mongo_db",
        "raw_collection_name",
        "postgres_database_url",
        "parser_api_url",
        "parser_api_key",
        "elasticsearch_url",
        "processed_index",
        "elasticsearch_api_key",
        "elasticsearch_username",
        "elasticsearch_password",
        "telegram_bot_token",
        "telegram_channel_id",
        "log_level",
        mode="before",
    )
    @classmethod
    def _strip_string_values(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip()
        return value

    @field_validator("telegram_progress_interval_minutes", mode="before")
    @classmethod
    def _coerce_telegram_progress_interval_minutes(cls, value: object) -> int:
        default_minutes = 30
        if isinstance(value, bool):
            parsed = default_minutes
        elif isinstance(value, int):
            parsed = value
        elif isinstance(value, str):
            try:
                parsed = int(value.strip())
            except ValueError:
                parsed = default_minutes
        else:
            parsed = default_minutes
        return max(1, parsed)


settings = IngestionServiceSettings()
