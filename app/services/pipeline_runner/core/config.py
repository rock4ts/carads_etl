"""Settings for pipeline runner service."""

from __future__ import annotations

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_INDEX = "carads1_local"


class PipelineRunnerSettings(BaseSettings):
    """Environment-backed settings used by pipeline runner."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    elasticsearch_url: str = Field(default="http://localhost:9200", alias="ELASTICSEARCH_URL")
    processed_index: str = Field(default=DEFAULT_INDEX, alias="PROCESSED_INDEX")
    elasticsearch_api_key: str | None = Field(default=None, alias="ELASTICSEARCH_API_KEY")
    elasticsearch_username: str | None = Field(default=None, alias="ELASTICSEARCH_USERNAME")
    elasticsearch_password: str | None = Field(default=None, alias="ELASTICSEARCH_PASSWORD")
    telegram_bot_token: str | None = Field(default=None, alias="TELEGRAM_BOT_TOKEN")
    telegram_channel_id: str | None = Field(default=None, alias="TELEGRAM_CHANNEL_ID")
    telegram_reporting_enabled: bool = Field(default=True, alias="TELEGRAM_REPORTING_ENABLED")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    @field_validator(
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


settings = PipelineRunnerSettings()
