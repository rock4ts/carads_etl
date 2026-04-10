"""Settings for the matching service worker."""

from __future__ import annotations

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


DEFAULT_INDEX = "carads1"
DEFAULT_POSTGRES_DATABASE_URL = "postgresql+psycopg://postgres:postgres@localhost:5432/car_intel"
DEFAULT_BATCH_SIZE = 500


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
    matching_sites_raw: str | None = Field(default=None, alias="MATCHING_SITES")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    elasticsearch_api_key: str | None = Field(default=None, alias="ELASTICSEARCH_API_KEY")
    elasticsearch_username: str | None = Field(default=None, alias="ELASTICSEARCH_USERNAME")
    elasticsearch_password: str | None = Field(default=None, alias="ELASTICSEARCH_PASSWORD")

    @field_validator("elasticsearch_url", "processed_index", "postgres_database_url", "log_level", mode="before")
    @classmethod
    def _strip_strings(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip()
        return value

    @field_validator("matching_batch_size", mode="before")
    @classmethod
    def _coerce_batch_size(cls, value: object) -> int:
        if isinstance(value, str):
            value = value.strip()
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            parsed = DEFAULT_BATCH_SIZE
        return max(500, min(parsed, 1000))

    @property
    def matching_sites(self) -> list[str] | None:
        raw_value = self.matching_sites_raw
        if raw_value is None:
            return None
        sites = [site.strip() for site in raw_value.split(",") if site.strip()]
        return sites or None


settings = MatchingServiceSettings()

