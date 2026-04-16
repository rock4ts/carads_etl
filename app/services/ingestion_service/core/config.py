"""Settings for ingestion service."""

from __future__ import annotations

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class IngestionServiceSettings(BaseSettings):
    """Environment-backed settings used by ingestion service."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    mongo_uri: str | None = Field(default="mongodb://localhost:27017", alias="MONGO_URI")
    mongo_db: str | None = Field(default="etl_db", alias="MONGO_DB")
    raw_collection_name: str | None = Field(default="raw_ads", alias="RAW_COLLECTION_NAME")
    elasticsearch_url: str | None = Field(default="http://localhost:9200", alias="ELASTICSEARCH_URL")
    elasticsearch_api_key: str | None = Field(default=None, alias="ELASTICSEARCH_API_KEY")
    elasticsearch_username: str | None = Field(default=None, alias="ELASTICSEARCH_USERNAME")
    elasticsearch_password: str | None = Field(default=None, alias="ELASTICSEARCH_PASSWORD")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    @field_validator(
        "mongo_uri",
        "mongo_db",
        "raw_collection_name",
        "elasticsearch_url",
        "elasticsearch_api_key",
        "elasticsearch_username",
        "elasticsearch_password",
        "log_level",
        mode="before",
    )
    @classmethod
    def _strip_string_values(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip()
        return value


settings = IngestionServiceSettings()
