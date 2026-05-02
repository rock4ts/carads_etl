"""Settings for the Mongo raw-ad archiving service."""

from __future__ import annotations

from typing import Any

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_ARCHIVE_RETENTION_DAYS = 60
DEFAULT_ARCHIVE_BATCH_SIZE = 5000
DEFAULT_S3_ENDPOINT = "https://storage.yandexcloud.net"
DEFAULT_S3_PREFIX = "raw-archive/"
DEFAULT_POSTGRES_DATABASE_URL = (
    "postgresql+psycopg://postgres:postgres@localhost:5432/car_intel"
)


class ArchivingServiceSettings(BaseSettings):
    """Environment-backed settings used by the archiving service."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    mongo_uri: str = Field(default="mongodb://localhost:27017", alias="MONGO_URI")
    mongo_db: str = Field(default="etl", alias="MONGO_DB")
    raw_collection_name: str = Field(default="raw_ads", alias="RAW_COLLECTION_NAME")
    postgres_database_url: str = Field(
        default=DEFAULT_POSTGRES_DATABASE_URL,
        alias="POSTGRES_DATABASE_URL",
    )
    archive_retention_days: int = Field(
        default=DEFAULT_ARCHIVE_RETENTION_DAYS, alias="ARCHIVE_RETENTION_DAYS"
    )
    archive_batch_size: int = Field(
        default=DEFAULT_ARCHIVE_BATCH_SIZE, alias="ARCHIVE_BATCH_SIZE"
    )
    s3_bucket: str = Field(default="your-bucket", alias="S3_BUCKET")
    s3_prefix: str = Field(default=DEFAULT_S3_PREFIX, alias="S3_PREFIX")
    s3_endpoint: str = Field(default=DEFAULT_S3_ENDPOINT, alias="S3_ENDPOINT")
    aws_access_key_id: str = Field(
        default="replace-with-access-key", alias="AWS_ACCESS_KEY_ID"
    )
    aws_secret_access_key: str = Field(
        default="replace-with-secret-key", alias="AWS_SECRET_ACCESS_KEY"
    )
    aws_region: str | None = Field(default=None, alias="AWS_REGION")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    @field_validator(
        "mongo_uri",
        "mongo_db",
        "raw_collection_name",
        "postgres_database_url",
        "s3_bucket",
        "s3_prefix",
        "s3_endpoint",
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_region",
        "log_level",
        mode="before",
    )
    @classmethod
    def _strip_strings(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip()
        return value

    @field_validator("archive_retention_days", mode="before")
    @classmethod
    def _coerce_archive_retention_days(cls, value: object) -> int:
        raw_value: Any = value
        if isinstance(raw_value, str):
            raw_value = raw_value.strip()
        try:
            parsed = int(raw_value)
        except (TypeError, ValueError):
            parsed = DEFAULT_ARCHIVE_RETENTION_DAYS
        return max(1, parsed)

    @field_validator("archive_batch_size", mode="before")
    @classmethod
    def _coerce_archive_batch_size(cls, value: object) -> int:
        raw_value: Any = value
        if isinstance(raw_value, str):
            raw_value = raw_value.strip()
        try:
            parsed = int(raw_value)
        except (TypeError, ValueError):
            parsed = DEFAULT_ARCHIVE_BATCH_SIZE
        return max(1000, min(parsed, 5000))

    @property
    def normalized_s3_prefix(self) -> str:
        return self.s3_prefix.strip("/")


settings = ArchivingServiceSettings()
