"""Yandex Object Storage adapter for archive uploads."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

import boto3


class ArchiveStorage(Protocol):
    """Archive object storage contract."""

    def upload_and_verify(self, *, local_path: Path, object_key: str) -> None:
        """Upload a local archive file and verify the object exists."""


class YandexObjectStorageArchiveStorage(ArchiveStorage):
    """S3-compatible archive storage backed by Yandex Object Storage."""

    def __init__(
        self,
        *,
        bucket: str,
        endpoint_url: str,
        access_key_id: str,
        secret_access_key: str,
        region_name: str | None = None,
    ) -> None:
        self._bucket = bucket
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region_name,
        )

    def upload_and_verify(self, *, local_path: Path, object_key: str) -> None:
        self._client.upload_file(str(local_path), self._bucket, object_key)
        self._client.head_object(Bucket=self._bucket, Key=object_key)
