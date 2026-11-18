"""Minimal object-storage abstraction over S3 (AWS or MinIO) and the local filesystem.

Encryption at rest is enforced by bucket default encryption (SSE-KMS, see infra/terraform),
so objects are written without per-request SSE headers (which MinIO would reject).
"""

from __future__ import annotations

import os
import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import BinaryIO, Protocol
from urllib.parse import urlparse


class Storage(Protocol):
    def list(self, prefix: str) -> list[str]: ...
    def exists(self, uri: str) -> bool: ...
    def open_lines(self, uri: str) -> Iterator[bytes]: ...
    def read_bytes(self, uri: str) -> bytes: ...
    def write_bytes(self, uri: str, data: bytes) -> None: ...
    def copy(self, src: str, dst: str) -> None: ...
    def upload_file(self, local_path: Path, dst: str) -> None: ...


def join(base: str, *parts: str) -> str:
    return "/".join([base.rstrip("/"), *[p.strip("/") for p in parts]])


class LocalStorage:
    def list(self, prefix: str) -> list[str]:
        root = Path(prefix)
        if not root.exists():
            return []
        return sorted(str(p) for p in root.rglob("*") if p.is_file())

    def exists(self, uri: str) -> bool:
        return Path(uri).exists()

    def open_lines(self, uri: str) -> Iterator[bytes]:
        with open(uri, "rb") as fh:
            yield from fh

    def read_bytes(self, uri: str) -> bytes:
        return Path(uri).read_bytes()

    def write_bytes(self, uri: str, data: bytes) -> None:
        Path(uri).parent.mkdir(parents=True, exist_ok=True)
        Path(uri).write_bytes(data)

    def copy(self, src: str, dst: str) -> None:
        Path(dst).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dst)

    def upload_file(self, local_path: Path, dst: str) -> None:
        self.copy(str(local_path), dst)


class S3Storage:
    def __init__(self, endpoint_url: str | None = None, client=None):
        if client is None:
            import boto3

            client = boto3.client("s3", endpoint_url=endpoint_url or os.getenv("S3_ENDPOINT_URL") or None)
        self.s3 = client

    @staticmethod
    def _split(uri: str) -> tuple[str, str]:
        parsed = urlparse(uri)
        return parsed.netloc, parsed.path.lstrip("/")

    def list(self, prefix: str) -> list[str]:
        bucket, key = self._split(prefix)
        paginator = self.s3.get_paginator("list_objects_v2")
        uris = []
        for page in paginator.paginate(Bucket=bucket, Prefix=key.rstrip("/") + "/"):
            uris += [f"s3://{bucket}/{obj['Key']}" for obj in page.get("Contents", [])]
        return sorted(uris)

    def exists(self, uri: str) -> bool:
        bucket, key = self._split(uri)
        try:
            self.s3.head_object(Bucket=bucket, Key=key)
            return True
        except self.s3.exceptions.ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def open_lines(self, uri: str) -> Iterator[bytes]:
        bucket, key = self._split(uri)
        body: BinaryIO = self.s3.get_object(Bucket=bucket, Key=key)["Body"]
        yield from body.iter_lines(keepends=True)

    def read_bytes(self, uri: str) -> bytes:
        bucket, key = self._split(uri)
        return self.s3.get_object(Bucket=bucket, Key=key)["Body"].read()

    def write_bytes(self, uri: str, data: bytes) -> None:
        bucket, key = self._split(uri)
        self.s3.put_object(Bucket=bucket, Key=key, Body=data)

    def copy(self, src: str, dst: str) -> None:
        src_bucket, src_key = self._split(src)
        dst_bucket, dst_key = self._split(dst)
        self.s3.copy(
            {"Bucket": src_bucket, "Key": src_key},
            dst_bucket,
            dst_key
        )

    def upload_file(self, local_path: Path, dst: str) -> None:
        bucket, key = self._split(dst)
        self.s3.upload_file(str(local_path), bucket, key)


def storage_for(uri: str, endpoint_url: str | None = None) -> Storage:
    if uri.startswith(("s3://", "s3a://")):
        return S3Storage(endpoint_url)
    return LocalStorage()
