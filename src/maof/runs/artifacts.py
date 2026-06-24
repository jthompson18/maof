"""Artifact store + reference passing.

Subagents write large outputs here and return *references*, not blobs — the
"avoid the game of telephone" pattern. Backends: Postgres (``pg``, the
default), S3/MinIO (``s3``), and an in-memory store for tests/embedded use.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from maof.persistence.base import ArtifactRepo


@runtime_checkable
class ArtifactStore(Protocol):
    async def put(self, run_id: str, name: str, data: bytes, content_type: str) -> str: ...

    async def get(self, ref: str) -> bytes: ...


class InMemoryArtifactStore:
    def __init__(self) -> None:
        self._store: dict[str, bytes] = {}
        self._seq = 0

    async def put(self, run_id: str, name: str, data: bytes, content_type: str) -> str:
        self._seq += 1
        ref = f"mem://{run_id}/{self._seq}/{name}"
        self._store[ref] = bytes(data)
        return ref

    async def get(self, ref: str) -> bytes:
        if ref not in self._store:
            raise KeyError(f"artifact not found: {ref!r}")
        return self._store[ref]


class PostgresArtifactStore:
    def __init__(self, repo: ArtifactRepo) -> None:
        self._repo = repo

    async def put(self, run_id: str, name: str, data: bytes, content_type: str) -> str:
        return await self._repo.put(run_id, name, data, content_type)

    async def get(self, ref: str) -> bytes:
        data = await self._repo.get(ref)
        if data is None:
            raise KeyError(f"artifact not found: {ref!r}")
        return data


class S3ArtifactStore:
    """S3/MinIO-backed store (requires the ``s3`` extra). Refs are ``s3://bucket/key``."""

    def __init__(
        self,
        bucket: str,
        *,
        region: str | None = None,
        endpoint_url: str | None = None,
        session: Any | None = None,
    ) -> None:
        self._bucket = bucket
        self._region = region
        self._endpoint_url = endpoint_url
        self._session = session

    def _client(self) -> Any:
        if self._session is None:
            import aioboto3

            self._session = aioboto3.Session()
        return self._session.client("s3", region_name=self._region, endpoint_url=self._endpoint_url)

    async def put(self, run_id: str, name: str, data: bytes, content_type: str) -> str:
        key = f"{run_id}/{name}"
        async with self._client() as s3:
            await s3.put_object(Bucket=self._bucket, Key=key, Body=data, ContentType=content_type)
        return f"s3://{self._bucket}/{key}"

    async def get(self, ref: str) -> bytes:
        _, _, rest = ref.partition("://")
        bucket, _, key = rest.partition("/")
        async with self._client() as s3:
            response = await s3.get_object(Bucket=bucket, Key=key)
            body: bytes = await response["Body"].read()
        return body


__all__ = ["ArtifactStore", "InMemoryArtifactStore", "PostgresArtifactStore", "S3ArtifactStore"]
