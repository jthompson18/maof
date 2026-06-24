"""S3 artifact store logic offline — fake aioboto3-style session.

The live MinIO round trip lives in tests/local (live tier); these cover ref
construction/parsing and the put/get client calls.
"""

from __future__ import annotations

from typing import Any

from maof.runs.artifacts import S3ArtifactStore


class _Body:
    def __init__(self, data: bytes) -> None:
        self._data = data

    async def read(self) -> bytes:
        return self._data


class _FakeS3Client:
    def __init__(self) -> None:
        self.puts: list[dict[str, Any]] = []
        self.objects: dict[tuple[str, str], bytes] = {}

    async def __aenter__(self) -> _FakeS3Client:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        return None

    async def put_object(self, *, Bucket: str, Key: str, Body: bytes, ContentType: str) -> None:
        self.puts.append({"Bucket": Bucket, "Key": Key, "ContentType": ContentType})
        self.objects[(Bucket, Key)] = Body

    async def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        return {"Body": _Body(self.objects[(Bucket, Key)])}


class _FakeSession:
    def __init__(self, client: _FakeS3Client) -> None:
        self._client = client
        self.kwargs: dict[str, Any] = {}

    def client(self, service: str, **kwargs: Any) -> _FakeS3Client:
        self.kwargs = {"service": service, **kwargs}
        return self._client


async def test_put_builds_ref_and_get_parses_it_back() -> None:
    client = _FakeS3Client()
    session = _FakeSession(client)
    store = S3ArtifactStore(
        "artifacts", region="us-east-1", endpoint_url="http://minio.local", session=session
    )
    ref = await store.put("run-1", "summary.json", b'{"ok":1}', "application/json")
    assert ref == "s3://artifacts/run-1/summary.json"
    assert client.puts[0]["ContentType"] == "application/json"
    assert session.kwargs == {
        "service": "s3",
        "region_name": "us-east-1",
        "endpoint_url": "http://minio.local",
    }
    assert await store.get(ref) == b'{"ok":1}'


async def test_get_parses_foreign_refs() -> None:
    client = _FakeS3Client()
    client.objects[("other-bucket", "deep/key/name.bin")] = b"\x00\x01"
    store = S3ArtifactStore("unused", session=_FakeSession(client))
    assert await store.get("s3://other-bucket/deep/key/name.bin") == b"\x00\x01"
