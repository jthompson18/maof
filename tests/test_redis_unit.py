"""Redis adapter logic offline — fake redis client (lists + BLMOVE semantics).

The live round trip lives in tests/local (live tier); these cover publish,
the crash-safe processing-list flow, retry headers, and dead-lettering.
"""

from __future__ import annotations

import asyncio

from maof.transport.redis import RedisStreamsBroker
from maof.transport.retry import ATTEMPT_HEADER
from maof.transport.wire import pack, unpack
from maof.types import IncomingMessage, QueueSpec


class _FakeRedis:
    def __init__(self) -> None:
        self.lists: dict[str, list[bytes]] = {}
        self.closed = False

    async def lpush(self, name: str, raw: bytes) -> None:
        self.lists.setdefault(name, []).insert(0, raw)

    async def blmove(
        self, src: str, dst: str, timeout_s: float, from_side: str, to_side: str
    ) -> bytes | None:
        source = self.lists.get(src) or []
        if not source:
            # Yield like the real blocking call would — without this, the adapter's
            # while-True consume loop would starve the event loop in tests.
            await asyncio.sleep(min(timeout_s, 0.01) or 0.001)
            return None
        raw = source.pop()  # RIGHT
        self.lists.setdefault(dst, []).insert(0, raw)  # LEFT
        return raw

    async def lrem(self, name: str, count: int, raw: bytes) -> None:
        items = self.lists.get(name, [])
        if raw in items:
            items.remove(raw)

    async def llen(self, name: str) -> int:
        return len(self.lists.get(name, []))

    async def aclose(self) -> None:
        self.closed = True


def _broker(client: _FakeRedis) -> RedisStreamsBroker:
    return RedisStreamsBroker("redis://unused", client=client)


async def test_publish_consume_once_round_trip_and_processing_cleanup() -> None:
    client = _FakeRedis()
    broker = _broker(client)
    await broker.publish(
        "q", b"job", headers={"idempotency_key": "ik"}, message_id="ik", correlation_id="run-1"
    )
    seen: list[IncomingMessage] = []

    async def handler(msg: IncomingMessage) -> None:
        # crash-safety: while handling, the message is parked in <queue>.processing
        assert await client.llen("q.processing") == 1
        seen.append(msg)

    await broker.consume_once("q", prefetch=1, handler=handler, timeout=1)
    assert seen[0].headers["idempotency_key"] == "ik"
    assert seen[0].correlation_id == "run-1"
    assert await client.llen("q.processing") == 0  # removed after success


async def test_consume_once_empty_queue_is_a_noop() -> None:
    broker = _broker(_FakeRedis())

    async def handler(msg: IncomingMessage) -> None:  # pragma: no cover - must not run
        raise AssertionError("no message expected")

    await broker.consume_once("q", prefetch=1, handler=handler, timeout=0)


async def test_failing_handler_requeues_with_attempt_then_dead_letters() -> None:
    client = _FakeRedis()
    broker = _broker(client)
    await broker.ensure_topology([QueueSpec(name="q", dlq_name="q.dlq", retry_steps=["0s"])])
    await broker.publish("q", b"poison", headers={}, message_id="p", correlation_id=None)

    attempts: list[int] = []

    async def failing(msg: IncomingMessage) -> None:
        attempts.append(msg.attempt)
        raise RuntimeError("boom")

    await broker.consume_once("q", prefetch=1, handler=failing, timeout=1)
    requeued = client.lists["q"][0]
    _, headers, _, _ = unpack(requeued)
    assert headers[ATTEMPT_HEADER] == "2"

    await broker.consume_once("q", prefetch=1, handler=failing, timeout=1)
    assert attempts == [1, 2]
    dead = client.lists["q.dlq"][0]
    body, _, _, _ = unpack(dead)
    assert body == b"poison"


async def test_consume_loop_dispatches_until_cancelled() -> None:
    client = _FakeRedis()
    broker = _broker(client)
    await client.lpush("q", pack(b"first", {}, "m1", None))
    handled = asyncio.Event()

    async def handler(msg: IncomingMessage) -> None:
        handled.set()

    task = asyncio.create_task(broker.consume("q", prefetch=1, handler=handler))
    await asyncio.wait_for(handled.wait(), timeout=2)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_close_closes_client() -> None:
    client = _FakeRedis()
    broker = _broker(client)
    await broker.close()
    assert client.closed
