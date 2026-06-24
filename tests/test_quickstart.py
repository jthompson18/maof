"""The quickstart example runs end-to-end offline and returns the expected result."""

from __future__ import annotations

from examples.quickstart.main import run


async def test_quickstart_governed_run() -> None:
    output = await run()
    assert output == {"greeting": "Hello, world!"}
