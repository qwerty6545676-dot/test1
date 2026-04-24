"""TelegramClient HTTP send + retry behaviour.

Tests point the client at a local ``aiohttp`` test server instead of
the real Bot API so we can script any response without touching the
network. Retry timings are shrunk via ``max_backoff_s`` so the full
matrix still runs in < 1 s.
"""

from __future__ import annotations

import asyncio
import json

import pytest
from aiohttp import web

from arbitrage.telegram_notify.client import TelegramClient


# ---------------------------------------------------------------------------
# Fake Bot API fixture

class _FakeBotAPI:
    """A stub ``sendMessage`` endpoint whose response is programmable."""

    def __init__(self) -> None:
        self.requests: list[dict] = []
        # List of (status, body) pairs returned in order.  Once the
        # list is drained, any further requests get 200.
        self.scripted: list[tuple[int, dict | str]] = []

    def script(self, *responses: tuple[int, dict | str]) -> None:
        self.scripted.extend(responses)

    async def handle(self, request: web.Request) -> web.Response:
        self.requests.append(await request.json())
        if self.scripted:
            status, body = self.scripted.pop(0)
        else:
            status, body = 200, {"ok": True}
        if isinstance(body, dict):
            return web.json_response(body, status=status)
        return web.Response(status=status, text=body)


@pytest.fixture
async def fake_api(aiohttp_unused_port):
    api = _FakeBotAPI()
    app = web.Application()
    app.router.add_post("/bot{token}/sendMessage", api.handle)
    runner = web.AppRunner(app)
    await runner.setup()
    port = aiohttp_unused_port()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()
    try:
        yield api, f"http://127.0.0.1:{port}"
    finally:
        await runner.cleanup()


@pytest.fixture
def aiohttp_unused_port():
    """Return a helper that picks an unused TCP port.

    Provided as a fixture instead of importing from ``aiohttp.pytest_plugin``
    because that plugin expects a ``web.Application`` per-test and ties
    into its own fixtures; we run the server manually here.
    """
    import socket

    def _find() -> int:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            return sock.getsockname()[1]

    return _find


# ---------------------------------------------------------------------------
# Tests


@pytest.mark.asyncio
async def test_send_success(fake_api):
    api, base_url = fake_api
    client = TelegramClient("TOKEN", base_url=base_url)
    await client.start()
    try:
        client.enqueue(chat_id=-100123, topic_id=5, text="hello")
        await asyncio.wait_for(client._queue.join(), timeout=2.0)
    finally:
        await client.stop()

    assert len(api.requests) == 1
    req = api.requests[0]
    assert req["chat_id"] == -100123
    assert req["text"] == "hello"
    assert req["message_thread_id"] == 5
    assert req["parse_mode"] == "HTML"


@pytest.mark.asyncio
async def test_send_without_topic(fake_api):
    api, base_url = fake_api
    client = TelegramClient("TOKEN", base_url=base_url)
    await client.start()
    try:
        client.enqueue(chat_id=42, topic_id=None, text="hi")
        await asyncio.wait_for(client._queue.join(), timeout=2.0)
    finally:
        await client.stop()

    assert len(api.requests) == 1
    assert "message_thread_id" not in api.requests[0]


@pytest.mark.asyncio
async def test_retries_on_429_respecting_retry_after(fake_api):
    api, base_url = fake_api
    # First call: 429 telling us to wait 0.05s, second call: 200.
    api.script(
        (429, {"ok": False, "parameters": {"retry_after": 0.05}}),
        (200, {"ok": True}),
    )
    client = TelegramClient("TOKEN", base_url=base_url)
    await client.start()
    try:
        client.enqueue(1, None, "x")
        await asyncio.wait_for(client._queue.join(), timeout=2.0)
    finally:
        await client.stop()

    assert len(api.requests) == 2


@pytest.mark.asyncio
async def test_retries_on_5xx_with_backoff(fake_api):
    api, base_url = fake_api
    api.script(
        (502, "bad gateway"),
        (502, "bad gateway"),
        (200, {"ok": True}),
    )
    # Crush the backoff to keep the test fast.
    client = TelegramClient("TOKEN", base_url=base_url, max_backoff_s=0.01)
    await client.start()
    try:
        client.enqueue(1, None, "x")
        await asyncio.wait_for(client._queue.join(), timeout=2.0)
    finally:
        await client.stop()

    assert len(api.requests) == 3


@pytest.mark.asyncio
async def test_gives_up_after_max_attempts_on_5xx(fake_api):
    api, base_url = fake_api
    # Permanent 500s.
    api.script(*[(500, "boom") for _ in range(10)])
    client = TelegramClient(
        "TOKEN", base_url=base_url, max_attempts=3, max_backoff_s=0.01
    )
    await client.start()
    try:
        client.enqueue(1, None, "x")
        await asyncio.wait_for(client._queue.join(), timeout=2.0)
    finally:
        await client.stop()

    assert len(api.requests) == 3


@pytest.mark.asyncio
async def test_does_not_retry_on_4xx(fake_api):
    api, base_url = fake_api
    api.script((400, {"ok": False, "description": "bad chat_id"}))
    client = TelegramClient("TOKEN", base_url=base_url, max_backoff_s=0.01)
    await client.start()
    try:
        client.enqueue(1, None, "x")
        await asyncio.wait_for(client._queue.join(), timeout=2.0)
    finally:
        await client.stop()

    assert len(api.requests) == 1


@pytest.mark.asyncio
async def test_queue_full_returns_false():
    # No need for a server: we never call start(), so nothing drains.
    client = TelegramClient("TOKEN", queue_size=1)
    assert client.enqueue(1, None, "a") is True
    assert client.enqueue(1, None, "b") is False  # dropped
    assert client.qsize() == 1


@pytest.mark.asyncio
async def test_rejects_empty_token():
    with pytest.raises(ValueError):
        TelegramClient("")


@pytest.mark.asyncio
async def test_stop_is_idempotent_before_start():
    client = TelegramClient("TOKEN")
    # Should not raise even though start() was never called.
    await client.stop()


@pytest.mark.asyncio
async def test_preserves_fifo_order(fake_api):
    api, base_url = fake_api
    client = TelegramClient("TOKEN", base_url=base_url)
    await client.start()
    try:
        for i in range(5):
            client.enqueue(1, None, f"msg-{i}")
        await asyncio.wait_for(client._queue.join(), timeout=2.0)
    finally:
        await client.stop()

    texts = [r["text"] for r in api.requests]
    assert texts == [f"msg-{i}" for i in range(5)]
