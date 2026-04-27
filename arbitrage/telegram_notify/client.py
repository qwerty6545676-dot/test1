"""aiohttp-based Telegram Bot API client with a bounded send queue.

The hot path (listener → comparator → notifier) must never block on
network I/O, so all sends go through a bounded ``asyncio.Queue`` and
a single worker task that drains it. If the queue overflows we log
an ERROR and drop the message rather than apply back-pressure to the
hot path.

Retry policy:
* **HTTP 429** — respect ``parameters.retry_after`` from the body
  (Telegram's official rate-limit signal).
* **HTTP 5xx** / network / timeout — exponential backoff up to a
  small cap, max 5 attempts, then drop.
* **HTTP 4xx (non-429)** — permanent (bad chat id, bad parse mode);
  log and drop immediately.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import aiohttp

from .. import metrics

logger = logging.getLogger("arbitrage.telegram.client")


class TelegramClient:
    """Bot API sender with retry + queue.

    Concurrency model: exactly one worker task drains the queue in
    FIFO order, so message ordering is preserved and rate-limit state
    is simple to reason about.
    """

    __slots__ = (
        "_token",
        "_base_url",
        "_queue",
        "_worker_task",
        "_session",
        "_send_timeout",
        "_max_attempts",
        "_max_backoff_s",
    )

    def __init__(
        self,
        token: str,
        *,
        base_url: str = "https://api.telegram.org",
        queue_size: int = 1024,
        send_timeout_s: float = 10.0,
        max_attempts: int = 5,
        max_backoff_s: float = 30.0,
    ) -> None:
        if not token:
            raise ValueError("TelegramClient: empty bot token")
        self._token = token
        self._base_url = base_url.rstrip("/")
        self._queue: asyncio.Queue[tuple[int, int | None, str]] = asyncio.Queue(
            maxsize=queue_size
        )
        self._worker_task: asyncio.Task[None] | None = None
        self._session: aiohttp.ClientSession | None = None
        self._send_timeout = aiohttp.ClientTimeout(total=send_timeout_s)
        self._max_attempts = max_attempts
        self._max_backoff_s = max_backoff_s

    # ------------------------------------------------------------------
    # Lifecycle

    async def start(self) -> None:
        if self._worker_task is not None:
            return
        self._session = aiohttp.ClientSession()
        self._worker_task = asyncio.create_task(self._worker(), name="telegram-worker")

    async def stop(self) -> None:
        task = self._worker_task
        session = self._session
        self._worker_task = None
        self._session = None

        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                # asyncio.CancelledError is expected; any other exception
                # from the worker is already logged, we just want to
                # release resources either way.
                pass
        if session is not None:
            await session.close()

    # ------------------------------------------------------------------
    # Producer API

    def enqueue(self, chat_id: int, topic_id: int | None, text: str) -> bool:
        """Non-blocking. Returns False if dropped (queue full)."""
        try:
            self._queue.put_nowait((chat_id, topic_id, text))
            metrics.set_telegram_queue_size(self._queue.qsize())
            return True
        except asyncio.QueueFull:
            logger.error(
                "telegram: queue full (size=%d), dropping message to chat=%d",
                self._queue.maxsize,
                chat_id,
            )
            metrics.record_telegram_dropped()
            return False

    def qsize(self) -> int:
        return self._queue.qsize()

    # ------------------------------------------------------------------
    # Worker

    async def _worker(self) -> None:
        # `get()` blocks until an item arrives; cancellation unwinds cleanly.
        while True:
            try:
                chat_id, topic_id, text = await self._queue.get()
                metrics.set_telegram_queue_size(self._queue.qsize())
            except asyncio.CancelledError:
                return
            try:
                await self._send(chat_id, topic_id, text)
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("telegram: unhandled send error, dropping")
            finally:
                self._queue.task_done()

    # ------------------------------------------------------------------
    # Single request with retry

    async def _send(self, chat_id: int, topic_id: int | None, text: str) -> None:
        session = self._session
        if session is None:
            logger.error("telegram: send called before start()")
            return

        url = f"{self._base_url}/bot{self._token}/sendMessage"
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if topic_id is not None:
            payload["message_thread_id"] = topic_id

        attempt = 0
        while True:
            try:
                async with session.post(
                    url, json=payload, timeout=self._send_timeout
                ) as resp:
                    if resp.status == 200:
                        return

                    if resp.status == 429:
                        retry_after = await _parse_retry_after(resp)
                        logger.warning(
                            "telegram: 429 from api, sleeping retry_after=%.1fs",
                            retry_after,
                        )
                        metrics.record_telegram_retry("429")
                        await asyncio.sleep(retry_after)
                        # 429 doesn't count as a "failed attempt" — keep looping.
                        continue

                    if 500 <= resp.status < 600:
                        attempt += 1
                        metrics.record_telegram_retry("5xx")
                        if attempt >= self._max_attempts:
                            body = (await resp.text())[:200]
                            logger.error(
                                "telegram: %d giving up after %d attempts: %s",
                                resp.status,
                                attempt,
                                body,
                            )
                            metrics.record_telegram_dropped()
                            return
                        backoff = min(2.0 ** (attempt - 1), self._max_backoff_s)
                        logger.warning(
                            "telegram: %d, backoff=%.1fs (attempt %d/%d)",
                            resp.status,
                            backoff,
                            attempt,
                            self._max_attempts,
                        )
                        await asyncio.sleep(backoff)
                        continue

                    # 4xx other than 429 — permanent client error.
                    body = (await resp.text())[:200]
                    logger.error(
                        "telegram: %d (won't retry) payload=%s response=%s",
                        resp.status,
                        {"chat_id": chat_id, "topic": topic_id},
                        body,
                    )
                    metrics.record_telegram_dropped()
                    return

            except asyncio.TimeoutError:
                attempt += 1
                metrics.record_telegram_retry("timeout")
                if attempt >= self._max_attempts:
                    logger.error(
                        "telegram: timeout giving up after %d attempts", attempt
                    )
                    metrics.record_telegram_dropped()
                    return
                backoff = min(2.0 ** (attempt - 1), self._max_backoff_s)
                logger.warning(
                    "telegram: timeout, backoff=%.1fs (attempt %d/%d)",
                    backoff,
                    attempt,
                    self._max_attempts,
                )
                await asyncio.sleep(backoff)

            except aiohttp.ClientError as exc:
                attempt += 1
                if attempt >= self._max_attempts:
                    logger.error(
                        "telegram: client error giving up after %d attempts: %s",
                        attempt,
                        exc,
                    )
                    return
                backoff = min(2.0 ** (attempt - 1), self._max_backoff_s)
                logger.warning(
                    "telegram: client error %s, backoff=%.1fs (attempt %d/%d)",
                    exc,
                    backoff,
                    attempt,
                    self._max_attempts,
                )
                await asyncio.sleep(backoff)


async def _parse_retry_after(resp: aiohttp.ClientResponse) -> float:
    """Telegram returns retry_after inside a JSON body on 429."""
    try:
        data = await resp.json(content_type=None)
    except Exception:
        return 1.0
    if isinstance(data, dict):
        params = data.get("parameters")
        if isinstance(params, dict):
            ra = params.get("retry_after")
            if isinstance(ra, (int, float)) and ra >= 0:
                return float(ra)
    return 1.0
