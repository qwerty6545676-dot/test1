"""In-process signal bus.

Listeners and the comparator push two kinds of events onto the bus:

* :class:`ArbSignal` — a concrete arbitrage opportunity detected by
  :mod:`arbitrage.comparator`.
* :class:`InfoEvent` — ops-level happenings (startup, shutdown,
  reconnect, silence/recovery of an exchange, ERROR-level log
  entries).

Consumers register handlers with :func:`SignalBus.register_arb` /
:func:`SignalBus.register_info`. The Telegram notifier is one such
consumer; tests can register their own to assert the right events
were emitted.

The bus is intentionally synchronous: each ``emit_*`` call runs every
registered handler on the caller's event-loop task. Handlers that do
I/O (e.g. the Telegram sender) must enqueue work and return fast, or
they will stall the hot path.
"""

from __future__ import annotations

import logging
import time
from typing import Callable, Literal

import msgspec

_logger = logging.getLogger("arbitrage.signals")

Market = Literal["spot", "perp"]
InfoKind = Literal[
    "startup",
    "shutdown",
    "error",
    "silence",
    "recovery",
    "reconnect",
    "notice",
    "watchdog",
]
Severity = Literal["info", "warn", "error"]


class ArbSignal(msgspec.Struct, frozen=True, gc=False):
    """One arbitrage opportunity, before cooldown / tier routing."""

    ts_ms: int
    market: Market
    symbol: str
    buy_ex: str
    sell_ex: str
    buy_ask: float
    sell_bid: float
    net_pct: float

    @property
    def key(self) -> tuple[str, str, str, str]:
        """Cooldown key: same tuple dedups identical re-emissions."""
        return (self.market, self.symbol, self.buy_ex, self.sell_ex)


class InfoEvent(msgspec.Struct, frozen=True, gc=False):
    """Ops-level event routed to the info-topic of one or both groups."""

    ts_ms: int
    kind: InfoKind
    message: str
    # ``None`` means "broadcast to both spot and perp info-topics".
    market: Market | None = None
    severity: Severity = "info"


ArbHandler = Callable[[ArbSignal], None]
InfoHandler = Callable[[InfoEvent], None]


class SignalBus:
    """Tiny pub/sub. Synchronous on purpose — see module docstring."""

    def __init__(self) -> None:
        self._arb_handlers: list[ArbHandler] = []
        self._info_handlers: list[InfoHandler] = []

    def register_arb(self, handler: ArbHandler) -> None:
        self._arb_handlers.append(handler)

    def register_info(self, handler: InfoHandler) -> None:
        self._info_handlers.append(handler)

    def reset(self) -> None:
        """Drop every handler. Used by tests."""
        self._arb_handlers.clear()
        self._info_handlers.clear()

    def emit_arb(self, signal: ArbSignal) -> None:
        for h in self._arb_handlers:
            try:
                h(signal)
            except Exception:
                _logger.exception("arb handler failed")

    def emit_info(self, event: InfoEvent) -> None:
        for h in self._info_handlers:
            try:
                h(event)
            except Exception:
                _logger.exception("info handler failed")


_bus: SignalBus | None = None


def get_bus() -> SignalBus:
    """Return the process-wide bus, creating it lazily."""
    global _bus
    if _bus is None:
        _bus = SignalBus()
    return _bus


def reset_bus() -> None:
    """Replace the process-wide bus with a fresh instance (for tests)."""
    global _bus
    _bus = SignalBus()


def emit_info(
    kind: InfoKind,
    message: str,
    *,
    market: Market | None = None,
    severity: Severity = "info",
) -> None:
    """Convenience wrapper used from listeners and the heartbeat task."""
    get_bus().emit_info(
        InfoEvent(
            ts_ms=int(time.time() * 1000),
            kind=kind,
            message=message,
            market=market,
            severity=severity,
        )
    )
