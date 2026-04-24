"""Logging → SignalBus bridge.

Any ``logging.ERROR`` record emitted anywhere in the app (exchange
listeners, background tasks, ``logger.exception(...)`` calls) is
turned into an :class:`~arbitrage.signals.InfoEvent` with
``kind="error"`` and routed through the bus. The Telegram notifier
then sends it to the info-topic of every configured group.

Two loops we explicitly avoid:

* Records from ``arbitrage.telegram.*`` are dropped, so an error from
  the Telegram client itself does NOT try to send another Telegram
  message.
* Records from ``arbitrage.signals`` are dropped for the same reason
  — a handler that raises would log via that logger.
"""

from __future__ import annotations

import logging
import time

from ..signals import InfoEvent, SignalBus


_SKIP_PREFIXES = (
    "arbitrage.telegram",
    "arbitrage.signals",
)


class BusErrorHandler(logging.Handler):
    """Forward ERROR log records to the SignalBus as InfoEvents."""

    def __init__(self, bus: SignalBus) -> None:
        super().__init__(level=logging.ERROR)
        self._bus = bus

    def emit(self, record: logging.LogRecord) -> None:
        # Handler.handle() doesn't re-check the level (that's normally
        # the logger's job), so do it here ourselves — makes the
        # handler safe to call directly.
        if record.levelno < self.level:
            return
        if record.name.startswith(_SKIP_PREFIXES):
            return
        try:
            msg = record.getMessage()
        except Exception:
            self.handleError(record)
            return
        if record.exc_info:
            # Include the exception class name so operators know what
            # they're looking at without having to open the log file.
            exc_type = record.exc_info[0]
            if exc_type is not None:
                msg = f"{exc_type.__name__}: {msg}"
        self._bus.emit_info(
            InfoEvent(
                ts_ms=int(time.time() * 1000),
                kind="error",
                message=f"{record.name}: {msg[:500]}",
                market=None,
                severity="error",
            )
        )
