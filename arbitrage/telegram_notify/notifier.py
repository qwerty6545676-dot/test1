"""Glue between the signal bus and the Telegram HTTP client.

Responsibilities:

* Subscribe to :class:`~arbitrage.signals.SignalBus`.
* For arb signals: apply cooldown, route to the tier's topic, enqueue
  to the client.
* For info events: enqueue to the appropriate market's info-topic
  (or both, when ``event.market is None``).

The notifier owns no threading primitives of its own; the client does
the queueing. Registering handlers with the bus is synchronous, but
the handlers themselves only enqueue (never ``await``) so they're
safe to call from the hot path.
"""

from __future__ import annotations

import logging

from ..cooldown import CooldownTracker
from ..settings import Settings
from ..signals import ArbSignal, InfoEvent, SignalBus
from ..tier_router import route_tier
from .client import TelegramClient
from .format import format_arb, format_info

logger = logging.getLogger("arbitrage.telegram.notifier")


class TelegramNotifier:
    """Wire a :class:`SignalBus` to a :class:`TelegramClient`."""

    __slots__ = (
        "_settings",
        "_client",
        "_bus",
        "_cd_spot",
        "_cd_perp",
        "_attached",
    )

    def __init__(
        self,
        settings: Settings,
        client: TelegramClient,
        bus: SignalBus,
    ) -> None:
        self._settings = settings
        self._client = client
        self._bus = bus
        self._cd_spot = CooldownTracker(settings.filters.spot.cooldown_seconds)
        self._cd_perp = CooldownTracker(settings.filters.perp.cooldown_seconds)
        self._attached = False

    def attach(self) -> None:
        if self._attached:
            return
        self._bus.register_arb(self._on_arb)
        self._bus.register_info(self._on_info)
        self._attached = True

    # ------------------------------------------------------------------
    # Arb signals

    def _on_arb(self, sig: ArbSignal) -> None:
        tracker = self._cd_spot if sig.market == "spot" else self._cd_perp
        if not tracker.should_emit(sig.key):
            return

        chat_id = self._chat_for_market(sig.market)
        if chat_id is None:
            # Telegram disabled / not configured for this market — no-op.
            return

        routed = route_tier(self._settings, sig.market, sig.net_pct)
        if routed is None:
            # Spread fell into a gap between configured tiers — silenced.
            return
        _tier_name, topic_id = routed

        self._client.enqueue(chat_id, topic_id, format_arb(sig))

    # ------------------------------------------------------------------
    # Info events

    def _on_info(self, ev: InfoEvent) -> None:
        text = format_info(ev)
        if ev.market is None:
            # Broadcast to both groups (if both are configured).
            self._enqueue_info_for("spot", text)
            self._enqueue_info_for("perp", text)
        else:
            self._enqueue_info_for(ev.market, text)

    def _enqueue_info_for(self, market: str, text: str) -> None:
        chat_id = self._chat_for_market(market)
        if chat_id is None:
            return
        mf = (
            self._settings.filters.spot
            if market == "spot"
            else self._settings.filters.perp
        )
        self._client.enqueue(chat_id, mf.info_topic_id, text)

    # ------------------------------------------------------------------
    # Shared helpers

    def _chat_for_market(self, market: str) -> int | None:
        if market == "spot":
            return self._settings.telegram.spot_chat_id
        if market == "perp":
            return self._settings.telegram.perp_chat_id
        return None
