"""Route an arb signal's profit percentage to a tier topic.

The tiers are configured per-market under
``settings.filters.{market}.tiers`` — e.g. ``low`` / ``mid`` /
``high``, each with a ``[from_pct, to_pct)`` band and a Telegram
``topic_id``.

A signal whose ``net_pct`` doesn't fit into any configured band is
silenced (we return ``None``). Gaps between tiers are allowed by
design so operators can choose to only get mid+ alerts, etc.
"""

from __future__ import annotations

from .settings import MarketFilters, Settings


def route_tier(
    settings: Settings,
    market: str,
    net_pct: float,
) -> tuple[str, int | None] | None:
    """Return ``(tier_name, topic_id)`` or ``None`` if not in any tier.

    ``topic_id`` may still be ``None`` if the tier matched but the
    operator hasn't set a Telegram topic id for it yet. Callers
    should treat that as "log the signal but skip Telegram".
    """
    mf = _market_filters(settings, market)
    for name, tier in mf.tiers.items():
        if tier.from_pct <= net_pct < tier.to_pct:
            return (name, tier.topic_id)
    return None


def _market_filters(settings: Settings, market: str) -> MarketFilters:
    if market == "spot":
        return settings.filters.spot
    if market == "perp":
        return settings.filters.perp
    raise ValueError(f"unknown market: {market!r}")
