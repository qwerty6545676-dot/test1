"""Backwards-compatible module-level constants.

The real source of truth is `arbitrage.settings` (driven by
`settings.yaml`). This module re-exports a handful of flat scalars so
existing listeners, the comparator and the heartbeat task can keep
using their current imports without knowing about the nested Settings
object.

New code should import from `arbitrage.settings` directly.
"""

from __future__ import annotations

from .settings import get_settings as _get_settings


def _reload_module_constants() -> None:
    """Refresh the flat constants from the currently-loaded settings.

    Exposed mostly for tests that swap out `settings.yaml` at runtime.
    """
    s = _get_settings()
    g = globals()
    # Spot universe is the one the *existing* (spot-only) listeners,
    # comparator and heartbeat use. Perp values live on `settings.fees.perp`
    # / `settings.symbols.perp` and are read by the perp-specific code
    # once it lands.
    g["SYMBOLS"] = tuple(s.symbols.spot)
    g["MAX_AGE_MS"] = s.limits.max_age_ms
    g["MIN_PROFIT_PCT"] = s.filters.spot.min_profit_pct
    g["HEARTBEAT_TIMEOUT_MS"] = s.limits.heartbeat_timeout_ms
    g["HEARTBEAT_INTERVAL_MS"] = s.limits.heartbeat_interval_ms
    g["FEES"] = dict(s.fees.spot)
    g["BACKOFF_MAX_S"] = s.limits.backoff_max_s
    g["BACKOFF_JITTER_S"] = s.limits.backoff_jitter_s


# Populate on import so existing `from .config import SYMBOLS` works.
SYMBOLS: tuple[str, ...] = ()
MAX_AGE_MS: int = 0
MIN_PROFIT_PCT: float = 0.0
HEARTBEAT_TIMEOUT_MS: int = 0
HEARTBEAT_INTERVAL_MS: int = 0
FEES: dict[str, float] = {}
BACKOFF_MAX_S: float = 0.0
BACKOFF_JITTER_S: float = 0.0

_reload_module_constants()
