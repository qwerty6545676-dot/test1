"""Typed settings loaded from `settings.yaml` + env.

Philosophy
----------
* YAML holds everything non-secret: symbols, fees, filters, Telegram
  chat/topic IDs, timeouts. It is committed and diffable.
* `.env` (or the process environment) holds secrets like
  `TELEGRAM_BOT_TOKEN`. Never committed.
* The whole config is parsed into frozen msgspec Structs so a typo
  fails loudly at startup, not three days later in prod.

Resolution order for the YAML file:
  1. `ARB_SETTINGS_PATH` environment variable (if set).
  2. `settings.yaml` in the current working directory.
  3. `settings.example.yaml` in the current working directory (fallback
     so CI / fresh clones work without extra setup).

Usage
-----
    from arbitrage.settings import get_settings
    s = get_settings()
    print(s.filters.spot.min_profit_pct)

The legacy `arbitrage.config` module re-exports the most-used scalars
(`SYMBOLS`, `FEES`, `MAX_AGE_MS`, ...) computed from `get_settings()`,
so existing listeners keep working unchanged.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import ClassVar

import msgspec


class TierFilter(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """One [from_pct, to_pct) band routed to a Telegram topic."""

    from_pct: float
    to_pct: float
    topic_id: int | None = None


class MarketFilters(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """Filters for one market type (spot or perp)."""

    min_profit_pct: float
    cooldown_seconds: int
    tiers: dict[str, TierFilter]
    info_topic_id: int | None = None


class FilterSet(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    spot: MarketFilters
    perp: MarketFilters


class Symbols(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    spot: list[str]
    perp: list[str]


class Fees(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    spot: dict[str, float]
    perp: dict[str, float]


class Limits(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    max_age_ms: int = 500
    heartbeat_interval_ms: int = 1000
    heartbeat_timeout_ms: int = 3000
    backoff_max_s: float = 60.0
    backoff_jitter_s: float = 1.0


class TelegramConfig(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    enabled: bool = False
    spot_chat_id: int | None = None
    perp_chat_id: int | None = None


class PaperSpotConfig(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    notional_per_leg_usd: float = 50.0
    slippage_pct: float = 0.05


class PaperPerpConfig(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    notional_per_leg_usd: float = 50.0
    close_threshold_pct: float = 0.5
    max_hold_seconds: int = 86_400
    slippage_pct: float = 0.03
    poll_interval_s: float = 1.0


class PaperTradingConfig(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    enabled: bool = False
    open_path: str = "data/paper_open.jsonl"
    closed_path: str = "data/paper_closed.jsonl"
    spot: PaperSpotConfig = msgspec.field(default_factory=PaperSpotConfig)
    perp: PaperPerpConfig = msgspec.field(default_factory=PaperPerpConfig)


class Settings(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """Root config object. Immutable once loaded."""

    symbols: Symbols
    filters: FilterSet
    fees: Fees
    limits: Limits = msgspec.field(default_factory=Limits)
    telegram: TelegramConfig = msgspec.field(default_factory=TelegramConfig)
    paper_trading: PaperTradingConfig = msgspec.field(default_factory=PaperTradingConfig)

    # Where in the filesystem did this come from? Useful for log lines.
    _source_path: ClassVar[str] = ""


# ---------------------------------------------------------------------------


def _resolve_path(explicit: str | os.PathLike[str] | None = None) -> Path:
    """Pick which YAML file to load."""
    if explicit is not None:
        return Path(explicit)

    env_path = os.environ.get("ARB_SETTINGS_PATH")
    if env_path:
        return Path(env_path)

    # Look relative to cwd first (so users can run from anywhere), then
    # fall back to the repo root next to this module. Tests generally
    # CWD into the repo, so this works.
    for name in ("settings.yaml", "settings.example.yaml"):
        p = Path(name)
        if p.is_file():
            return p

    # Finally, try alongside the package — useful when installed.
    here = Path(__file__).resolve().parent.parent
    for name in ("settings.yaml", "settings.example.yaml"):
        p = here / name
        if p.is_file():
            return p

    raise FileNotFoundError(
        "No settings.yaml or settings.example.yaml found. "
        "Set ARB_SETTINGS_PATH or create one in the repo root."
    )


def load_settings(path: str | os.PathLike[str] | None = None) -> Settings:
    """Parse the YAML file into a `Settings` struct.

    Raises msgspec.ValidationError if the file is malformed or types
    don't match.  Raises FileNotFoundError if no config file is found.
    """
    resolved = _resolve_path(path)
    with open(resolved, "rb") as f:
        data = f.read()
    settings = msgspec.yaml.decode(data, type=Settings)
    # Cheap cross-field sanity — catches common fat-finger mistakes.
    _validate(settings)
    return settings


def _validate(s: Settings) -> None:
    """Extra validation that the type system alone can't express."""
    for name, mf in (("spot", s.filters.spot), ("perp", s.filters.perp)):
        if mf.min_profit_pct < 0:
            raise ValueError(f"filters.{name}.min_profit_pct must be >= 0")
        if mf.cooldown_seconds < 0:
            raise ValueError(f"filters.{name}.cooldown_seconds must be >= 0")
        for tier_name, tier in mf.tiers.items():
            if tier.from_pct >= tier.to_pct:
                raise ValueError(
                    f"filters.{name}.tiers.{tier_name}: "
                    f"from_pct ({tier.from_pct}) must be < to_pct ({tier.to_pct})"
                )
    if s.limits.max_age_ms <= 0:
        raise ValueError("limits.max_age_ms must be > 0")
    if s.limits.heartbeat_timeout_ms < s.limits.max_age_ms:
        # Not strictly wrong, but weird: a stale tick would be evicted
        # before the comparator's MAX_AGE_MS would even ignore it.
        # We allow it but warn via a ValueError only in the egregious
        # case (heartbeat < max_age / 5).
        if s.limits.heartbeat_timeout_ms * 5 < s.limits.max_age_ms:
            raise ValueError(
                "limits.heartbeat_timeout_ms is much smaller than max_age_ms; "
                "check your config"
            )


# ---------------------------------------------------------------------------
# Module-level cached settings.  We load on first access so importing
# this module has no side effects (handy for tests that want to mock the
# path via ARB_SETTINGS_PATH *before* first use).

_cached: Settings | None = None


def get_settings() -> Settings:
    global _cached
    if _cached is None:
        _cached = load_settings()
    return _cached


def reload_settings() -> Settings:
    """Force a reload — mostly for tests."""
    global _cached
    _cached = load_settings()
    return _cached


def get_telegram_bot_token() -> str | None:
    """Read the bot token from the process environment.

    Returns None (not an empty string) if unset so callers can
    `if token is None: ...`.
    """
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    return token or None
