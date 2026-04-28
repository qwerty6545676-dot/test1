"""Tests for arbitrage.settings YAML loader and validation."""

from __future__ import annotations

import textwrap
from pathlib import Path

import msgspec
import pytest

from arbitrage import settings as settings_mod


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_VALID_YAML = """
symbols:
  spot: [BTCUSDT, ETHUSDT]
  perp: [BTCUSDT]

filters:
  spot:
    min_profit_pct: 3.0
    cooldown_seconds: 180
    info_topic_id: 104
    tiers:
      low:  { from_pct: 3.0,  to_pct: 5.0,   topic_id: 101 }
      mid:  { from_pct: 5.0,  to_pct: 10.0,  topic_id: 102 }
      high: { from_pct: 10.0, to_pct: 999.0, topic_id: 103 }
  perp:
    min_profit_pct: 0.5
    cooldown_seconds: 180
    info_topic_id: 204
    tiers:
      low:  { from_pct: 0.5, to_pct: 1.0,   topic_id: 201 }
      mid:  { from_pct: 1.0, to_pct: 3.0,   topic_id: 202 }
      high: { from_pct: 3.0, to_pct: 999.0, topic_id: 203 }

fees:
  spot:
    binance: 0.001
    bybit:   0.001
  perp:
    binance: 0.0004
    bybit:   0.00055

limits:
  max_age_ms: 500
  heartbeat_interval_ms: 1000
  heartbeat_timeout_ms: 3000
  backoff_max_s: 60.0
  backoff_jitter_s: 1.0

telegram:
  enabled: true
  spot_chat_id: -1001234567890
  perp_chat_id: -1009876543210
"""


def _write(tmp: Path, body: str) -> Path:
    p = tmp / "settings.yaml"
    p.write_text(textwrap.dedent(body))
    return p


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------


def test_loads_valid_yaml(tmp_path: Path):
    p = _write(tmp_path, _VALID_YAML)
    s = settings_mod.load_settings(p)

    assert s.symbols.spot == ["BTCUSDT", "ETHUSDT"]
    assert s.symbols.perp == ["BTCUSDT"]
    assert s.filters.spot.min_profit_pct == 3.0
    assert s.filters.spot.cooldown_seconds == 180
    assert s.filters.perp.tiers["high"].from_pct == 3.0
    assert s.fees.spot["binance"] == 0.001
    assert s.fees.perp["bybit"] == 0.00055
    assert s.limits.max_age_ms == 500
    assert s.telegram.enabled is True
    assert s.telegram.spot_chat_id == -1001234567890


def test_example_yaml_is_loadable():
    """The committed settings.example.yaml must always parse cleanly."""
    # Resolve from the repo root regardless of where pytest is invoked.
    repo_root = Path(__file__).resolve().parent.parent
    p = repo_root / "settings.example.yaml"
    assert p.exists(), "settings.example.yaml must be committed"
    s = settings_mod.load_settings(p)
    # Sanity: both markets configured, all 7 exchanges in fees.
    assert len(s.symbols.spot) > 0
    assert len(s.symbols.perp) > 0
    expected_exchanges = {"binance", "bybit", "gateio", "bitget",
                          "kucoin", "bingx", "mexc"}
    assert set(s.fees.spot.keys()) == expected_exchanges
    assert set(s.fees.perp.keys()) == expected_exchanges


def test_defaults_for_optional_sections(tmp_path: Path):
    """limits / telegram omitted -> defaults fill in."""
    yaml = """
    symbols:
      spot: [BTCUSDT]
      perp: [BTCUSDT]
    filters:
      spot:
        min_profit_pct: 1.0
        cooldown_seconds: 60
        tiers:
          low: { from_pct: 1.0, to_pct: 999.0 }
      perp:
        min_profit_pct: 1.0
        cooldown_seconds: 60
        tiers:
          low: { from_pct: 1.0, to_pct: 999.0 }
    fees:
      spot: { binance: 0.001 }
      perp: { binance: 0.0004 }
    """
    p = _write(tmp_path, yaml)
    s = settings_mod.load_settings(p)
    assert s.limits.max_age_ms == 500          # default
    assert s.telegram.enabled is False         # default
    assert s.telegram.spot_chat_id is None     # default
    assert s.filters.spot.tiers["low"].topic_id is None  # TierFilter default


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def test_rejects_negative_min_profit(tmp_path: Path):
    bad = _VALID_YAML.replace("min_profit_pct: 3.0", "min_profit_pct: -1.0", 1)
    p = _write(tmp_path, bad)
    with pytest.raises(ValueError, match="min_profit_pct"):
        settings_mod.load_settings(p)


def test_rejects_inverted_tier_band(tmp_path: Path):
    bad = _VALID_YAML.replace(
        "low:  { from_pct: 3.0,  to_pct: 5.0,   topic_id: 101 }",
        "low:  { from_pct: 5.0,  to_pct: 3.0,   topic_id: 101 }",
        1,
    )
    p = _write(tmp_path, bad)
    with pytest.raises(ValueError, match="from_pct"):
        settings_mod.load_settings(p)


def test_rejects_zero_max_age(tmp_path: Path):
    bad = _VALID_YAML.replace("max_age_ms: 500", "max_age_ms: 0", 1)
    p = _write(tmp_path, bad)
    with pytest.raises(ValueError, match="max_age_ms"):
        settings_mod.load_settings(p)


def test_rejects_unknown_field(tmp_path: Path):
    bad = _VALID_YAML + "\nweird_extra_key: oops\n"
    p = _write(tmp_path, bad)
    with pytest.raises(msgspec.ValidationError):
        settings_mod.load_settings(p)


# ---------------------------------------------------------------------------
# Coverage map
# ---------------------------------------------------------------------------


def test_coverage_defaults_empty(tmp_path: Path):
    """Without a ``coverage:`` section, both maps are empty — that's
    the legacy fallback path: every venue subscribes to every symbol.
    """
    p = _write(tmp_path, _VALID_YAML)
    s = settings_mod.load_settings(p)
    assert s.coverage.spot == {}
    assert s.coverage.perp == {}


def test_coverage_loads_per_symbol_venue_lists(tmp_path: Path):
    yaml = _VALID_YAML + """
coverage:
  spot:
    BTCUSDT: [binance, bybit, mexc]
    ETHUSDT: [binance, bybit]
  perp:
    BTCUSDT: [binance-perp, bybit-perp, kucoin-perp]
"""
    p = _write(tmp_path, yaml)
    s = settings_mod.load_settings(p)
    assert s.coverage.spot["BTCUSDT"] == ["binance", "bybit", "mexc"]
    assert s.coverage.spot["ETHUSDT"] == ["binance", "bybit"]
    assert s.coverage.perp["BTCUSDT"] == ["binance-perp", "bybit-perp", "kucoin-perp"]


def test_coverage_rejects_single_venue_entry(tmp_path: Path):
    """Arbitrage requires >= 2 venues; a 1-venue entry is a useless
    subscription and probably a discovery bug — fail loudly."""
    yaml = _VALID_YAML + """
coverage:
  spot:
    BTCUSDT: [binance]
"""
    p = _write(tmp_path, yaml)
    with pytest.raises(ValueError, match=r"needs >= 2 venues"):
        settings_mod.load_settings(p)


def test_coverage_rejects_unknown_venue(tmp_path: Path):
    """Typos like ``binance-spot`` or wrong-market suffix must fail."""
    yaml = _VALID_YAML + """
coverage:
  spot:
    BTCUSDT: [binance, binance-perp]
"""
    p = _write(tmp_path, yaml)
    with pytest.raises(ValueError, match=r"unknown venue"):
        settings_mod.load_settings(p)


def test_coverage_perp_rejects_bare_venue_name(tmp_path: Path):
    """Perp entries must use the ``-perp`` suffix to match the
    ``Tick.exchange`` convention used by the listeners."""
    yaml = _VALID_YAML + """
coverage:
  perp:
    BTCUSDT: [binance, bybit]
"""
    p = _write(tmp_path, yaml)
    with pytest.raises(ValueError, match=r"unknown venue"):
        settings_mod.load_settings(p)


def test_rejects_missing_required_section(tmp_path: Path):
    yaml = """
    # Missing `fees`.
    symbols:
      spot: [BTCUSDT]
      perp: [BTCUSDT]
    filters:
      spot:
        min_profit_pct: 1.0
        cooldown_seconds: 60
        tiers: { low: { from_pct: 1.0, to_pct: 999.0 } }
      perp:
        min_profit_pct: 1.0
        cooldown_seconds: 60
        tiers: { low: { from_pct: 1.0, to_pct: 999.0 } }
    """
    p = _write(tmp_path, yaml)
    with pytest.raises(msgspec.ValidationError):
        settings_mod.load_settings(p)


# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------


def test_respects_arb_settings_path_env(tmp_path: Path, monkeypatch):
    p = _write(tmp_path, _VALID_YAML)
    monkeypatch.setenv("ARB_SETTINGS_PATH", str(p))
    # Bypass cache
    s = settings_mod.load_settings()
    assert s.filters.spot.min_profit_pct == 3.0


def test_missing_file_errors_cleanly(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("ARB_SETTINGS_PATH", str(tmp_path / "nope.yaml"))
    with pytest.raises(FileNotFoundError):
        settings_mod.load_settings()


# ---------------------------------------------------------------------------
# Telegram token (from env)
# ---------------------------------------------------------------------------


def test_telegram_token_from_env(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123:abc")
    assert settings_mod.get_telegram_bot_token() == "123:abc"


def test_telegram_token_missing_returns_none(monkeypatch):
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    assert settings_mod.get_telegram_bot_token() is None


def test_telegram_token_whitespace_treated_as_empty(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "   \n")
    assert settings_mod.get_telegram_bot_token() is None


# ---------------------------------------------------------------------------
# Backwards-compatible config module
# ---------------------------------------------------------------------------


def test_config_module_populated_from_settings():
    """config.py constants must reflect settings.example.yaml values."""
    from arbitrage import config

    # Spot universe surfaces on the legacy constants.
    assert isinstance(config.SYMBOLS, tuple)
    assert len(config.SYMBOLS) > 0
    # All 7 exchanges in the spot fees mapping.
    assert {"binance", "bybit", "gateio", "bitget",
            "kucoin", "bingx", "mexc"} <= set(config.FEES.keys())
    # Scalars have sane ranges.
    assert config.MAX_AGE_MS > 0
    assert config.HEARTBEAT_TIMEOUT_MS > 0
    assert config.HEARTBEAT_INTERVAL_MS > 0
    assert config.BACKOFF_MAX_S > 0
