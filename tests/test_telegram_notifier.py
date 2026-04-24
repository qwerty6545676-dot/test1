"""TelegramNotifier: bus -> cooldown -> tier -> client queue."""

from __future__ import annotations

import pytest

from arbitrage.settings import (
    Fees,
    FilterSet,
    Limits,
    MarketFilters,
    Settings,
    Symbols,
    TelegramConfig,
    TierFilter,
)
from arbitrage.signals import ArbSignal, InfoEvent, SignalBus
from arbitrage.telegram_notify.notifier import TelegramNotifier


class _FakeClient:
    """Minimal stand-in for ``TelegramClient`` — captures enqueues."""

    def __init__(self) -> None:
        self.calls: list[tuple[int, int | None, str]] = []

    def enqueue(self, chat_id: int, topic_id: int | None, text: str) -> bool:
        self.calls.append((chat_id, topic_id, text))
        return True


def _settings(
    *,
    spot_chat: int | None = -100,
    perp_chat: int | None = -200,
    spot_cooldown: int = 10,
    perp_cooldown: int = 10,
) -> Settings:
    spot = MarketFilters(
        min_profit_pct=3.0,
        cooldown_seconds=spot_cooldown,
        info_topic_id=900,
        tiers={
            "low": TierFilter(from_pct=3.0, to_pct=5.0, topic_id=911),
            "mid": TierFilter(from_pct=5.0, to_pct=10.0, topic_id=912),
            "high": TierFilter(from_pct=10.0, to_pct=999.0, topic_id=913),
        },
    )
    perp = MarketFilters(
        min_profit_pct=0.5,
        cooldown_seconds=perp_cooldown,
        info_topic_id=800,
        tiers={
            "low": TierFilter(from_pct=0.5, to_pct=1.0, topic_id=811),
            "mid": TierFilter(from_pct=1.0, to_pct=3.0, topic_id=812),
            "high": TierFilter(from_pct=3.0, to_pct=999.0, topic_id=813),
        },
    )
    return Settings(
        symbols=Symbols(spot=["BTCUSDT"], perp=["BTCUSDT"]),
        filters=FilterSet(spot=spot, perp=perp),
        fees=Fees(spot={"a": 0.001}, perp={"a": 0.0004}),
        limits=Limits(),
        telegram=TelegramConfig(
            enabled=True,
            spot_chat_id=spot_chat,
            perp_chat_id=perp_chat,
        ),
    )


def _arb(market: str, net: float, *, buy="binance", sell="bybit") -> ArbSignal:
    return ArbSignal(
        ts_ms=0,
        market=market,  # type: ignore[arg-type]
        symbol="BTCUSDT",
        buy_ex=buy,
        sell_ex=sell,
        buy_ask=100.0,
        sell_bid=101.0,
        net_pct=net,
    )


def test_spot_arb_routes_to_mid_topic():
    s = _settings()
    bus = SignalBus()
    client = _FakeClient()
    TelegramNotifier(s, client, bus).attach()

    bus.emit_arb(_arb("spot", 7.0))

    assert len(client.calls) == 1
    chat_id, topic_id, _text = client.calls[0]
    assert chat_id == -100  # spot group
    assert topic_id == 912  # mid tier


def test_perp_arb_routes_to_low_topic():
    s = _settings()
    bus = SignalBus()
    client = _FakeClient()
    TelegramNotifier(s, client, bus).attach()

    bus.emit_arb(_arb("perp", 0.8))

    assert len(client.calls) == 1
    chat_id, topic_id, _text = client.calls[0]
    assert chat_id == -200  # perp group
    assert topic_id == 811  # low tier


def test_arb_cooldown_suppresses_duplicates():
    s = _settings(spot_cooldown=60)
    bus = SignalBus()
    client = _FakeClient()
    TelegramNotifier(s, client, bus).attach()

    bus.emit_arb(_arb("spot", 7.0))
    bus.emit_arb(_arb("spot", 7.1))  # same (market, symbol, buy, sell) key
    assert len(client.calls) == 1


def test_arb_different_pair_bypasses_cooldown():
    s = _settings()
    bus = SignalBus()
    client = _FakeClient()
    TelegramNotifier(s, client, bus).attach()

    bus.emit_arb(_arb("spot", 7.0, buy="binance", sell="bybit"))
    bus.emit_arb(_arb("spot", 7.0, buy="bitget", sell="mexc"))
    assert len(client.calls) == 2


def test_arb_below_tier_is_silenced():
    s = _settings()
    bus = SignalBus()
    client = _FakeClient()
    TelegramNotifier(s, client, bus).attach()

    # 2.0% is below spot's lowest tier (3.0%).
    bus.emit_arb(_arb("spot", 2.0))
    assert client.calls == []


def test_arb_dropped_when_chat_not_configured():
    s = _settings(spot_chat=None)
    bus = SignalBus()
    client = _FakeClient()
    TelegramNotifier(s, client, bus).attach()

    bus.emit_arb(_arb("spot", 7.0))
    assert client.calls == []


def test_market_scoped_info_goes_to_that_group_only():
    s = _settings()
    bus = SignalBus()
    client = _FakeClient()
    TelegramNotifier(s, client, bus).attach()

    bus.emit_info(InfoEvent(ts_ms=0, kind="silence", message="bybit: quiet", market="spot"))
    assert len(client.calls) == 1
    chat_id, topic_id, _text = client.calls[0]
    assert chat_id == -100
    assert topic_id == 900  # spot info topic


def test_global_info_fanouts_to_both_groups():
    s = _settings()
    bus = SignalBus()
    client = _FakeClient()
    TelegramNotifier(s, client, bus).attach()

    bus.emit_info(InfoEvent(ts_ms=0, kind="startup", message="up", market=None))
    assert len(client.calls) == 2
    chats = {c[0] for c in client.calls}
    assert chats == {-100, -200}


def test_global_info_skips_unconfigured_chat():
    s = _settings(perp_chat=None)
    bus = SignalBus()
    client = _FakeClient()
    TelegramNotifier(s, client, bus).attach()

    bus.emit_info(InfoEvent(ts_ms=0, kind="startup", message="up", market=None))
    assert len(client.calls) == 1
    assert client.calls[0][0] == -100


def test_attach_is_idempotent():
    s = _settings()
    bus = SignalBus()
    client = _FakeClient()
    notifier = TelegramNotifier(s, client, bus)
    notifier.attach()
    notifier.attach()  # second attach must not register twice

    bus.emit_arb(_arb("spot", 7.0))
    assert len(client.calls) == 1
