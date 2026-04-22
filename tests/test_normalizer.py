from arbitrage.normalizer import Tick, validate_tick


def _tick(bid: float, ask: float) -> Tick:
    return Tick(
        exchange="binance",
        symbol="BTCUSDT",
        bid=bid,
        ask=ask,
        ts_exchange=1,
        ts_local=1,
    )


def test_valid_tick():
    assert validate_tick(_tick(100.0, 100.1)) is True


def test_zero_or_negative_prices_invalid():
    assert validate_tick(_tick(0.0, 100.0)) is False
    assert validate_tick(_tick(100.0, 0.0)) is False
    assert validate_tick(_tick(-1.0, 100.0)) is False


def test_crossed_book_invalid():
    # ask < bid
    assert validate_tick(_tick(101.0, 100.0)) is False


def test_huge_spread_invalid():
    # ask / bid > 1.05 → spread > 5%
    assert validate_tick(_tick(100.0, 106.0)) is False
    # exactly 5% is still rejected (>1.05 strictly)
    assert validate_tick(_tick(100.0, 105.0)) is True


def test_tick_has_no_gc_tracking():
    # msgspec.Struct(gc=False) must not be GC-tracked
    import gc

    t = _tick(1.0, 2.0)
    assert gc.is_tracked(t) is False
