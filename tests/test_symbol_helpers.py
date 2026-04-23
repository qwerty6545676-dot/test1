import pytest

from arbitrage.exchanges._common import (
    from_native,
    split_canonical,
    to_native,
)


@pytest.mark.parametrize(
    "canonical,base,quote",
    [
        ("BTCUSDT", "BTC", "USDT"),
        ("ETHUSDT", "ETH", "USDT"),
        ("SOLUSDT", "SOL", "USDT"),
        ("ETHBTC", "ETH", "BTC"),
        ("BNBUSDC", "BNB", "USDC"),
    ],
)
def test_split_canonical(canonical, base, quote):
    assert split_canonical(canonical) == (base, quote)


def test_split_canonical_unknown_quote():
    with pytest.raises(ValueError):
        split_canonical("DOGEXYZ")


@pytest.mark.parametrize(
    "canonical,sep,native",
    [
        ("BTCUSDT", "_", "BTC_USDT"),
        ("BTCUSDT", "-", "BTC-USDT"),
        ("ETHUSDT", "_", "ETH_USDT"),
        ("BTCUSDT", "", "BTCUSDT"),  # empty separator -> no-op
    ],
)
def test_to_native(canonical, sep, native):
    assert to_native(canonical, sep) == native


@pytest.mark.parametrize(
    "native,sep,canonical",
    [
        ("BTC_USDT", "_", "BTCUSDT"),
        ("BTC-USDT", "-", "BTCUSDT"),
        ("BTCUSDT", "", "BTCUSDT"),
    ],
)
def test_from_native(native, sep, canonical):
    assert from_native(native, sep) == canonical
