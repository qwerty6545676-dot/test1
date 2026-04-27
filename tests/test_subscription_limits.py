"""Subscribe-message batching and connection sharding.

These two listeners are the only ones in the project that can't take
``len(symbols) ~= 100`` on a single WS in a single sub message:

* **Bybit spot** caps a single ``op:subscribe`` request at 10 args
  (server replies ``args size > 10`` and silently drops the request
  if exceeded). The listener splits its symbol list into ≤10 chunks.

* **MEXC** (spot + perp) caps streams per connection at 30. The
  ``run_mexc`` wrapper therefore shards across multiple parallel WS
  connections at 25 streams each (leaves headroom for sub-acks).

The other five venues (Binance, Gate.io, Bitget, KuCoin, BingX)
documented limits all comfortably exceed 100, so we don't bother
testing them.
"""

from __future__ import annotations

from arbitrage.exchanges.spot import bybit as bybit_spot
from arbitrage.exchanges.spot import mexc as mexc_spot
from arbitrage.exchanges.perp import mexc as mexc_perp


def test_bybit_chunked_helper_groups_by_n() -> None:
    args = [f"orderbook.1.SYM{i}" for i in range(23)]
    chunks = bybit_spot._chunked(args, 10)
    assert [len(c) for c in chunks] == [10, 10, 3]
    # Concatenating the chunks must round-trip the input exactly.
    flat = [a for c in chunks for a in c]
    assert flat == args


def test_bybit_chunked_helper_smaller_than_chunk_size() -> None:
    # Below the chunk size ⇒ one chunk that's the whole input.
    args = ["orderbook.1.A", "orderbook.1.B"]
    assert bybit_spot._chunked(args, 10) == [args]


def test_bybit_subscribe_chunks_at_ten_args(monkeypatch) -> None:
    """Subscribing to >10 symbols must produce >1 sub frame."""
    sent: list[bytes] = []

    class FakeTransport:
        def send(self, _msg_type, payload: bytes) -> None:
            sent.append(payload)

    listener = bybit_spot.BybitListener(
        prices={},
        symbols=tuple(f"SYM{i:02d}USDT" for i in range(23)),
    )
    # Avoid spawning the ping task during this unit test.
    monkeypatch.setattr(
        "arbitrage.exchanges.spot.bybit.asyncio.create_task",
        lambda coro: coro.close() or None,  # type: ignore[func-returns-value]
    )
    listener.on_ws_connected(FakeTransport())  # type: ignore[arg-type]
    # 23 symbols ⇒ ceil(23/10) = 3 sub frames.
    assert len(sent) == 3
    # Each frame's args list must be ≤ 10.
    import json
    decoded = [json.loads(p) for p in sent]
    for frame in decoded:
        assert frame["op"] == "subscribe"
        assert len(frame["args"]) <= 10
    # Concatenated args round-trip the symbol set.
    all_args = [a for frame in decoded for a in frame["args"]]
    assert sorted(all_args) == sorted(
        f"orderbook.1.SYM{i:02d}USDT" for i in range(23)
    )


def test_mexc_spot_chunk_helper() -> None:
    syms = tuple(f"SYM{i}USDT" for i in range(60))
    chunks = mexc_spot._chunk(syms, 25)
    assert [len(c) for c in chunks] == [25, 25, 10]
    flat = tuple(s for c in chunks for s in c)
    assert flat == syms


def test_mexc_spot_below_shard_limit_runs_single_connection(monkeypatch) -> None:
    """≤25 symbols → no sharding → single ``_run_one`` invocation."""
    calls: list[tuple[tuple[str, ...], int]] = []

    async def fake_run_one(prices, symbols, shard):  # noqa: ARG001
        calls.append((symbols, shard))

    monkeypatch.setattr(mexc_spot, "_run_one", fake_run_one)
    import asyncio
    asyncio.run(mexc_spot.run_mexc({}, tuple(f"SYM{i}USDT" for i in range(20))))
    assert len(calls) == 1
    assert calls[0][1] == 0


def test_mexc_spot_above_shard_limit_fans_out(monkeypatch) -> None:
    """100 symbols → ceil(100/25) = 4 connections."""
    calls: list[tuple[tuple[str, ...], int]] = []

    async def fake_run_one(prices, symbols, shard):  # noqa: ARG001
        calls.append((symbols, shard))

    monkeypatch.setattr(mexc_spot, "_run_one", fake_run_one)
    import asyncio
    asyncio.run(
        mexc_spot.run_mexc({}, tuple(f"SYM{i:03d}USDT" for i in range(100)))
    )
    assert len(calls) == 4
    # Shard ids are unique 0..3 — important so log lines stay
    # disambiguated when reading mexc-perp[shard=2] crashes.
    assert sorted(c[1] for c in calls) == [0, 1, 2, 3]
    # Union of all shards' symbols equals the input.
    flat = tuple(s for sym_tuple, _ in calls for s in sym_tuple)
    assert sorted(flat) == sorted(f"SYM{i:03d}USDT" for i in range(100))


def test_mexc_perp_chunk_helper() -> None:
    # Same helper, on the perp module — guard against future drift
    # where one module gets refactored and the other doesn't.
    syms = tuple(f"SYM{i}USDT" for i in range(60))
    chunks = mexc_perp._chunk(syms, 25)
    assert [len(c) for c in chunks] == [25, 25, 10]


def test_mexc_perp_above_shard_limit_fans_out(monkeypatch) -> None:
    calls: list[tuple[tuple[str, ...], int]] = []

    async def fake_run_one(prices, symbols, shard):  # noqa: ARG001
        calls.append((symbols, shard))

    monkeypatch.setattr(mexc_perp, "_run_one", fake_run_one)
    import asyncio
    asyncio.run(
        mexc_perp.run_mexc({}, tuple(f"SYM{i:03d}USDT" for i in range(80)))
    )
    # ceil(80/25) = 4
    assert len(calls) == 4
