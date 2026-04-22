from arbitrage.exchanges.bybit import _merge_side


def test_snapshot_populates_both_sides():
    state: dict[str, tuple[float, float]] = {}
    _merge_side(state, [["100.5", "1.0"]], "b")
    _merge_side(state, [["101.0", "2.0"]], "a")
    assert state == {"b": (100.5, 1.0), "a": (101.0, 2.0)}


def test_delta_updates_only_affected_side():
    state = {"b": (100.5, 1.0), "a": (101.0, 2.0)}
    _merge_side(state, [["100.6", "0.5"]], "b")
    assert state == {"b": (100.6, 0.5), "a": (101.0, 2.0)}


def test_delta_with_qty_zero_deletes_level():
    state = {"b": (100.5, 1.0), "a": (101.0, 2.0)}
    _merge_side(state, [["100.5", "0"]], "b")
    assert state == {"a": (101.0, 2.0)}


def test_missing_side_is_noop():
    state = {"b": (100.5, 1.0), "a": (101.0, 2.0)}
    _merge_side(state, None, "b")
    assert state == {"b": (100.5, 1.0), "a": (101.0, 2.0)}


def test_empty_side_is_noop():
    state = {"b": (100.5, 1.0), "a": (101.0, 2.0)}
    _merge_side(state, [], "b")
    assert state == {"b": (100.5, 1.0), "a": (101.0, 2.0)}


def test_malformed_level_returns_false():
    state: dict[str, tuple[float, float]] = {}
    # too short
    assert _merge_side(state, [["100.5"]], "b") is False
    # non-numeric
    assert _merge_side(state, [["nope", "1"]], "b") is False
    # state untouched
    assert state == {}
