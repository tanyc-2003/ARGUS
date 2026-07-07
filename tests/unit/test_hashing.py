from datetime import date, datetime

from argus.core.hashing import canonical_hash, raw_bytes_hash


def test_dict_key_order_invariance() -> None:
    assert canonical_hash({"a": 1, "b": 2}) == canonical_hash({"b": 2, "a": 1})


def test_float_representation_noise_invariance() -> None:
    assert canonical_hash({"x": 0.1 + 0.2}) == canonical_hash({"x": 0.3})
    assert canonical_hash({"x": -0.0}) == canonical_hash({"x": 0.0})


def test_real_value_change_detected() -> None:
    # sub-1e-6 noise collapses under 6dp rounding; changes at or above 1e-6 survive
    assert canonical_hash({"close": 101.23 + 1e-9}) == canonical_hash({"close": 101.23})
    assert canonical_hash({"close": 101.230001}) != canonical_hash({"close": 101.23})
    assert canonical_hash({"close": 101.23}) != canonical_hash({"close": 101.24})


def test_nested_structures() -> None:
    a = {"bars": [{"d": date(2026, 1, 5), "c": 10.0}], "src": "stooq"}
    b = {"src": "stooq", "bars": [{"c": 10.0, "d": date(2026, 1, 5)}]}
    assert canonical_hash(a) == canonical_hash(b)


def test_list_order_matters() -> None:
    assert canonical_hash([1, 2]) != canonical_hash([2, 1])


def test_datetime_and_date_distinct() -> None:
    assert canonical_hash(date(2026, 1, 5)) != canonical_hash(datetime(2026, 1, 5))


def test_nan_and_inf_stable() -> None:
    assert canonical_hash(float("nan")) == canonical_hash(float("nan"))
    assert canonical_hash(float("inf")) != canonical_hash(float("-inf"))


def test_bool_not_confused_with_int() -> None:
    assert canonical_hash(True) != canonical_hash(1.0)


def test_raw_bytes_hash() -> None:
    assert raw_bytes_hash(b"abc") == raw_bytes_hash(b"abc")
    assert raw_bytes_hash(b"abc") != raw_bytes_hash(b"abd")
