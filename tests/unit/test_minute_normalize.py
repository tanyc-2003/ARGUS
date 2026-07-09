import gzip
import io
import json
from datetime import UTC, datetime

import pandas as pd
import polars as pl
import pytest

from argus.normalize.minute import bucket_quotes, parse_yf_minute_parquet
from argus.ops.errors import SchemaDrift


def _yf_payload(tz: str | None) -> bytes:
    idx = pd.DatetimeIndex(
        [pd.Timestamp("2026-07-07 09:30"), pd.Timestamp("2026-07-07 09:31")],
        name="Datetime",
    )
    if tz:
        idx = idx.tz_localize(tz)
    frame = pd.DataFrame(
        {"Open": [1.0, 1.1], "High": [1.2, 1.2], "Low": [0.9, 1.0],
         "Close": [1.1, 1.15], "Volume": [1000, 1100]},
        index=idx,
    ).reset_index()
    buf = io.BytesIO()
    frame.to_parquet(buf, index=False)
    return buf.getvalue()


def test_parse_tz_aware_converts_to_utc() -> None:
    out = parse_yf_minute_parquet(_yf_payload("America/New_York"), "spy")
    assert out["minute_ts"][0] == datetime(2026, 7, 7, 13, 30, tzinfo=UTC)  # EDT +4
    assert out["ticker"][0] == "SPY"


def test_parse_naive_treated_as_exchange_local() -> None:
    out = parse_yf_minute_parquet(_yf_payload(None), "spy")
    assert out["minute_ts"][0] == datetime(2026, 7, 7, 13, 30, tzinfo=UTC)


def test_parse_garbage_is_drift() -> None:
    with pytest.raises(SchemaDrift):
        parse_yf_minute_parquet(b"nope", "SPY")


def _quote_payload(quotes: list[dict]) -> bytes:
    return gzip.compress(json.dumps({"quotes": quotes}).encode())


def test_bucket_quotes_close_and_time_weighted_mean() -> None:
    session_close = datetime(2026, 7, 7, 20, 0, tzinfo=UTC)
    payload = _quote_payload(
        [
            {"t": "2026-07-07T14:30:10Z", "bp": 10.0, "ap": 10.1},
            {"t": "2026-07-07T14:30:40Z", "bp": 10.05, "ap": 10.15},
            {"t": "2026-07-07T14:31:20Z", "bp": 10.0, "ap": 10.2},
        ]
    )
    out = bucket_quotes(payload, "spy", session_close).sort("minute_ts")
    assert out.height == 2

    m1 = out.row(0, named=True)
    assert m1["bid_close"] == 10.05 and m1["ask_close"] == 10.15
    assert m1["n_quotes"] == 2
    # TW mean: 10.0 stands 30s, 10.05 stands 20s (to minute end) -> (300+201)/50
    assert abs(m1["bid_twm"] - (10.0 * 30 + 10.05 * 20) / 50) < 1e-9

    m2 = out.row(1, named=True)
    assert m2["bid_close"] == 10.0 and m2["ask_close"] == 10.2
    assert m2["n_quotes"] == 1
    assert abs(m2["bid_twm"] - 10.0) < 1e-9  # lone quote stands to its minute end


def test_bucket_drops_zero_priced_quotes() -> None:
    session_close = datetime(2026, 7, 7, 20, 0, tzinfo=UTC)
    payload = _quote_payload(
        [
            {"t": "2026-07-07T14:30:10Z", "bp": 0.0, "ap": 10.1},  # empty book side
            {"t": "2026-07-07T14:30:40Z", "bp": 10.0, "ap": 10.1},
        ]
    )
    out = bucket_quotes(payload, "spy", session_close)
    assert out.height == 1
    assert out["n_quotes"][0] == 1


def test_bucket_empty_payload_returns_typed_empty() -> None:
    out = bucket_quotes(_quote_payload([]), "spy", datetime(2026, 7, 7, 20, 0, tzinfo=UTC))
    assert out.is_empty()
    assert isinstance(out.schema["minute_ts"], pl.Datetime)


def test_bucket_malformed_quote_is_drift() -> None:
    with pytest.raises(SchemaDrift):
        bucket_quotes(_quote_payload([{"bp": 1.0}]), "spy",
                      datetime(2026, 7, 7, 20, 0, tzinfo=UTC))
