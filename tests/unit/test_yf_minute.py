from datetime import date

import pandas as pd

from argus.core import calendars
from argus.ops.ratelimit import TokenBucket
from argus.sources import yf_minute


def _fast_bucket() -> TokenBucket:
    clock = {"t": 0.0}

    def tick() -> float:
        clock["t"] += 0.001
        return clock["t"]

    return TokenBucket(rate_per_sec=1e6, capacity=1e6, clock=tick, sleep=lambda _: None)


def _frame() -> pd.DataFrame:
    idx = pd.DatetimeIndex(
        [pd.Timestamp("2026-07-07 13:30:00", tz="UTC")], name="Datetime"
    )
    return pd.DataFrame(
        {"Open": [1.0], "High": [1.1], "Low": [0.9], "Close": [1.05], "Volume": [1000]},
        index=idx,
    )


def test_capture_lands_one_payload_per_ticker_session(ctx) -> None:
    calls: list[tuple[str, date]] = []

    def downloader(ticker: str, session: date) -> pd.DataFrame:
        calls.append((ticker, session))
        return _frame()

    result = yf_minute.capture(ctx, downloader=downloader, bucket=_fast_bucket())
    n_sessions = len(calendars.sessions_within(ctx.trade_date, yf_minute.LOOKBACK_DAYS))
    # test watchlist has 2 tickers (conftest)
    assert n_sessions > 10  # ~17 sessions inside a 25-calendar-day window
    assert result.rows_out == 2 * n_sessions == len(calls)


def test_capture_idempotent_across_runs(ctx) -> None:
    yf_minute.capture(ctx, downloader=lambda t, s: _frame(), bucket=_fast_bucket())
    second = yf_minute.capture(ctx, downloader=lambda t, s: _frame(), bucket=_fast_bucket())
    assert second.rows_out == 0
    assert second.budget_used == 0  # nothing refetched — the L0 cache guarantee


def test_empty_frames_not_landed(ctx) -> None:
    result = yf_minute.capture(ctx, downloader=lambda t, s: pd.DataFrame(), bucket=_fast_bucket())
    assert result.rows_out == 0
    assert "empty=" in result.detail
    n = ctx.conn.execute("SELECT COUNT(*) FROM landing_manifest").fetchone()[0]
    assert n == 0


def test_per_request_failures_do_not_kill_the_job(ctx) -> None:
    def flaky(ticker: str, session: date) -> pd.DataFrame:
        if ticker == "SPY":
            raise RuntimeError("yahoo hiccup")
        return _frame()

    result = yf_minute.capture(ctx, downloader=flaky, bucket=_fast_bucket())
    assert result.rows_out > 0
    assert "failures=" in result.detail


def test_multiindex_columns_flattened(ctx) -> None:
    df = _frame()
    df.columns = pd.MultiIndex.from_product([df.columns, ["SPY"]])
    payload = yf_minute._to_parquet_bytes(df)
    import io

    back = pd.read_parquet(io.BytesIO(payload))
    assert all(isinstance(c, str) for c in back.columns)
