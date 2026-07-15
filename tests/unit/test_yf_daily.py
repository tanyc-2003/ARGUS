import io
from datetime import date, timedelta

import pandas as pd
import polars as pl
import pytest

from argus.normalize.daily import parse_yf_daily_parquet
from argus.ops.errors import SchemaDrift
from argus.ops.ratelimit import TokenBucket
from argus.sources import yf_daily


def _fast_bucket() -> TokenBucket:
    clock = {"t": 0.0}

    def tick() -> float:
        clock["t"] += 0.001
        return clock["t"]

    return TokenBucket(rate_per_sec=1e6, capacity=1e6, clock=tick, sleep=lambda _: None)


def _pd_frame() -> pd.DataFrame:
    idx = pd.DatetimeIndex([pd.Timestamp("2026-07-06"), pd.Timestamp("2026-07-07")],
                           name="Date")
    return pd.DataFrame(
        {
            "Open": [100.0, 101.0], "High": [102.0, 103.0], "Low": [99.0, 100.0],
            "Close": [101.0, 102.0], "Adj Close": [100.5, 101.5],
            "Volume": [1_000_000, 1_100_000],
        },
        index=idx,
    )


def test_capture_lands_one_payload_per_ticker(ctx) -> None:
    result = yf_daily.capture(ctx, downloader=lambda t, s, e: _pd_frame(),
                              bucket=_fast_bucket())
    assert result.rows_out == 2  # SPY + AAPL in the test universe
    second = yf_daily.capture(ctx, downloader=lambda t, s, e: _pd_frame(),
                              bucket=_fast_bucket())
    assert second.rows_out == 0 and second.budget_used == 0


def _add_ticker(ctx, ticker: str) -> None:
    """Simulate the user editing universe.yaml after ARGUS is already running."""
    path = ctx.settings.config_dir / "universe.yaml"
    path.write_text(path.read_text(encoding="utf-8") + f"  - {{ticker: {ticker}, role: sp100}}\n",
                    encoding="utf-8")


def test_backfill_pulls_deep_history_only_for_new_tickers(ctx) -> None:
    """Edit universe.yaml after setup -> the new name gets its full history, and
    the already-bootstrapped names are not re-fetched or rewritten."""
    yf_daily.capture_history(ctx, downloader=lambda t, s, e: _pd_frame(), bucket=_fast_bucket())
    _add_ticker(ctx, "NVDA")

    seen: list[tuple[str, date]] = []

    def downloader(t: str, s: date, e: date) -> pd.DataFrame:
        seen.append((t, s))
        return _pd_frame()

    result = yf_daily.backfill_new_tickers(ctx, downloader=downloader, bucket=_fast_bucket())

    assert [t for t, _ in seen] == ["NVDA"]  # SPY/AAPL untouched — old data left alone
    assert seen[0][1] == yf_daily.HISTORY_START  # full depth, not the 12-day window
    assert result.rows_out == 1


def test_backfill_is_a_noop_once_every_ticker_has_history(ctx) -> None:
    """Runs every night, so it must cost nothing when nothing was added."""
    yf_daily.capture_history(ctx, downloader=lambda t, s, e: _pd_frame(), bucket=_fast_bucket())
    calls: list[str] = []

    def downloader(t: str, s: date, e: date) -> pd.DataFrame:
        calls.append(t)
        return _pd_frame()

    result = yf_daily.backfill_new_tickers(ctx, downloader=downloader, bucket=_fast_bucket())
    assert calls == [] and result.rows_out == 0 and result.budget_used == 0


def test_backfill_does_not_refetch_on_a_later_trade_date(ctx) -> None:
    """The history key carries the trade date, so 'already has history' must be
    matched on the ticker — not the key — or every night would re-pull 36 years."""
    _add_ticker(ctx, "NVDA")
    yf_daily.backfill_new_tickers(ctx, downloader=lambda t, s, e: _pd_frame(),
                                  bucket=_fast_bucket())
    ctx.trade_date = ctx.trade_date + timedelta(days=1)  # next night

    calls: list[str] = []
    result = yf_daily.backfill_new_tickers(
        ctx, downloader=lambda t, s, e: (calls.append(t), _pd_frame())[1], bucket=_fast_bucket()
    )
    assert calls == [] and result.rows_out == 0


def test_nightly_incrementals_still_cover_a_new_ticker(ctx) -> None:
    """Backfill handles depth; j02 must still pick the new name up day-to-day."""
    _add_ticker(ctx, "NVDA")
    result = yf_daily.capture(ctx, downloader=lambda t, s, e: _pd_frame(), bucket=_fast_bucket())
    assert result.rows_out == 3  # SPY + AAPL + NVDA


def test_capture_window_covers_revision_days(ctx) -> None:
    windows: list[tuple[date, date]] = []

    def downloader(t: str, s: date, e: date) -> pd.DataFrame:
        windows.append((s, e))
        return _pd_frame()

    yf_daily.capture(ctx, downloader=downloader, bucket=_fast_bucket())
    start, end = windows[0]
    assert (ctx.trade_date - start).days == yf_daily.LOOKBACK_DAYS  # T-5 sessions inside
    assert end == ctx.trade_date + timedelta(days=1)  # exclusive end


def test_parse_plain_columns() -> None:
    buf = io.BytesIO()
    _pd_frame().reset_index().to_parquet(buf, index=False)
    out = parse_yf_daily_parquet(buf.getvalue(), "aapl")
    assert out["ticker"].to_list() == ["AAPL", "AAPL"]
    assert out["close"].to_list() == [101.0, 102.0]
    assert out["adj_close"].to_list() == [100.5, 101.5]


def test_parse_suffixed_columns() -> None:
    df = pl.DataFrame(
        {
            "Date": [date(2026, 7, 7)], "Open_AAPL": [1.0], "High_AAPL": [2.0],
            "Low_AAPL": [0.5], "Close_AAPL": [1.5], "Adj Close_AAPL": [1.4],
            "Volume_AAPL": [1000.0],
        }
    )
    buf = io.BytesIO()
    df.write_parquet(buf)
    out = parse_yf_daily_parquet(buf.getvalue(), "AAPL")
    assert out["close"][0] == 1.5
    assert out["volume"][0] == 1000.0


def test_parse_missing_close_is_drift() -> None:
    df = pl.DataFrame({"Date": [date(2026, 7, 7)], "Open": [1.0]})
    buf = io.BytesIO()
    df.write_parquet(buf)
    with pytest.raises(SchemaDrift, match="missing"):
        parse_yf_daily_parquet(buf.getvalue(), "AAPL")


def test_parse_garbage_is_drift() -> None:
    with pytest.raises(SchemaDrift, match="unreadable"):
        parse_yf_daily_parquet(b"<html>not parquet</html>", "AAPL")
