"""Symbol-directory parsing (Nasdaq Trader daily files).

Two pipe-delimited formats, both with a `File Creation Time` footer:
  nasdaqlisted.txt: Symbol|Security Name|Market Category|Test Issue|Financial Status|...|ETF|...
  otherlisted.txt:  ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|...|Test Issue|...

Test issues are dropped; everything else is kept verbatim — preferred-share
suffixes ($), units, warrants included. The graveyard diff works on the union
of both files per date, so an exchange move (NYSE -> Nasdaq) is not a death.
"""

from __future__ import annotations

import polars as pl

from argus.ops.errors import SchemaDrift

SNAPSHOT_SCHEMA: dict[str, type[pl.DataType]] = {
    "ticker": pl.Utf8,
    "security_name": pl.Utf8,
    "exchange": pl.Utf8,
    "is_etf": pl.Boolean,
}


def _parse(text: str, *, kind: str, symbol_col: str, exchange: str | None) -> pl.DataFrame:
    lines = [ln for ln in text.strip().splitlines() if ln.strip()]
    if len(lines) < 2 or "|" not in lines[0]:
        raise SchemaDrift(f"symbol_dirs:{kind} not pipe-delimited", source="nasdaqtrader")
    if "file creation time" in lines[-1].lower():
        lines = lines[:-1]
    header = [h.strip() for h in lines[0].split("|")]
    required = [symbol_col, "Security Name", "Test Issue"]
    missing = [c for c in required if c not in header]
    if missing:
        raise SchemaDrift(
            f"symbol_dirs:{kind} missing columns {missing} (got {header})",
            source="nasdaqtrader",
        )
    idx = {name: header.index(name) for name in header}

    rows: list[dict[str, object]] = []
    for ln in lines[1:]:
        parts = ln.split("|")
        if len(parts) < len(header):
            continue  # ragged trailer lines
        if parts[idx["Test Issue"]].strip().upper() == "Y":
            continue
        ticker = parts[idx[symbol_col]].strip().upper()
        if not ticker:
            continue
        etf_raw = parts[idx["ETF"]].strip().upper() if "ETF" in idx else ""
        if exchange is not None:
            exch: str | None = exchange
        elif "Exchange" in idx:
            exch = parts[idx["Exchange"]].strip()
        else:
            exch = None
        rows.append(
            {
                "ticker": ticker,
                "security_name": parts[idx["Security Name"]].strip(),
                "exchange": exch,
                "is_etf": etf_raw == "Y",
            }
        )
    if not rows:
        raise SchemaDrift(f"symbol_dirs:{kind} parsed to zero rows", source="nasdaqtrader")
    return pl.DataFrame(rows, schema=SNAPSHOT_SCHEMA).unique(
        subset=["ticker"], keep="first", maintain_order=True
    )


def parse_nasdaqlisted(text: str) -> pl.DataFrame:
    return _parse(text, kind="nasdaqlisted", symbol_col="Symbol", exchange="NASDAQ")


def parse_otherlisted(text: str) -> pl.DataFrame:
    return _parse(text, kind="otherlisted", symbol_col="ACT Symbol", exchange=None)


PARSERS = {"nasdaqlisted": parse_nasdaqlisted, "otherlisted": parse_otherlisted}
