"""Shared yfinance plumbing.

yfinance is the one source we do not drive through FetchClient (its library
manages Yahoo auth), so vendor-side warts have to be handled at the call site
rather than in ops/http.py.
"""

from __future__ import annotations

import warnings
from collections.abc import Iterator
from contextlib import contextmanager


def yahoo_symbol(ticker: str) -> str:
    """Translate a canonical ARGUS ticker into Yahoo's spelling.

    Share classes are the one place vendors disagree on the symbol itself:
    Alpaca and Polygon want `BRK.B`, Yahoo wants `BRK-B`, and neither accepts
    the other's form (verified 2026-07: BRK.B -> 0 rows from Yahoo, BRK-B ->
    TransportFailure from Alpaca). universe.yaml therefore stores the dotted
    canonical form and each adapter spells it for its own vendor.
    """
    return ticker.replace(".", "-")


@contextmanager
def quiet_vendor_deprecations() -> Iterator[None]:
    """Mute yfinance's own pandas-deprecation noise around a download call.

    yfinance 0.2.66 calls `pd.Timestamp.utcnow()` in scrapers/history.py, which
    pandas 3 deprecates (Pandas4Warning, a DeprecationWarning subclass). It is
    the vendor's line, not ours -- there is no argument that avoids it, and
    patching site-packages would be undone by the next reinstall. So we silence
    it where we call them.

    Deliberately narrow: matched on the message and scoped to the download call,
    so a genuine deprecation raised by ARGUS still reaches the log. Drop this
    once yfinance ships the fix (then the filter simply matches nothing).
    """
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=r".*utcnow is deprecated.*",
            category=DeprecationWarning,
        )
        yield
