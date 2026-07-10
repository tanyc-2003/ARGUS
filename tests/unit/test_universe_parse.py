import pytest

from argus.normalize.universe import parse_nasdaqlisted, parse_otherlisted
from argus.ops.errors import SchemaDrift

NASDAQ = "\n".join([
    "Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF"
    "|NextShares",
    "AAPL|Apple Inc. - Common Stock|Q|N|N|100|N|N",
    "QQQ|Invesco QQQ Trust|G|N|N|100|Y|N",
    "ZTEST|Test Listing - DO NOT USE|Q|Y|N|100|N|N",
    "File Creation Time: 0709202622:01|||||||",
])

OTHER = "\n".join([
    "ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol",
    "SPY|SPDR S&P 500 ETF Trust|P|SPY|Y|100|N|SPY",
    "BRK.B|Berkshire Hathaway Class B|N|BRK B|N|100|N|BRK.B",
    "AAM$A|AA Mission Preferred A|N|AAM pA|N|100|N|AAM-A",
    "ZZTEST|NYSE Test|N|ZZTEST|N|100|Y|ZZTEST",
    "File Creation Time: 0709202622:01|||||||",
])


def test_parse_nasdaqlisted_golden() -> None:
    df = parse_nasdaqlisted(NASDAQ)
    assert df["ticker"].to_list() == ["AAPL", "QQQ"]  # test issue dropped
    assert df["is_etf"].to_list() == [False, True]
    assert set(df["exchange"].to_list()) == {"NASDAQ"}


def test_parse_otherlisted_golden() -> None:
    df = parse_otherlisted(OTHER)
    assert df["ticker"].to_list() == ["SPY", "BRK.B", "AAM$A"]  # preferred kept verbatim
    assert df["exchange"].to_list() == ["P", "N", "N"]
    assert df["is_etf"].to_list() == [True, False, False]


def test_html_maintenance_page_is_drift() -> None:
    with pytest.raises(SchemaDrift):
        parse_nasdaqlisted("<html>down for maintenance</html>")


def test_missing_column_is_drift() -> None:
    bad = "Symbol|Name\nAAPL|Apple\nFile Creation Time: x|"
    with pytest.raises(SchemaDrift, match="missing columns"):
        parse_nasdaqlisted(bad)
