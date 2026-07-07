import httpx
import pytest

from argus.ops.errors import SchemaDrift
from argus.ops.http import FetchClient
from argus.sources import symbol_dirs

GOOD_FILE = "\n".join(
    ["Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size"]
    + [f"TICK{i}|Test Security {i}|Q|N|N|100" for i in range(12)]
    + ["File Creation Time: 0707202622:01|||||"]
)


def _fake_client(body: str) -> FetchClient:
    return FetchClient(
        "nasdaqtrader",
        client=httpx.Client(
            transport=httpx.MockTransport(lambda r: httpx.Response(200, text=body))
        ),
        sleep=lambda _: None,
    )


def test_capture_lands_both_files(ctx) -> None:
    result = symbol_dirs.capture(ctx, client=_fake_client(GOOD_FILE))
    assert result.rows_out == 2
    n = ctx.conn.execute(
        "SELECT COUNT(*) FROM landing_manifest WHERE dataset='symbol_dirs'"
    ).fetchone()[0]
    assert n == 2


def test_capture_is_idempotent(ctx) -> None:
    symbol_dirs.capture(ctx, client=_fake_client(GOOD_FILE))
    result = symbol_dirs.capture(ctx, client=_fake_client(GOOD_FILE))
    assert result.rows_out == 0
    assert "already=2" in result.detail


def test_malformed_payload_raises_schema_drift(ctx) -> None:
    with pytest.raises(SchemaDrift):
        symbol_dirs.capture(ctx, client=_fake_client("<html>maintenance page</html>"))


def test_missing_footer_raises_schema_drift(ctx) -> None:
    body = GOOD_FILE.rsplit("\n", 1)[0]  # drop the File Creation Time footer
    with pytest.raises(SchemaDrift):
        symbol_dirs.capture(ctx, client=_fake_client(body))
