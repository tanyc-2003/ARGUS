import gzip
import json

import httpx
import pytest

from argus.ops.errors import SourceDown
from argus.ops.http import FetchClient
from argus.settings import Settings
from argus.sources import alpaca


def _settings_with_keys(settings: Settings) -> Settings:
    return settings.model_copy(
        update={"alpaca_key_id": "key", "alpaca_secret_key": "secret"}
    )


def _paged_client() -> tuple[FetchClient, list[httpx.Request]]:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        token = request.url.params.get("page_token")
        if token is None:
            return httpx.Response(
                200,
                json={"quotes": [{"t": "a", "bp": 1.0, "ap": 1.1}], "next_page_token": "p2"},
            )
        return httpx.Response(
            200, json={"quotes": [{"t": "b", "bp": 1.0, "ap": 1.2}], "next_page_token": None}
        )

    fc = FetchClient(
        "alpaca_iex",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=lambda _: None,
    )
    return fc, requests


def test_missing_credentials_is_source_down(ctx) -> None:
    with pytest.raises(SourceDown, match="not configured"):
        alpaca.capture(ctx)


def test_pagination_aggregated_into_one_payload(ctx) -> None:
    ctx.settings = _settings_with_keys(ctx.settings)
    client, requests = _paged_client()
    result = alpaca.capture(ctx, client=client)

    # 2 tickers x LOOKBACK_SESSIONS sessions, 2 pages each
    n_payloads = ctx.conn.execute(
        "SELECT COUNT(*) FROM landing_manifest WHERE dataset='quote_ticks'"
    ).fetchone()[0]
    assert result.rows_out == n_payloads == 2 * alpaca.LOOKBACK_SESSIONS
    assert len(requests) == 2 * n_payloads

    path = ctx.conn.execute(
        "SELECT path FROM landing_manifest WHERE dataset='quote_ticks' LIMIT 1"
    ).fetchone()[0]
    with open(path, "rb") as fh:
        body = json.loads(gzip.decompress(fh.read()))
    assert body["n_quotes"] == 2  # both pages merged
    assert body["feed"] == "iex"


def test_rerun_is_idempotent(ctx) -> None:
    ctx.settings = _settings_with_keys(ctx.settings)
    alpaca.capture(ctx, client=_paged_client()[0])
    client, requests = _paged_client()
    second = alpaca.capture(ctx, client=client)
    assert second.rows_out == 0
    assert len(requests) == 0  # never fetched twice


def test_schema_drift_on_missing_quotes_key(ctx) -> None:
    ctx.settings = _settings_with_keys(ctx.settings)
    fc = FetchClient(
        "alpaca_iex",
        client=httpx.Client(
            transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"data": []}))
        ),
        sleep=lambda _: None,
    )
    from argus.ops.errors import SchemaDrift

    with pytest.raises(SchemaDrift):
        alpaca.capture(ctx, client=fc)
