"""Offline tests of URL, date, listing and conditional-fetch logic."""

from __future__ import annotations

import datetime as dt
import json

import httpx
import pytest

from dtcc_sdr import dtcc
from dtcc_sdr.errors import ExternalServiceError

D = dt.date(2024, 11, 12)


@pytest.mark.parametrize(
    ("regulator", "expected"),
    [
        (
            "cftc",
            "https://kgc0418-tdw-data-0.s3.amazonaws.com/cftc/eod/CFTC_CUMULATIVE_RATES_2024_11_12.zip",
        ),
        (
            "sec",
            "https://kgc0418-tdw-data-0.s3.amazonaws.com/sec/eod/SEC_CUMULATIVE_RATES_2024_11_12.zip",
        ),
        (
            "ca",
            "https://kgc0418-tdw-data-0.s3.amazonaws.com/ca/eod/CA_CUMULATIVE_RATES_2024_11_12.zip",
        ),
    ],
)
def test_url_for(regulator: str, expected: str):
    assert dtcc.url_for(regulator, D) == expected


def test_url_for_other_asset_class():
    assert dtcc.url_for("cftc", D, "CREDITS").endswith(
        "/cftc/eod/CFTC_CUMULATIVE_CREDITS_2024_11_12.zip"
    )


def test_unknown_regulator():
    with pytest.raises(KeyError):
        _ = dtcc.url_for("esma", D)


def test_parse_file_name_roundtrips():
    assert dtcc.parse_file_name(dtcc.file_name("sec", D)) == ("sec", "RATES", D)
    with pytest.raises(ExternalServiceError):
        _ = dtcc.parse_file_name("CFTC_SLICE_RATES_2024_11_12_1.zip")


def test_default_window_after_publish():
    # 00:15 UTC files are out by 01:00 UTC, so yesterday is the last day.
    now = dt.datetime(2026, 11, 11, 2, 0, tzinfo=dt.UTC)
    start, end = dtcc.default_window(now)
    assert end == dt.date(2026, 11, 10)
    assert start == dt.date(2024, 11, 10)


def test_default_window_before_publish():
    now = dt.datetime(2026, 11, 11, 0, 10, tzinfo=dt.UTC)
    _, end = dtcc.default_window(now)
    assert end == dt.date(2026, 11, 9)


def test_date_range_inclusive_every_calendar_day():
    days = list(dtcc.date_range(dt.date(2024, 11, 8), dt.date(2024, 11, 11)))
    # Weekend files exist on the tape, so weekends are not skipped.
    assert days == [dt.date(2024, 11, d) for d in (8, 9, 10, 11)]


@pytest.mark.parametrize(
    ("status", "body", "expected"),
    [
        (403, b"<Error><Code>InvalidObjectState</Code></Error>", "archived"),
        (403, b"<Error><Code>AccessDenied</Code></Error>", "missing"),
        (404, b"<Error><Code>NoSuchKey</Code></Error>", "missing"),
        (500, b"", "error"),
    ],
)
def test_classify_error(status: int, body: bytes, expected: str):
    assert dtcc.classify_error(status, body) == expected


def _listing_item(name: str, slice_id: int, rows: int = 10) -> dict[str, object]:
    return {
        "sliceId": slice_id,
        "fileName": name,
        "startTs": "2026-11-10T00:15:07Z",
        "endTs": f"2026-11-10T00:15:{slice_id % 60:02d}Z",
        "rowCount": rows,
        "dissemDTM": "2026-11-10T00:00:00Z",
        "fullFilePath": f"{dtcc.BASE_URL}/cftc/eod/{name}",
    }


def _client(handler: httpx.MockTransport) -> dtcc.Client:
    return dtcc.Client(per_second=1000.0, http=httpx.Client(transport=handler))


def test_list_files_keeps_newest_slice_per_file():
    name = "CFTC_CUMULATIVE_RATES_2026_11_09.zip"
    body = [_listing_item(name, 5), _listing_item(name, 9, rows=11)]

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/ppd/api/cumulative/CFTC/IR"
        return httpx.Response(200, content=json.dumps(body))

    client = _client(httpx.MockTransport(handler))
    listed = client.list_files("cftc")
    assert [(e.file_name, e.slice_id, e.row_count) for e in listed] == [(name, 9, 11)]
    assert client.traffic.requests == 1


def test_list_files_wraps_non_list():
    client = _client(httpx.MockTransport(lambda _r: httpx.Response(200, content=b"{}")))
    with pytest.raises(ExternalServiceError):
        _ = client.list_files("sec")


def test_fetch_sends_if_none_match_and_maps_304():
    seen: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("if-none-match"))
        return httpx.Response(304)

    client = _client(httpx.MockTransport(handler))
    result = client.fetch("https://example.test/a.zip", '"abc"')
    assert result.outcome == "not_modified"
    assert seen == ['"abc"']


def test_fetch_200_and_403_outcomes():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("new.zip"):
            return httpx.Response(200, content=b"zip", headers={"ETag": '"e1"'})
        if request.url.path.endswith("old.zip"):
            return httpx.Response(403, content=b"<Code>InvalidObjectState</Code>")
        return httpx.Response(403, content=b"<Code>AccessDenied</Code>")

    client = _client(httpx.MockTransport(handler))
    new = client.fetch("https://example.test/new.zip")
    assert (new.outcome, new.body, new.etag) == ("fetched", b"zip", '"e1"')
    assert client.fetch("https://example.test/old.zip").outcome == "archived"
    assert client.fetch("https://example.test/none.zip").outcome == "missing"
    assert client.traffic.requests == 3


def test_fetch_retries_5xx_then_succeeds():
    calls = {"n": 0}

    def handler(_request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(503) if calls["n"] < 3 else httpx.Response(200, content=b"ok")

    client = _client(httpx.MockTransport(handler))
    assert client.fetch("https://example.test/x.zip").outcome == "fetched"
    assert calls["n"] == 3
