"""Locate, list and fetch DTCC's public end-of-day cumulative files.

DTCC serves each regulator's cumulative file from a public S3 bucket for about
two years, after which the object moves to DEEP_ARCHIVE and is refused. The
bucket denies listing, so an absent key answers 403 AccessDenied rather than
404, and a rolled-off key answers 403 InvalidObjectState.

The PPD API lists the last 366 days per regulator with each file's slice id,
write time and row count, so one request per regulator says what exists and
whether a held file was reposted. ``httpx`` errors never leak past this module.
"""

from __future__ import annotations

import datetime as dt
import re
import threading
import time
from http import HTTPStatus
from typing import TYPE_CHECKING, Literal

import httpx
import stamina
import structlog
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from dtcc_sdr.errors import ExternalServiceError

if TYPE_CHECKING:
    from collections.abc import Iterator

    from structlog.stdlib import BoundLogger

log: BoundLogger = structlog.get_logger(__name__)  # pyright: ignore[reportAny]

BASE_URL = "https://kgc0418-tdw-data-0.s3.amazonaws.com"
LIST_URL = "https://pddata.dtcc.com/ppd/api/cumulative/{api_reg}/{api_asset}"
# regulator -> (S3 prefix, file-name prefix, PPD API regulator)
REGULATORS: dict[str, tuple[str, str, str]] = {
    "cftc": ("cftc", "CFTC", "CFTC"),
    "sec": ("sec", "SEC", "SEC"),
    "ca": ("ca", "CA", "CA"),
}
# file-name asset class -> PPD API asset class
ASSET_CLASSES: dict[str, str] = {
    "RATES": "IR",
    "CREDITS": "CR",
    "EQUITIES": "EQ",
    "FOREX": "FX",
    "COMMODITIES": "CO",
}
# Slightly longer than the S3 retention, so the oldest still-served day is covered.
WINDOW_DAYS = 731
# Cumulative files appear at about 00:15 UTC for the prior day.
PUBLISH_HOUR_UTC = 1
USER_AGENT = "dtcc-sdr (+https://github.com/ak2k/dtcc-sdr)"
_FILE_RE = re.compile(r"^(CFTC|SEC|CA)_CUMULATIVE_([A-Z]+)_(\d{4})_(\d{2})_(\d{2})\.zip$")
_TIMEOUT = httpx.Timeout(30.0, read=300.0)
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})

Outcome = Literal["fetched", "not_modified", "missing", "archived"]


def file_name(regulator: str, day: dt.date, asset_class: str = "RATES") -> str:
    """``CFTC_CUMULATIVE_RATES_2024_11_12.zip`` for ``("cftc", 2024-11-12)``."""
    return f"{REGULATORS[regulator][1]}_CUMULATIVE_{asset_class}_{day:%Y_%m_%d}.zip"


def url_for(regulator: str, day: dt.date, asset_class: str = "RATES") -> str:
    """Public S3 URL of one day's cumulative file."""
    prefix = REGULATORS[regulator][0]
    return f"{BASE_URL}/{prefix}/eod/{file_name(regulator, day, asset_class)}"


def parse_file_name(name: str) -> tuple[str, str, dt.date]:
    """``(regulator, asset_class, day)`` from a cumulative file name.

    Raises:
        ExternalServiceError: if the name is not a cumulative file name.

    """
    match = _FILE_RE.match(name)
    if not match:
        raise ExternalServiceError(f"not a DTCC cumulative file name: {name!r}")
    reg, asset, y, m, d = match.groups()
    return reg.lower(), asset, dt.date(int(y), int(m), int(d))


def default_window(now: dt.datetime | None = None) -> tuple[dt.date, dt.date]:
    """``(start, end)`` of the served window; ``end`` is the newest published day."""
    now = now or dt.datetime.now(dt.UTC)
    end = now.date() - dt.timedelta(days=1 if now.hour >= PUBLISH_HOUR_UTC else 2)
    return now.date() - dt.timedelta(days=WINDOW_DAYS), end


def date_range(start: dt.date, end: dt.date) -> Iterator[dt.date]:
    """Every calendar day from ``start`` to ``end`` inclusive (weekend files exist)."""
    day = start
    while day <= end:
        yield day
        day += dt.timedelta(days=1)


def classify_error(status: int, body: bytes) -> Outcome | Literal["error"]:
    """Map a non-success S3 answer to an outcome."""
    if status == HTTPStatus.FORBIDDEN and b"InvalidObjectState" in body:
        return "archived"
    if status in (HTTPStatus.FORBIDDEN, HTTPStatus.NOT_FOUND):
        return "missing"
    return "error"


class Listed(BaseModel):
    """One entry of the PPD cumulative listing."""

    model_config = ConfigDict(extra="ignore", frozen=True, populate_by_name=True)

    slice_id: int = Field(alias="sliceId")
    file_name: str = Field(alias="fileName")
    end_ts: str = Field(alias="endTs")
    row_count: int = Field(alias="rowCount")
    url: str = Field(alias="fullFilePath")


class Fetched(BaseModel):
    """Result of one conditional GET."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    outcome: Outcome
    body: bytes = b""
    etag: str | None = None
    last_modified: str | None = None


class _RateLimiter:
    """Spaces request starts at least ``1 / per_second`` seconds apart."""

    def __init__(self, per_second: float) -> None:
        self.interval = 1.0 / per_second
        self.lock = threading.Lock()
        self.next_at = 0.0

    def wait(self) -> None:
        with self.lock:
            now = time.monotonic()
            delay = self.next_at - now
            self.next_at = max(now, self.next_at) + self.interval
        if delay > 0:
            time.sleep(delay)


class Traffic(BaseModel):
    """Requests made and body bytes received, for the run summary."""

    model_config = ConfigDict(extra="forbid")

    requests: int = 0
    bytes: int = 0


class Client:
    """Rate-limited DTCC client that counts its own traffic."""

    def __init__(self, per_second: float = 2.0, http: httpx.Client | None = None) -> None:
        """Wrap ``http`` (default: a fresh ``httpx.Client``) with pacing and counters."""
        self.http = http or httpx.Client(
            headers={"User-Agent": USER_AGENT}, timeout=_TIMEOUT, follow_redirects=True
        )
        self.limiter = _RateLimiter(per_second)
        self.traffic = Traffic()

    def _send(self, url: str, headers: dict[str, str] | None = None) -> httpx.Response:
        self.limiter.wait()
        response = self.http.get(url, headers=headers or {})
        self.traffic.requests += 1
        self.traffic.bytes += len(response.content)
        if response.status_code in _RETRYABLE_STATUS:
            _ = response.raise_for_status()
        return response

    @stamina.retry(on=(httpx.TransportError, httpx.HTTPStatusError), attempts=4, wait_max=30.0)
    def _get(self, url: str, headers: dict[str, str] | None = None) -> httpx.Response:
        return self._send(url, headers)

    def list_files(self, regulator: str, asset_class: str = "RATES") -> list[Listed]:
        """The PPD listing for one regulator, newest slice per file name.

        Raises:
            ExternalServiceError: on transport/HTTP failure or a drifted response.

        """
        url = LIST_URL.format(
            api_reg=REGULATORS[regulator][2], api_asset=ASSET_CLASSES[asset_class]
        )
        try:
            response = self._get(url)
            _ = response.raise_for_status()
            raw: object = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise ExternalServiceError(f"failed to list {regulator} {asset_class}: {exc}") from exc
        if not isinstance(raw, list):
            raise ExternalServiceError(f"listing for {regulator} is not a JSON list")
        newest: dict[str, Listed] = {}
        try:
            for item in raw:  # pyright: ignore[reportUnknownVariableType]
                entry = Listed.model_validate(item)
                held = newest.get(entry.file_name)
                # A reposted file appears as a second slice; the later one is current.
                if held is None or entry.slice_id > held.slice_id:
                    newest[entry.file_name] = entry
        except ValidationError as exc:
            raise ExternalServiceError(f"drifted listing entry for {regulator}: {exc}") from exc
        return sorted(newest.values(), key=lambda e: e.file_name)

    def fetch(self, url: str, etag: str | None = None) -> Fetched:
        """GET ``url``, conditional on ``etag`` when one is held.

        Raises:
            ExternalServiceError: on transport failure or an unexpected status.

        """
        headers = {"If-None-Match": etag} if etag else None
        try:
            response = self._get(url, headers)
        except httpx.HTTPError as exc:
            raise ExternalServiceError(f"failed to fetch {url}: {exc}") from exc
        if response.status_code == HTTPStatus.NOT_MODIFIED:
            return Fetched(outcome="not_modified", etag=etag)
        if response.status_code == HTTPStatus.OK:
            return Fetched(
                outcome="fetched",
                body=response.content,
                etag=response.headers.get("etag"),
                last_modified=response.headers.get("last-modified"),
            )
        outcome = classify_error(response.status_code, response.content)
        if outcome == "error":
            raise ExternalServiceError(f"unexpected HTTP {response.status_code} for {url}")
        return Fetched(outcome=outcome)
