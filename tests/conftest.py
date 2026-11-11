"""Shared test fixtures."""

from __future__ import annotations

import zipfile
from typing import TYPE_CHECKING

import pytest
import stamina

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


# Used by pytest via autouse; basedpyright can't see that wiring.
@pytest.fixture(autouse=True)
def _instant_retries() -> Iterator[None]:  # pyright: ignore[reportUnusedFunction]
    """Make stamina retries instant in tests — no real backoff sleeps."""
    stamina.set_testing(True, attempts=5)  # retry (capped), but with no waits
    yield
    stamina.set_testing(False)


HEADER = (
    "Dissemination Identifier,Action type,Event timestamp,Amendment indicator,"
    "Effective Date,Notional amount-Leg 1,Fixed rate-Leg 1,Other payment amount,"
    "Spread notation-Leg 1,Brand new column"
)
ROWS = (
    '6247439659000000101,NEWT,2026-11-09T22:51:43Z,FALSE,2027-06-30,"160,000,000+",'
    "0.04397,5260.00000;10.5,3,x",
    '6247433660000000101,CORR,2026-11-09T23:03:04Z,true,2026-09-30,"32,000,000",,,,',
)


def make_zip(path: Path, rows: tuple[str, ...] = ROWS, header: str = HEADER) -> Path:
    """Write a DTCC-shaped cumulative ZIP holding one CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(path.with_suffix(".csv").name, "\n".join((header, *rows)) + "\n")
    return path
