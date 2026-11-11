"""Parsing and blob-id logic of the git-history backfill."""

from __future__ import annotations

import datetime as dt
import subprocess
from typing import TYPE_CHECKING

from dtcc_sdr import gitbackfill

if TYPE_CHECKING:
    from pathlib import Path

_A = "0" * 40
LOG = f"""C c2 2026-11-11T03:30:08Z

:100644 000000 {"b" * 40} {_A} D\tCFTC_CUMULATIVE_RATES_2024_01_22.csv
:000000 100644 {_A} {"c" * 40} A\tCFTC_CUMULATIVE_RATES_2024_01_23.csv
C c1 2026-11-10T03:30:08Z

:000000 100644 {_A} {"d" * 40} A\tCFTC_CUMULATIVE_RATES_2024_01_23.csv
:000000 100644 {_A} {"e" * 40} A\tCFTC_CUMULATIVE_CREDITS_2024_01_23.csv
:000000 100644 {_A} {"f" * 40} A\tCFTC_CUMULATIVE_RATES_2024_01_22.csv
"""


def test_parse_raw_log_keeps_latest_add_per_day_and_rates_only():
    files = gitbackfill.parse_raw_log(LOG)
    assert [(f.day, f.commit, f.blob[0]) for f in files] == [
        (dt.date(2024, 1, 22), "c1", "f"),
        (dt.date(2024, 1, 23), "c2", "c"),
    ]


def test_blob_id_matches_git(tmp_path: Path):
    data = b"a,b\n1,2\n"
    path = tmp_path / "x.csv"
    _ = path.write_bytes(data)
    expected = subprocess.run(  # noqa: S603
        ["git", "hash-object", str(path)],  # noqa: S607
        capture_output=True,
        check=True,
        text=True,
    ).stdout.strip()
    assert gitbackfill.blob_id(data) == expected
