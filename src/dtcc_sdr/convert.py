"""Convert one DTCC cumulative ZIP to a typed Parquet partition.

Each ZIP holds one CSV whose header carries the CFTC Part 43 public field names
(the SEC and Canadian files use the same 110 columns). DTCC ships no schema, so
the types live in :data:`COLUMN_TYPES`, keyed by source header; a header not in
that table stays text, so a new DTCC column is carried rather than dropped.

Casts are strict (``CAST``, not ``TRY_CAST``): a value that does not parse fails
the conversion instead of turning into a silent NULL. Notional amounts above the
public dissemination cap are published as e.g. ``160,000,000+``; the number goes
in the column and the ``+`` in a ``<column>_capped`` flag. Multi-valued fields
(``;``-separated schedules) stay text.

Output path, one file per regulator and day::

    <out_dir>/rates/regulator=<reg>/date=YYYY-MM-DD/data.parquet
"""

from __future__ import annotations

import csv
import re
import zipfile
from typing import TYPE_CHECKING, Literal

import duckdb
import structlog
from pydantic import BaseModel, ConfigDict

from dtcc_sdr.errors import ExternalServiceError

if TYPE_CHECKING:
    import datetime as dt
    from pathlib import Path

    from structlog.stdlib import BoundLogger

log: BoundLogger = structlog.get_logger(__name__)  # pyright: ignore[reportAny]

Kind = Literal["id", "int", "decimal", "capped", "bool", "date", "timestamp"]

_KINDS: dict[Kind, tuple[str, ...]] = {
    "id": ("Dissemination Identifier", "Original Dissemination Identifier"),
    "int": (
        "Quantity frequency multiplier-Leg 1",
        "Quantity frequency multiplier-Leg 2",
        "Price notation",
        "Spread notation-Leg 1",
        "Spread notation-Leg 2",
        "Strike price notation",
        "Floating rate reset frequency period multiplier-leg 1",
        "Floating rate reset frequency period multiplier-leg 2",
        "Fixed rate payment frequency period multiplier-Leg 1",
        "Floating rate payment frequency period multiplier-Leg 1",
        "Fixed rate payment frequency period multiplier-Leg 2",
        "Floating rate payment frequency period multiplier-Leg 2",
        "Package transaction price notation",
        "Package transaction spread notation",
    ),
    "capped": (
        "Notional amount-Leg 1",
        "Notional amount-Leg 2",
        "Notional quantity-Leg 1",
        "Notional quantity-Leg 2",
        "Total notional quantity-Leg 1",
        "Total notional quantity-Leg 2",
        "Call amount",
        "Put amount",
    ),
    "decimal": (
        "Exchange rate",
        "Fixed rate-Leg 1",
        "Fixed rate-Leg 2",
        "Option Premium Amount",
        "Price",
        "Spread-Leg 1",
        "Spread-Leg 2",
        "Strike Price",
        "Index factor",
        "Package transaction price",
        "Package transaction spread",
    ),
    "bool": (
        "Amendment indicator",
        "Mandatory clearing indicator",
        "Non-standardized term indicator",
        "Prime brokerage transaction indicator",
        "Block trade election indicator",
        "Large notional off-facility swap election indicator",
        "Post-priced swap indicator",
        "Custom basket indicator",
        "Package indicator",
    ),
    "date": (
        "Effective Date",
        "Expiration Date",
        "Maturity date of the underlier",
        "First exercise date",
    ),
    "timestamp": ("Event timestamp", "Execution Timestamp"),
}
COLUMN_TYPES: dict[str, Kind] = {name: kind for kind, names in _KINDS.items() for name in names}

# 26 integer digits holds any notional DTCC publishes; 12 decimals holds every
# rate and price observed in the files.
_DECIMAL = "DECIMAL(38,12)"
_SQL_TYPE: dict[Kind, str] = {
    "id": "BIGINT",
    "int": "INTEGER",
    "decimal": _DECIMAL,
    "capped": _DECIMAL,
    "bool": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMPTZ",
}


class Converted(BaseModel):
    """One converted file."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    rows: int
    columns: int


def column_name(source: str) -> str:
    """Snake-case Parquet name for a DTCC header, e.g. ``notional_amount_leg_1``."""
    return re.sub(r"[^0-9a-z]+", "_", source.lower()).strip("_")


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _select_exprs(header: list[str]) -> list[str]:
    exprs: list[str] = []
    seen: set[str] = set()
    for source in header:
        name = column_name(source)
        if name in seen:
            raise ExternalServiceError(f"two DTCC headers map to column {name!r}")
        seen.add(name)
        src = _quote(source)
        out = _quote(name)
        kind = COLUMN_TYPES.get(source)
        # Some Canadian files pad values ("25,000,000 ", a lone " " indicator).
        empty = f"NULLIF(trim({src}), '')"
        if kind is None:
            exprs.append(f"{src} AS {out}")
        elif kind == "capped":
            digits = f"replace(replace({empty}, ',', ''), '+', '')"
            exprs.append(f"CAST({digits} AS {_DECIMAL}) AS {out}")
            exprs.append(f"({empty} LIKE '%+') AS {_quote(name + '_capped')}")
        elif kind == "decimal":
            exprs.append(f"CAST(replace({empty}, ',', '') AS {_DECIMAL}) AS {out}")
        else:
            exprs.append(f"CAST({empty} AS {_SQL_TYPE[kind]}) AS {out}")
    return exprs


def partition_path(regulator: str, day: dt.date, table: str = "rates") -> str:
    """Repo-relative path of one day's partition."""
    return f"{table}/regulator={regulator}/date={day.isoformat()}/data.parquet"


def _member(zf: zipfile.ZipFile) -> str:
    names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
    if len(names) != 1:
        raise ExternalServiceError(f"expected one CSV in the ZIP, found {names}")
    return names[0]


def _header(path: Path) -> list[str]:
    with path.open(encoding="utf-8-sig", newline="") as fh:
        return next(csv.reader([fh.readline()]))


def convert_csv(
    csv_path: Path,
    out_file: Path,
    *,
    memory_limit: str | None = None,
    threads: int | None = None,
) -> Converted:
    """Convert one DTCC CSV to ``out_file`` (atomic: written to a temp name first).

    Raises:
        ExternalServiceError: if a value fails its cast or the CSV is malformed.

    """
    out_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_pq = out_file.with_name(".tmp-data.parquet")
    header = _header(csv_path)
    select = ",\n  ".join(_select_exprs(header))
    con = duckdb.connect()
    try:
        if memory_limit:
            _ = con.execute(f"SET memory_limit='{memory_limit}'")
        if threads:
            _ = con.execute(f"SET threads={int(threads)}")
        row = con.execute(
            f"""
            COPY (
              SELECT {select}
              FROM read_csv('{csv_path}', header=true, all_varchar=true, delim=',',
                            quote='"', escape='"', strict_mode=true)
            ) TO '{tmp_pq}' (FORMAT PARQUET, COMPRESSION zstd)
            """  # noqa: S608  # identifiers are quoted DTCC headers, paths are ours
        ).fetchone()
        _ = tmp_pq.replace(out_file)
    except duckdb.Error as exc:
        raise ExternalServiceError(f"cannot convert {csv_path}: {exc}") from exc
    finally:
        con.close()
        tmp_pq.unlink(missing_ok=True)
    rows = int(row[0]) if row else 0  # pyright: ignore[reportAny]
    extra = sum(1 for h in header if COLUMN_TYPES.get(h) == "capped")
    return Converted(rows=rows, columns=len(header) + extra)


def convert_zip(
    zip_path: Path,
    out_file: Path,
    *,
    memory_limit: str | None = None,
    threads: int | None = None,
) -> Converted:
    """Extract the ZIP's CSV beside ``out_file`` and convert it.

    Raises:
        ExternalServiceError: if the ZIP is unreadable or a value fails its cast.

    """
    out_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_csv = out_file.with_name(".tmp-data.csv")
    try:
        try:
            with (
                zipfile.ZipFile(zip_path) as zf,
                zf.open(_member(zf)) as src,
                tmp_csv.open("wb") as dst,
            ):
                while chunk := src.read(1 << 20):
                    _ = dst.write(chunk)
        except (OSError, zipfile.BadZipFile) as exc:
            raise ExternalServiceError(f"cannot read {zip_path}: {exc}") from exc
        return convert_csv(tmp_csv, out_file, memory_limit=memory_limit, threads=threads)
    finally:
        tmp_csv.unlink(missing_ok=True)
