"""One-off seed: convert a local archive of DTCC ZIPs into the dataset layout.

The archive is ``<root>/<reg>/<YYYY-MM-DD>/<FILE>.zip`` plus ``<root>/manifest.csv``
(``file,url,bytes,sha256,fetched_at``). Every archived file becomes a partition
and a manifest entry; a single failure fails the seed, so nothing is dropped.
Files are converted in parallel processes, one DuckDB thread each.
"""

from __future__ import annotations

import csv
import hashlib
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING

import structlog
from pydantic import BaseModel, ConfigDict

from dtcc_sdr import convert, dtcc
from dtcc_sdr.errors import ExternalServiceError
from dtcc_sdr.manifest import Entry, Manifest

if TYPE_CHECKING:
    from structlog.stdlib import BoundLogger

log: BoundLogger = structlog.get_logger(__name__)  # pyright: ignore[reportAny]


class ArchiveRow(BaseModel):
    """One row of the local archive's ``manifest.csv``."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    file: str
    url: str
    bytes: int
    sha256: str
    fetched_at: str


def read_archive(root: Path) -> list[ArchiveRow]:
    """Rows of ``<root>/manifest.csv``."""
    with (root / "manifest.csv").open(newline="", encoding="utf-8") as fh:
        return [ArchiveRow.model_validate(row) for row in csv.DictReader(fh)]


def _md5(path: Path) -> str:
    digest = hashlib.md5()  # noqa: S324  # S3's single-part ETag is the MD5; not a security use
    with path.open("rb") as fh:
        while chunk := fh.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def _convert_one(root: str, row: ArchiveRow, data_dir: str) -> Entry:
    path = Path(root) / row.file
    regulator, _, day = dtcc.parse_file_name(path.name)
    if _sha256(path) != row.sha256:
        raise ExternalServiceError(f"{row.file}: SHA-256 differs from the archive manifest")
    partition = convert.partition_path(regulator, day)
    result = convert.convert_zip(path, Path(data_dir) / partition, threads=1)
    return Entry(
        regulator=regulator,
        day=day,
        file=row.file,
        url=row.url,
        bytes=row.bytes,
        sha256=row.sha256,
        rows=result.rows,
        partition=partition,
        etag=f'"{_md5(path)}"',
        fetched_at=row.fetched_at,
    )


def build(
    root: Path,
    data_dir: Path,
    *,
    workers: int | None = None,
    listings: dict[str, list[dtcc.Listed]] | None = None,
) -> Manifest:
    """Convert every archived file; return the manifest describing them.

    Raises:
        ExternalServiceError: if any file fails, after all others are tried.

    """
    rows = read_archive(root)
    manifest = Manifest()
    failures: list[str] = []
    workers = workers or max(1, (os.cpu_count() or 2) // 2)
    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_convert_one, str(root), r, str(data_dir)): r for r in rows}
        for done, fut in enumerate(as_completed(futures), 1):
            row = futures[fut]
            try:
                entry = fut.result()
            except ExternalServiceError as exc:
                failures.append(f"{row.file}: {exc}")
                continue
            manifest.put(entry)
            if done % 200 == 0:
                log.info("seed.progress", done=done, total=len(rows))
    if failures:
        raise ExternalServiceError(f"{len(failures)} archive files failed: {failures[:5]}")
    if len(manifest.entries) != len(rows):
        raise ExternalServiceError(
            f"{len(rows)} archive rows produced {len(manifest.entries)} entries"
        )
    for regulator, listed in (listings or {}).items():
        for item in listed:
            _, _, day = dtcc.parse_file_name(item.file_name)
            held = manifest.get(regulator, day)
            if held is not None:
                manifest.put(
                    held.model_copy(
                        update={
                            "slice_id": item.slice_id,
                            "end_ts": item.end_ts,
                            "listed_rows": item.row_count,
                        }
                    )
                )
    return manifest
