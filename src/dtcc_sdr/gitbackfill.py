"""One-off backfill of CFTC rates days from the git history of xbowery/SDR-parser.

That repository's daily Action has committed the previous day's
``CFTC_CUMULATIVE_RATES_YYYY_MM_DD.csv`` (unzipped from DTCC) since 2024-01-23,
so its history holds CFTC days that DTCC has since archived. Use a blobless
bare clone::

    git clone --filter=blob:none --bare https://github.com/xbowery/SDR-parser.git

Only days the dataset does not hold are taken; each gets ``source="xbowery-git"``
and the commit sha. Where a day is held from DTCC and the local ZIP archive is
given, the archived CSV's git blob id is compared with the committed blob, which
checks byte identity without downloading the committed file.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import re
import shutil
import subprocess
import zipfile
from typing import TYPE_CHECKING

import structlog
from pydantic import BaseModel, ConfigDict

from dtcc_sdr import convert
from dtcc_sdr.errors import ExternalServiceError
from dtcc_sdr.manifest import Entry, Manifest

if TYPE_CHECKING:
    from pathlib import Path

    from structlog.stdlib import BoundLogger

log: BoundLogger = structlog.get_logger(__name__)  # pyright: ignore[reportAny]

REPO_URL = "https://github.com/xbowery/SDR-parser"
_ADD_RE = re.compile(
    r"^:\d+ \d+ 0{40} ([0-9a-f]{40}) A\t(CFTC_CUMULATIVE_RATES_(\d{4})_(\d{2})_(\d{2})\.csv)$"
)


class GitFile(BaseModel):
    """One day's rates CSV as committed."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    day: dt.date
    commit: str
    committed_at: str
    blob: str
    path: str


class Overlap(BaseModel):
    """Agreement between committed CSVs and archived DTCC files on shared days."""

    model_config = ConfigDict(extra="forbid")

    compared: int = 0
    identical: int = 0
    differing: list[str] = []
    not_in_archive: int = 0


def _git() -> str:
    path = shutil.which("git")
    if path is None:
        raise ExternalServiceError("git is not on PATH")
    return path


def _run(repo: Path, *args: str, stdin: bytes | None = None) -> bytes:
    try:
        done = subprocess.run(  # noqa: S603  # fixed git argv, no shell
            [_git(), "-C", str(repo), *args], input=stdin, capture_output=True, check=True
        )
    except subprocess.CalledProcessError as exc:
        raise ExternalServiceError(f"git {args[0]} failed: {exc.stderr.decode()[:500]}") from exc
    return done.stdout


def parse_raw_log(text: str) -> list[GitFile]:
    """Added rates CSVs from ``git log --format='C %H %cI' --raw --no-abbrev``.

    The log is newest first; a day added more than once keeps its latest commit.
    """
    out: dict[dt.date, GitFile] = {}
    commit = committed_at = ""
    for line in text.splitlines():
        if line.startswith("C "):
            _, commit, committed_at = line.split(" ", 2)
            continue
        match = _ADD_RE.match(line)
        if not match:
            continue
        blob, path, y, m, d = match.groups()
        day = dt.date(int(y), int(m), int(d))
        if day not in out:
            out[day] = GitFile(
                day=day, commit=commit, committed_at=committed_at, blob=blob, path=path
            )
    return sorted(out.values(), key=lambda g: g.day)


def list_files(repo: Path) -> list[GitFile]:
    """Every rates CSV day in the clone's history."""
    raw = _run(
        repo,
        "log",
        "--format=C %H %cI",
        "--raw",
        "--no-abbrev",
        "--no-renames",
        "HEAD",
        "--",
        "CFTC_CUMULATIVE_RATES_*.csv",
    )
    return parse_raw_log(raw.decode())


def blob_id(data: bytes) -> str:
    """Git's object id for a blob with this content."""
    return hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()  # noqa: S324  # git object id


def prefetch(repo: Path, blobs: list[str], batch: int = 25) -> None:
    """Fetch missing blobs of a partial clone in batches, not one request per blob.

    One request for a few hundred large blobs sat for minutes with no bytes
    received; batches of 25 return in about 30 s each and show progress.
    """
    for start in range(0, len(blobs), batch):
        chunk = blobs[start : start + batch]
        _ = _run(
            repo,
            "-c",
            "fetch.negotiationAlgorithm=noop",
            "fetch",
            "origin",
            "--no-tags",
            "--no-write-fetch-head",
            # No --filter here: a blob:none filter would strip the very blobs asked for.
            "--stdin",
            stdin=("\n".join(chunk) + "\n").encode(),
        )
        log.info("backfill.prefetch", done=start + len(chunk), total=len(blobs))


def compare_overlap(files: list[GitFile], manifest: Manifest, archive: Path) -> Overlap:
    """Compare committed blobs with the archived ZIPs' CSVs on days held from DTCC."""
    result = Overlap()
    for item in files:
        held = manifest.get("cftc", item.day)
        if held is None or held.source != "dtcc-s3":
            continue
        zip_path = archive / held.file
        if not zip_path.exists():
            result.not_in_archive += 1
            continue
        with zipfile.ZipFile(zip_path) as zf:
            data = zf.read(zf.namelist()[0])
        result.compared += 1
        if blob_id(data) == item.blob:
            result.identical += 1
        else:
            result.differing.append(item.day.isoformat())
    return result


def backfill(
    repo: Path,
    manifest: Manifest,
    data_dir: Path,
    work_dir: Path,
    files: list[GitFile],
    *,
    failed: dict[str, str] | None = None,
) -> dict[str, Path]:
    """Convert committed CSVs for CFTC days not held; return the partitions written.

    A day whose CSV does not convert is left out and recorded in ``failed``
    (day -> reason) rather than stopping the other days.
    """
    todo = [f for f in files if manifest.get("cftc", f.day) is None]
    log.info("backfill.plan", days=len(todo))
    prefetch(repo, [f.blob for f in todo])
    work_dir.mkdir(parents=True, exist_ok=True)
    written: dict[str, Path] = {}
    for item in todo:
        csv_path = work_dir / item.path
        data = _run(repo, "cat-file", "blob", item.blob)
        _ = csv_path.write_bytes(data)
        partition = convert.partition_path("cftc", item.day)
        try:
            result = convert.convert_csv(csv_path, data_dir / partition)
        except ExternalServiceError as exc:
            if failed is not None:
                failed[item.day.isoformat()] = str(exc).splitlines()[-1][:200]
            log.warning("backfill.failed", day=str(item.day), error=str(exc)[:200])
            continue
        finally:
            csv_path.unlink(missing_ok=True)
        manifest.put(
            Entry(
                regulator="cftc",
                day=item.day,
                file=f"cftc/{item.day.isoformat()}/{item.path}",
                url=f"{REPO_URL}/blob/{item.commit}/{item.path}",
                bytes=len(data),
                sha256=hashlib.sha256(data).hexdigest(),
                rows=result.rows,
                partition=partition,
                source="xbowery-git",
                source_ref=item.commit,
                fetched_at=item.committed_at,
            )
        )
        written[partition] = data_dir / partition
        log.info("backfill.day", day=str(item.day), rows=result.rows)
    return written
