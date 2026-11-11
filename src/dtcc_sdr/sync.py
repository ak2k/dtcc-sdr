"""Incremental sync: decide what to request, fetch it, commit only what changed.

A run reads the manifest, asks the PPD listing (one request per regulator)
what DTCC currently publishes, and requests a file only when it is not held,
or when it is held, falls inside the recheck window and the listing shows a
new slice for it. A held file is requested with ``If-None-Match`` so an
unchanged object answers 304 with no body. If the listing is unavailable the
run probes every day from the recheck start instead, still conditionally.

Only new or changed partitions are written, all in one commit with the
manifest and card; a run that changes nothing commits nothing.
"""

from __future__ import annotations

import datetime as dt
import hashlib
from typing import TYPE_CHECKING, Literal

import structlog
from pydantic import BaseModel, ConfigDict

from dtcc_sdr import card, convert, dtcc
from dtcc_sdr.errors import ExternalServiceError
from dtcc_sdr.manifest import Entry, Manifest

if TYPE_CHECKING:
    from pathlib import Path

    from structlog.stdlib import BoundLogger

    from dtcc_sdr.hf import Store

log: BoundLogger = structlog.get_logger(__name__)  # pyright: ignore[reportAny]


class Candidate(BaseModel):
    """One file to request."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    regulator: str
    day: dt.date
    url: str
    file: str
    held: Entry | None = None
    listed: dtcc.Listed | None = None


class Summary(BaseModel):
    """What a run did."""

    model_config = ConfigDict(extra="forbid")

    mode: Literal["listing", "probe"] = "listing"
    candidates: int = 0
    outcomes: dict[str, int] = {}
    written: list[str] = []
    committed: bool = False
    requests: int = 0
    bytes: int = 0
    row_mismatches: list[str] = []

    def count(self, outcome: str) -> None:
        """Tally one outcome."""
        self.outcomes[outcome] = self.outcomes.get(outcome, 0) + 1


def utc_stamp() -> str:
    """Now, ISO 8601 UTC to the second."""
    return dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _recheck_from(
    manifest: Manifest, regulator: str, recheck_days: int, window_start: dt.date
) -> dt.date:
    newest = manifest.newest(regulator)
    if newest is None:
        return window_start
    return max(window_start, newest - dt.timedelta(days=recheck_days))


def plan_from_listing(
    manifest: Manifest,
    listings: dict[str, list[dtcc.Listed]],
    recheck_days: int,
    window: tuple[dt.date, dt.date],
) -> list[Candidate]:
    """Candidates from the PPD listing: unheld files, and reposts in the recheck window."""
    out: list[Candidate] = []
    for regulator, listed in listings.items():
        recheck_from = _recheck_from(manifest, regulator, recheck_days, window[0])
        for item in listed:
            _, _, day = dtcc.parse_file_name(item.file_name)
            if not window[0] <= day <= window[1]:
                continue
            held = manifest.get(regulator, day)
            if held is not None and (day < recheck_from or held.slice_id == item.slice_id):
                continue
            out.append(
                Candidate(
                    regulator=regulator,
                    day=day,
                    url=item.url,
                    file=item.file_name,
                    held=held,
                    listed=item,
                )
            )
    return out


def plan_probe(
    manifest: Manifest,
    regulators: list[str],
    recheck_days: int,
    window: tuple[dt.date, dt.date],
) -> list[Candidate]:
    """Candidates without a listing: every day from the recheck start, per regulator."""
    out: list[Candidate] = []
    for regulator in regulators:
        start = _recheck_from(manifest, regulator, recheck_days, window[0])
        for day in dtcc.date_range(start, window[1]):
            out.append(
                Candidate(
                    regulator=regulator,
                    day=day,
                    url=dtcc.url_for(regulator, day),
                    file=dtcc.file_name(regulator, day),
                    held=manifest.get(regulator, day),
                )
            )
    return out


def _with_listing(entry: Entry, listed: dtcc.Listed | None) -> Entry:
    if listed is None:
        return entry
    return entry.model_copy(
        update={
            "slice_id": listed.slice_id,
            "end_ts": listed.end_ts,
            "listed_rows": listed.row_count,
        }
    )


def apply(
    cand: Candidate,
    fetched: dtcc.Fetched,
    manifest: Manifest,
    data_dir: Path,
    work_dir: Path,
    summary: Summary,
    *,
    memory_limit: str | None = None,
) -> str | None:
    """Fold one fetch result into ``manifest``; return the partition written, if any."""
    summary.count(fetched.outcome)
    if fetched.outcome in ("missing", "archived"):
        log.info("fetch.unavailable", regulator=cand.regulator, day=str(cand.day), outcome=fetched.outcome)
        return None
    if fetched.outcome == "not_modified":
        if cand.held is not None and cand.listed is not None:
            updated = _with_listing(cand.held, cand.listed)
            if updated != cand.held:
                manifest.put(updated)
        return None
    sha = hashlib.sha256(fetched.body).hexdigest()
    if cand.held is not None and cand.held.sha256 == sha:
        # Same bytes under new metadata: keep the partition, refresh the record.
        manifest.put(
            _with_listing(
                cand.held.model_copy(
                    update={"etag": fetched.etag, "last_modified": fetched.last_modified}
                ),
                cand.listed,
            )
        )
        summary.count("same_content")
        return None
    work_dir.mkdir(parents=True, exist_ok=True)
    zip_path = work_dir / cand.file
    _ = zip_path.write_bytes(fetched.body)
    partition = convert.partition_path(cand.regulator, cand.day)
    try:
        result = convert.convert_zip(zip_path, data_dir / partition, memory_limit=memory_limit)
    finally:
        zip_path.unlink(missing_ok=True)
    entry = _with_listing(
        Entry(
            regulator=cand.regulator,
            day=cand.day,
            file=f"{cand.regulator}/{cand.day.isoformat()}/{cand.file}",
            url=cand.url,
            bytes=len(fetched.body),
            sha256=sha,
            rows=result.rows,
            partition=partition,
            etag=fetched.etag,
            last_modified=fetched.last_modified,
            fetched_at=utc_stamp(),
        ),
        cand.listed,
    )
    if entry.listed_rows is not None and entry.listed_rows != entry.rows:
        summary.row_mismatches.append(f"{entry.key}: listed {entry.listed_rows}, parsed {entry.rows}")
        log.warning("convert.row_mismatch", key=entry.key, listed=entry.listed_rows, rows=entry.rows)
    manifest.put(entry)
    log.info("convert.done", key=entry.key, rows=entry.rows, replaced=cand.held is not None)
    return partition


def run(
    store: Store,
    client: dtcc.Client,
    *,
    repo_id: str,
    data_dir: Path,
    work_dir: Path,
    recheck_days: int,
    regulators: list[str],
    probe: bool = False,
    now: dt.datetime | None = None,
    memory_limit: str | None = None,
) -> Summary:
    """One incremental sync. Commits once if anything changed, else not at all."""
    manifest = store.read_manifest()
    before = manifest.to_json()
    window = dtcc.default_window(now)
    summary = Summary()
    candidates: list[Candidate] | None = None
    if not probe:
        try:
            listings = {reg: client.list_files(reg) for reg in regulators}
            candidates = plan_from_listing(manifest, listings, recheck_days, window)
        except ExternalServiceError as exc:
            log.warning("listing.unavailable", error=str(exc))
    if candidates is None:
        summary.mode = "probe"
        candidates = plan_probe(manifest, regulators, recheck_days, window)
    summary.candidates = len(candidates)
    log.info("sync.plan", mode=summary.mode, candidates=len(candidates))

    written: dict[str, Path] = {}
    for cand in candidates:
        etag = cand.held.etag if cand.held else None
        fetched = client.fetch(cand.url, etag)
        partition = apply(
            cand, fetched, manifest, data_dir, work_dir, summary, memory_limit=memory_limit
        )
        if partition:
            written[partition] = data_dir / partition

    summary.requests = client.traffic.requests
    summary.bytes = client.traffic.bytes
    summary.written = sorted(written)
    if manifest.to_json() == before:
        log.info("sync.nothing_to_commit")
        return summary
    readme = card.write_card(repo_id, manifest, data_dir)
    files = {**written, "README.md": readme}
    store.commit(files, manifest, f"sync: {len(written)} partitions ({summary.mode})")
    summary.committed = True
    return summary
