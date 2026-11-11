"""The state manifest: what the dataset holds, kept in the dataset itself.

A run reads this one file to learn what it already has, so it never lists or
downloads the Parquet tree. It lives under ``_state/``: the ``datasets`` loader
and the HF viewer ignore paths starting with ``_``, so it is not taken for data.
"""

from __future__ import annotations

import datetime as dt
from typing import Literal

from pydantic import BaseModel, ConfigDict

MANIFEST_PATH = "_state/manifest.json"
Source = Literal["dtcc-s3", "xbowery-git"]


class Entry(BaseModel):
    """One held source file and the partition built from it."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    regulator: str
    day: dt.date
    file: str
    url: str
    bytes: int
    sha256: str
    rows: int
    partition: str
    source: Source = "dtcc-s3"
    # S3 ETag (the MD5 of a single-part object), sent back as If-None-Match.
    etag: str | None = None
    last_modified: str | None = None
    # PPD listing fields; a new slice id for a held file means DTCC reposted it.
    slice_id: int | None = None
    end_ts: str | None = None
    listed_rows: int | None = None
    # Commit sha for a file recovered from a third-party git history.
    source_ref: str | None = None
    fetched_at: str

    @property
    def key(self) -> str:
        """``<regulator>/<day>``: one partition per key."""
        return f"{self.regulator}/{self.day.isoformat()}"


class Manifest(BaseModel):
    """All held entries, keyed by ``<regulator>/<day>``."""

    model_config = ConfigDict(extra="forbid")

    version: Literal[1] = 1
    entries: dict[str, Entry] = {}

    def put(self, entry: Entry) -> None:
        """Insert or replace the entry for ``entry.key``."""
        self.entries[entry.key] = entry

    def get(self, regulator: str, day: dt.date) -> Entry | None:
        """The held entry for one regulator and day, if any."""
        return self.entries.get(f"{regulator}/{day.isoformat()}")

    def newest(self, regulator: str) -> dt.date | None:
        """Latest day held for ``regulator``."""
        days = [e.day for e in self.entries.values() if e.regulator == regulator]
        return max(days) if days else None

    def to_json(self) -> str:
        """Deterministic JSON (sorted keys) so an unchanged manifest is byte-identical."""
        ordered = Manifest(entries=dict(sorted(self.entries.items())))
        return ordered.model_dump_json(indent=None, exclude_none=True) + "\n"
