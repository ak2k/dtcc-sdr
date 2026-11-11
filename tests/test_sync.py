"""Incremental sync against a fake DTCC and a local store."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from typing import TYPE_CHECKING

import httpx

from dtcc_sdr import dtcc, sync
from dtcc_sdr.hf import LocalStore
from dtcc_sdr.manifest import MANIFEST_PATH
from tests.conftest import ROWS, make_zip

if TYPE_CHECKING:
    from pathlib import Path

NOW = dt.datetime(2026, 11, 11, 2, 0, tzinfo=dt.UTC)
DAYS = [dt.date(2026, 11, 8), dt.date(2026, 11, 9), dt.date(2026, 11, 10)]


class FakeDtcc:
    """PPD listing plus S3 objects, honoring If-None-Match; logs every request."""

    def __init__(self, tmp: Path) -> None:
        self.tmp = tmp
        self.objects: dict[str, bytes] = {}
        self.slices: dict[str, int] = {}
        self.listing_ok = True
        self.log: list[str] = []

    def put(self, regulator: str, day: dt.date, rows: tuple[str, ...], slice_id: int) -> None:
        name = dtcc.file_name(regulator, day)
        path = make_zip(self.tmp / "src" / f"{slice_id}" / name, rows)
        self.objects[dtcc.url_for(regulator, day)] = path.read_bytes()
        self.slices[dtcc.url_for(regulator, day)] = slice_id

    def handler(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        self.log.append(url)
        if request.url.host == "pddata.dtcc.com":
            if not self.listing_ok:
                return httpx.Response(500)
            reg = request.url.path.split("/")[-2].lower()
            items = [
                {
                    "sliceId": sid,
                    "fileName": u.rsplit("/", 1)[1],
                    "endTs": f"2026-11-11T00:15:{sid % 60:02d}Z",
                    "rowCount": len(ROWS),
                    "fullFilePath": u,
                }
                for u, sid in self.slices.items()
                if f"/{reg}/eod/" in u
            ]
            return httpx.Response(200, content=json.dumps(items))
        body = self.objects.get(url)
        if body is None:
            return httpx.Response(403, content=b"<Code>AccessDenied</Code>")
        etag = '"' + hashlib.md5(body).hexdigest() + '"'  # noqa: S324
        if request.headers.get("if-none-match") == etag:
            return httpx.Response(304)
        return httpx.Response(200, content=body, headers={"ETag": etag})

    def client(self) -> dtcc.Client:
        return dtcc.Client(1000.0, httpx.Client(transport=httpx.MockTransport(self.handler)))


def _run(fake: FakeDtcc, store: LocalStore, tmp: Path, **kw: object) -> sync.Summary:
    return sync.run(
        store,
        fake.client(),
        repo_id="me/ds",
        data_dir=tmp / "build",
        work_dir=tmp / "work",
        recheck_days=int(kw.get("recheck_days", 30)),  # pyright: ignore[reportArgumentType]
        regulators=["cftc"],
        probe=bool(kw.get("probe", False)),
        now=NOW,
    )


def _seeded(tmp: Path) -> tuple[FakeDtcc, LocalStore]:
    fake = FakeDtcc(tmp)
    for i, day in enumerate(DAYS):
        fake.put("cftc", day, ROWS, 100 + i)
    store = LocalStore(tmp / "ds")
    first = _run(fake, store, tmp)
    assert first.committed
    assert first.outcomes == {"fetched": 3}
    assert store.commits == 1
    return fake, store


def test_first_run_fetches_all_listed_in_one_commit(tmp_path: Path):
    _, store = _seeded(tmp_path)
    manifest = store.read_manifest()
    assert sorted(manifest.entries) == [f"cftc/{d.isoformat()}" for d in DAYS]
    assert all(e.rows == len(ROWS) and e.etag for e in manifest.entries.values())
    assert (tmp_path / "ds" / "README.md").exists()
    assert (tmp_path / "ds" / "rates/regulator=cftc/date=2026-11-10/data.parquet").exists()


def test_no_change_run_lists_only_and_does_not_commit(tmp_path: Path):
    fake, store = _seeded(tmp_path)
    fake.log.clear()
    summary = _run(fake, store, tmp_path)
    assert fake.log == [dtcc.LIST_URL.format(api_reg="CFTC", api_asset="IR")]
    assert summary.candidates == 0
    assert not summary.committed
    assert store.commits == 1


def test_new_day_fetches_only_that_file(tmp_path: Path):
    fake, store = _seeded(tmp_path)
    old = tmp_path / "ds" / "rates/regulator=cftc/date=2026-11-08/data.parquet"
    before = old.stat().st_mtime_ns
    new_day = dt.date(2026, 11, 10) - dt.timedelta(days=5)  # a gap inside the listing
    fake.put("cftc", new_day, ROWS, 200)
    fake.log.clear()
    summary = _run(fake, store, tmp_path)
    assert len(fake.log) == 2
    assert fake.log[1] == dtcc.url_for("cftc", new_day)
    assert summary.written == [f"rates/regulator=cftc/date={new_day.isoformat()}/data.parquet"]
    assert store.commits == 2
    assert old.stat().st_mtime_ns == before


def test_repost_in_window_replaces_only_that_partition(tmp_path: Path):
    fake, store = _seeded(tmp_path)
    keep = tmp_path / "ds" / "rates/regulator=cftc/date=2026-11-10/data.parquet"
    before = keep.stat().st_mtime_ns
    fake.put("cftc", DAYS[0], (*ROWS, ROWS[1].replace("0101,", "0102,", 1)), 300)
    summary = _run(fake, store, tmp_path)
    assert summary.written == ["rates/regulator=cftc/date=2026-11-08/data.parquet"]
    entry = store.read_manifest().get("cftc", DAYS[0])
    assert entry is not None
    assert (entry.slice_id, entry.rows) == (300, 3)
    assert keep.stat().st_mtime_ns == before


def test_repost_outside_window_is_ignored(tmp_path: Path):
    fake, store = _seeded(tmp_path)
    fake.put("cftc", DAYS[0], ROWS[:1], 301)
    fake.log.clear()
    summary = _run(fake, store, tmp_path, recheck_days=1)
    assert summary.candidates == 0
    assert len(fake.log) == 1
    assert not summary.committed


def test_new_slice_same_bytes_answers_304_and_records_slice(tmp_path: Path):
    fake, store = _seeded(tmp_path)
    url = dtcc.url_for("cftc", DAYS[1])
    fake.slices[url] = 400
    summary = _run(fake, store, tmp_path)
    assert summary.outcomes == {"not_modified": 1}
    assert summary.written == []
    assert summary.committed  # manifest-only commit records the new slice id
    entry = store.read_manifest().get("cftc", DAYS[1])
    assert entry is not None
    assert entry.slice_id == 400


def test_listing_failure_falls_back_to_conditional_probe(tmp_path: Path):
    fake, store = _seeded(tmp_path)
    fake.listing_ok = False
    fake.log.clear()
    summary = _run(fake, store, tmp_path, recheck_days=2)
    assert summary.mode == "probe"
    # 2026-11-08..11-10 held (304 each); nothing new; no commit.
    assert summary.outcomes == {"not_modified": 3}
    assert not summary.committed
    assert (tmp_path / "ds" / MANIFEST_PATH).exists()
