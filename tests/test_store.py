"""Manifest, stores, card and seed."""

from __future__ import annotations

import csv
import datetime as dt
import hashlib
from typing import TYPE_CHECKING

import httpx
import pytest
from huggingface_hub.errors import RemoteEntryNotFoundError, RepositoryNotFoundError

from dtcc_sdr import card, hf, seed
from dtcc_sdr.errors import ExternalServiceError
from dtcc_sdr.manifest import Entry, Manifest
from tests.conftest import ROWS, make_zip

if TYPE_CHECKING:
    from pathlib import Path


def _entry(day: dt.date, reg: str = "cftc") -> Entry:
    return Entry(
        regulator=reg,
        day=day,
        file=f"{reg}/{day}/X.zip",
        url="u",
        bytes=1,
        sha256="s",
        rows=2,
        partition=f"rates/regulator={reg}/date={day}/data.parquet",
        fetched_at="t",
    )


def test_manifest_json_is_deterministic_and_roundtrips():
    a, b = Manifest(), Manifest()
    for day in (dt.date(2026, 1, 2), dt.date(2026, 1, 1)):
        a.put(_entry(day))
    for day in (dt.date(2026, 1, 1), dt.date(2026, 1, 2)):
        b.put(_entry(day))
    assert a.to_json() == b.to_json()
    assert Manifest.model_validate_json(a.to_json()) == b
    assert a.newest("cftc") == dt.date(2026, 1, 2)
    assert a.newest("sec") is None


def _hf_error(cls: type[Exception]) -> Exception:
    response = httpx.Response(404, request=httpx.Request("GET", "https://hf.test"))
    return cls("nope", response=response)  # pyright: ignore[reportCallIssue]


def test_hf_missing_repo_is_empty(monkeypatch: pytest.MonkeyPatch):
    def raise_missing(*_a: object, **_k: object) -> str:
        raise _hf_error(RepositoryNotFoundError)

    monkeypatch.setattr(hf, "hf_hub_download", raise_missing)
    assert hf.HfStore("me/ds", None).read_manifest().entries == {}


def test_hf_repo_without_manifest_raises(monkeypatch: pytest.MonkeyPatch):
    def raise_entry(*_a: object, **_k: object) -> str:
        raise _hf_error(RemoteEntryNotFoundError)

    monkeypatch.setattr(hf, "hf_hub_download", raise_entry)
    with pytest.raises(ExternalServiceError, match="seed"):
        _ = hf.HfStore("me/ds", None).read_manifest()


def test_card_states_source_window_and_no_warranty():
    m = Manifest()
    m.put(_entry(dt.date(2024, 11, 11)))
    m.put(_entry(dt.date(2026, 11, 10)))
    text = card.render("me/ds", m)
    assert "CFTC Part 43" in text
    assert "Regulation SBSR" in text
    assert "Canadian" in text
    assert "about two years" in text
    assert "no warranty" in text
    assert "CFTC 2024-11-11 to 2026-11-10 (2 files)" in text
    assert "hf://datasets/me/ds/rates/" in text


def _archive(root: Path) -> list[Path]:
    paths = [
        make_zip(root / "cftc" / "2026-11-09" / "CFTC_CUMULATIVE_RATES_2026_11_09.zip"),
        make_zip(root / "sec" / "2026-11-09" / "SEC_CUMULATIVE_RATES_2026_11_09.zip", ROWS[:1]),
    ]
    with (root / "manifest.csv").open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["file", "url", "bytes", "sha256", "fetched_at"])
        for p in paths:
            body = p.read_bytes()
            rel = p.relative_to(root).as_posix()
            writer.writerow(
                [rel, f"https://x/{p.name}", len(body), hashlib.sha256(body).hexdigest(), "t"]
            )
    return paths


def test_seed_converts_every_archived_file(tmp_path: Path):
    paths = _archive(tmp_path / "arc")
    manifest = seed.build(tmp_path / "arc", tmp_path / "ds", workers=2)
    assert sorted(manifest.entries) == ["cftc/2026-11-09", "sec/2026-11-09"]
    cftc = manifest.get("cftc", dt.date(2026, 11, 9))
    assert cftc is not None
    assert cftc.rows == 2
    assert cftc.etag == '"' + hashlib.md5(paths[0].read_bytes()).hexdigest() + '"'  # noqa: S324
    assert (tmp_path / "ds" / cftc.partition).exists()


def test_seed_fails_on_a_changed_file(tmp_path: Path):
    paths = _archive(tmp_path / "arc")
    make_zip(paths[1], ROWS)  # bytes no longer match manifest.csv
    with pytest.raises(ExternalServiceError, match="1 archive files failed"):
        _ = seed.build(tmp_path / "arc", tmp_path / "ds", workers=1)
