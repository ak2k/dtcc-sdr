"""Command-line entry point (typer).

dtcc-sdr sync                    # fetch new or reposted files, one HF commit
dtcc-sdr sync --local DIR        # same, against a local dataset directory
dtcc-sdr sync --probe            # skip the PPD listing; probe days conditionally
dtcc-sdr seed --archive DIR      # one-off: convert + upload a local ZIP archive
dtcc-sdr backfill-git --repo GIT  # one-off: CFTC days from xbowery/SDR-parser history
dtcc-sdr convert ZIP...          # convert ZIPs locally and print row counts
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Annotated

import structlog
import typer

from dtcc_sdr import card, convert, dtcc, gitbackfill
from dtcc_sdr import seed as seed_mod
from dtcc_sdr import sync as sync_mod
from dtcc_sdr.config import get_settings
from dtcc_sdr.errors import ExternalServiceError
from dtcc_sdr.hf import HfStore, LocalStore, Store
from dtcc_sdr.log import configure

if TYPE_CHECKING:
    from structlog.stdlib import BoundLogger

log: BoundLogger = structlog.get_logger("dtcc_sdr")  # pyright: ignore[reportAny]

app = typer.Typer(add_completion=False, help="Mirror DTCC public swap data to HuggingFace.")

_ALL = sorted(dtcc.REGULATORS)


def _listings(client: dtcc.Client, regulators: list[str]) -> dict[str, list[dtcc.Listed]]:
    out: dict[str, list[dtcc.Listed]] = {}
    for reg in regulators:
        try:
            out[reg] = client.list_files(reg)
        except ExternalServiceError as exc:
            log.warning("listing.unavailable", regulator=reg, error=str(exc))
    return out


@app.command()
def sync(
    local: Annotated[
        Path | None, typer.Option(help="Use this directory as the dataset instead of HF.")
    ] = None,
    probe: Annotated[bool, typer.Option(help="Probe each day instead of the listing.")] = False,
    recheck_days: Annotated[
        int | None, typer.Option(help="Override RECHECK_DAYS (default 30).")
    ] = None,
    regulator: Annotated[
        list[str] | None, typer.Option(help="Repeatable; default cftc, sec, ca.")
    ] = None,
) -> None:
    """Fetch files not yet held or reposted, and commit them once."""
    settings = get_settings()
    configure(settings.log_level, settings.log_json)
    store: Store
    if local is not None:
        store = LocalStore(local)
    else:
        hf_store = HfStore(settings.hf_repo_id, settings.hf_token)
        hf_store.ensure_repo()
        store = hf_store
    summary = sync_mod.run(
        store,
        dtcc.Client(settings.per_second),
        repo_id=settings.hf_repo_id,
        data_dir=settings.data_dir,
        work_dir=settings.work_dir,
        recheck_days=settings.recheck_days if recheck_days is None else recheck_days,
        regulators=regulator or _ALL,
        probe=probe,
        memory_limit=settings.memory_limit,
    )
    typer.echo(summary.model_dump_json())


@app.command()
def seed(
    archive: Annotated[Path, typer.Option(help="Local archive root (has manifest.csv).")],
    local: Annotated[
        Path | None, typer.Option(help="Write the dataset here instead of uploading.")
    ] = None,
    workers: Annotated[int | None, typer.Option(help="Conversion processes.")] = None,
    listing: Annotated[bool, typer.Option(help="Record PPD listing slice ids.")] = True,
) -> None:
    """Convert every file in a local archive and upload it with a manifest."""
    settings = get_settings()
    configure(settings.log_level, settings.log_json)
    listings = _listings(dtcc.Client(settings.per_second), _ALL) if listing else None
    data_dir = local if local is not None else settings.data_dir
    manifest = seed_mod.build(archive, data_dir, workers=workers, listings=listings)
    readme = card.write_card(settings.hf_repo_id, manifest, data_dir)
    if local is not None:
        LocalStore(local).commit({"README.md": readme}, manifest, "seed")
    else:
        hf_store = HfStore(settings.hf_repo_id, settings.hf_token)
        hf_store.ensure_repo()
        hf_store.upload_tree(data_dir)
        hf_store.commit({"README.md": readme}, manifest, "seed: manifest and card")
    rows = sum(e.rows for e in manifest.entries.values())
    typer.echo(json.dumps({"entries": len(manifest.entries), "rows": rows}))


@app.command("backfill-git")
def backfill_git(
    repo: Annotated[Path, typer.Option(help="Blobless bare clone of xbowery/SDR-parser.")],
    archive: Annotated[
        Path | None, typer.Option(help="Local ZIP archive, to check agreement on shared days.")
    ] = None,
    local: Annotated[
        Path | None, typer.Option(help="Use this directory as the dataset instead of HF.")
    ] = None,
) -> None:
    """One-off: add CFTC rates days from that repo's history that the dataset lacks."""
    settings = get_settings()
    configure(settings.log_level, settings.log_json)
    store: Store = LocalStore(local) if local else HfStore(settings.hf_repo_id, settings.hf_token)
    manifest = store.read_manifest()
    files = gitbackfill.list_files(repo)
    overlap = gitbackfill.compare_overlap(files, manifest, archive) if archive else None
    written = gitbackfill.backfill(repo, manifest, settings.data_dir, settings.work_dir, files)
    if written:
        readme = card.write_card(settings.hf_repo_id, manifest, settings.data_dir)
        store.commit(
            {**written, "README.md": readme},
            manifest,
            f"backfill: {len(written)} CFTC days from git",
        )
    days = sorted(p.split("date=")[1][:10] for p in written)
    typer.echo(
        json.dumps(
            {
                "git_days": len(files),
                "recovered": len(written),
                "first": days[0] if days else None,
                "last": days[-1] if days else None,
                "recovered_days": days,
                "overlap": overlap.model_dump() if overlap else None,
            }
        )
    )


@app.command("convert")
def convert_cmd(
    zips: Annotated[list[Path], typer.Argument(help="DTCC cumulative ZIPs.")],
    out: Annotated[Path, typer.Option(help="Output dataset directory.")] = Path("data"),
) -> None:
    """Convert ZIPs to partitions and print one JSON line of row counts per file."""
    settings = get_settings()
    configure(settings.log_level, settings.log_json)
    for path in zips:
        regulator, _, day = dtcc.parse_file_name(path.name)
        partition = convert.partition_path(regulator, day)
        result = convert.convert_zip(path, out / partition, memory_limit=settings.memory_limit)
        typer.echo(json.dumps({"file": path.name, "partition": partition, "rows": result.rows}))


def main() -> None:
    """Console-script entry point."""
    app()


if __name__ == "__main__":
    main()
