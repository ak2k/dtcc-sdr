# dtcc-sdr

Mirror the DTCC Data Repository's public end-of-day **cumulative
interest-rate files** (CFTC Part 43, SEC Regulation SBSR and Canadian public
dissemination) to a HuggingFace dataset as typed, partitioned Parquet.

DTCC serves each file from S3 for about two years and then archives it. A
daily run keeps what it fetched while it was served.

- **Incremental and low-load.** The dataset holds `_state/manifest.json` (file,
  URL, ETag, size, SHA-256, rows, partition). A run reads only that file, then
  one PPD listing request per regulator (`pddata.dtcc.com/ppd/api/cumulative/<REG>/IR`,
  the last 366 days with slice ids and row counts). It requests a file only if
  it is not held, or if it is held, is inside the recheck window (default 30
  days before the newest held day, `RECHECK_DAYS`) and the listing shows a new
  slice. Held files are requested with `If-None-Match`, so an unchanged object
  answers 304 with no body. If the listing fails, the run probes each day from
  the recheck start, still conditionally (`--probe` forces this).
- **One commit per run.** New or reposted partitions, the manifest and the card
  go up in a single `create_commit`. Unchanged partitions are never rewritten;
  a run that changes nothing commits nothing.
- **Typed.** Column types are in `convert.COLUMN_TYPES`; casts are strict, so a
  value that does not parse fails the run rather than becoming NULL. Capped
  notionals (`160,000,000+`) keep the number and set `<column>_capped`.

## Layout of the dataset

```
rates/regulator=<cftc|sec|ca>/date=YYYY-MM-DD/data.parquet
_state/manifest.json
README.md
```

## Usage

```bash
uv sync
export HF_TOKEN=...                 # for upload
export HF_REPO_ID="<user>/dtcc-sdr"

uv run dtcc-sdr sync                          # daily incremental run
uv run dtcc-sdr sync --local ./ds             # same, against a local directory
uv run dtcc-sdr sync --probe                  # skip the listing
uv run dtcc-sdr seed --archive /path/to/dtcc-sdr          # one-off seed + upload
uv run dtcc-sdr seed --archive /path/to/dtcc-sdr --local ./ds   # seed locally only
uv run dtcc-sdr convert FILE.zip... --out ./ds  # convert and print row counts
```

The seed archive is `<root>/<reg>/<YYYY-MM-DD>/<FILE>.zip` plus
`<root>/manifest.csv` (`file,url,bytes,sha256,fetched_at`).

## Develop

```bash
make check    # ruff + basedpyright strict + pytest
make fix      # autofix + format, then check
nix develop   # optional: uv + make in a pinned shell
```

## CI

`.github/workflows/sync.yml` runs daily at 02:00 UTC (DTCC writes the prior
day's files at about 00:15 UTC) and on dispatch. Configure:

- var `HF_REPO_ID`: the target dataset, e.g. `trader298/dtcc-sdr`.
- secret `HF_TOKEN`: HuggingFace write token.

## Layout of the code

| Path | |
|------|--|
| `src/dtcc_sdr/dtcc.py` | URLs, PPD listing, conditional fetch, pacing |
| `src/dtcc_sdr/convert.py` | typed Parquet conversion (DuckDB) |
| `src/dtcc_sdr/manifest.py` | the state manifest kept in the dataset |
| `src/dtcc_sdr/sync.py` | incremental plan, fetch, single commit |
| `src/dtcc_sdr/seed.py` | one-off conversion of a local archive |
| `src/dtcc_sdr/hf.py` | HuggingFace and local stores |
| `src/dtcc_sdr/card.py` | dataset card from the manifest |
| `src/dtcc_sdr/cli.py` | `dtcc-sdr` CLI (typer) |

The data is DTCC's public dissemination, subject to DTCC's terms of use. This
is an unofficial mirror, provided with no warranty.
