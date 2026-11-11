"""Generate the HuggingFace dataset card (README.md) from the manifest.

The card is built from the manifest, not the local tree, so a daily run that
fetched one file still writes a card that describes the whole dataset.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dtcc_sdr.convert import COLUMN_TYPES, column_name

if TYPE_CHECKING:
    from pathlib import Path

    from dtcc_sdr.manifest import Manifest

_REGULATOR_LABEL = {
    "cftc": "CFTC (U.S., CFTC Part 43 real-time public reporting)",
    "sec": "SEC (U.S., Regulation SBSR public dissemination)",
    "ca": "CA (Canadian provincial regulators' public dissemination)",
}

_BODY = """\
# DTCC public swap data: interest-rate cumulative files

Every end-of-day **cumulative interest-rate file** that the DTCC Data Repository
publicly disseminates for three regulatory regimes, mirrored as typed,
partitioned Parquet that DuckDB can query in place.

- **Source**: DTCC Public Price Dissemination (PPD),
  <https://pddata.dtcc.com/ppd/>. The files are the public dissemination that
  DTCC makes as a swap data repository under CFTC Part 43 (U.S. swaps), the SEC's
  Regulation SBSR (U.S. security-based swaps) and the Canadian provincial trade
  reporting rules. Each file is one regulator's interest-rate trade, amendment
  and cancellation messages for one day.
- **Window**: DTCC serves each file for about two years, then moves it to
  archive storage and stops serving it. This dataset keeps every file it fetched
  while it was served, so it reaches further back than DTCC's rolling free
  window.
- **Coverage**: {coverage}
- **Built by**: <https://github.com/ak2k/dtcc-sdr>, a daily GitHub Action that
  fetches only files that are new or that DTCC reposted.

## Dataset structure

```
rates/regulator=<cftc|sec|ca>/date=YYYY-MM-DD/data.parquet
```

One partition per regulator and dissemination day, so a filter on `regulator`
or `date` reads only those files. Column names are the DTCC field names in
snake case (`Notional amount-Leg 1` is `notional_amount_leg_1`).

Types: identifiers `BIGINT`; timestamps `TIMESTAMPTZ` (UTC); dates `DATE`;
indicators `BOOLEAN`; amounts, rates and prices `DECIMAL(38,12)`. A notional
above the public dissemination cap is published as e.g. `160,000,000+`: the
number is in the column and `<column>_capped` is true. Fields that carry a
`;`-separated schedule, and every field not listed below, are text exactly as
published.

Typed columns:

{typed}

`_state/manifest.json` records, for each held file, its source URL, S3 ETag,
size, SHA-256, row count and partition path.

## Usage

```sql
SELECT date, count(*) AS messages, sum(notional_amount_leg_1) AS notional
FROM 'hf://datasets/{repo_id}/rates/regulator=cftc/**/*.parquet'
WHERE notional_currency_leg_1 = 'USD' AND action_type = 'NEWT'
GROUP BY date ORDER BY date;
```

```python
from datasets import load_dataset

ds = load_dataset("{repo_id}", "rates", split="train", streaming=True)
```

## Terms, provenance and caveats

The data is DTCC's public dissemination, mirrored unmodified apart from typing.
Use of it is subject to DTCC's terms of use for the PPD site and data
(<https://www.dtcc.com/terms-of-use>); this mirror does not grant any right that
those terms do not. Values are as disseminated, including later corrections
and cancellations as separate messages; notionals above the cap are capped.

This dataset is provided as is, with **no warranty** of any kind, express or
implied, including accuracy, completeness, timeliness or fitness for a
purpose. It is not affiliated with or endorsed by DTCC, the CFTC, the SEC or
any Canadian regulator, and it is not investment advice. DTCC's site is the
authoritative source.
"""


def _coverage(manifest: Manifest) -> str:
    parts: list[str] = []
    for reg in ("cftc", "sec", "ca"):
        days = sorted(e.day for e in manifest.entries.values() if e.regulator == reg)
        if days:
            parts.append(f"{reg.upper()} {days[0]} to {days[-1]} ({len(days)} files)")
    if not parts:
        return "refreshed daily"
    return "; ".join(parts) + "; refreshed daily"


def _typed() -> str:
    return "\n".join(
        f"- `{column_name(src)}`: {kind}" for src, kind in sorted(COLUMN_TYPES.items())
    )


def _frontmatter() -> str:
    regulators = "\n".join(f"  # {label}" for label in _REGULATOR_LABEL.values())
    return f"""---
license: other
license_name: dtcc-terms-of-use
license_link: https://www.dtcc.com/terms-of-use
pretty_name: DTCC public swap data (interest-rate cumulative files)
tags:
- finance
- swaps
- interest-rates
- derivatives
configs:
- config_name: rates
  data_files:
  - split: train
    path: rates/**/*.parquet
# regulators:
{regulators}
---
"""


def render(repo_id: str, manifest: Manifest) -> str:
    """The card text."""
    body = _BODY.format(repo_id=repo_id, coverage=_coverage(manifest), typed=_typed())
    return _frontmatter() + "\n" + body


def write_card(repo_id: str, manifest: Manifest, out_dir: Path) -> Path:
    """Write ``README.md`` under ``out_dir``."""
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "README.md"
    _ = path.write_text(render(repo_id, manifest), encoding="utf-8")
    return path
