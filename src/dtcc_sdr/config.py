"""Typed configuration sourced from the environment.

`extra="forbid"` + `frozen=True` so stray env vars and mid-run mutation fail
loudly. Field names map to upper-case env vars (case-insensitive): `hf_token`
reads `HF_TOKEN`, `recheck_days` reads `RECHECK_DAYS`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

LogLevel = Literal["debug", "info", "warning", "error"]


class Settings(BaseSettings):
    """Application settings sourced from environment + `.env` file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="forbid",
        frozen=True,
    )

    hf_token: str | None = Field(
        default=None,
        description="HuggingFace write token (only needed for upload).",
    )
    hf_repo_id: str = Field(
        default="trader298/dtcc-sdr",
        description="Target HuggingFace dataset repo id.",
    )
    data_dir: Path = Field(
        default=Path("data"),
        description="Output directory for the partitioned Parquet tree.",
    )
    work_dir: Path = Field(
        default=Path(".work"),
        description="Scratch directory for downloaded ZIPs.",
    )
    recheck_days: int = Field(
        default=30,
        ge=0,
        description=(
            "Days before the newest held file that are re-checked for a corrected "
            "repost. DTCC replaces cumulative files in place."
        ),
    )
    per_second: float = Field(
        default=2.0,
        gt=0,
        description="Cap on request starts per second to DTCC.",
    )
    memory_limit: str | None = Field(
        default=None,
        description="DuckDB memory limit, e.g. '4GB' (default: unbounded).",
    )
    log_level: LogLevel = Field(default="info", description="Application log level.")
    log_json: bool = Field(default=False, description="Emit JSON logs (production).")


def get_settings() -> Settings:
    """Construct Settings; BaseSettings populates fields from the environment."""
    return Settings()
