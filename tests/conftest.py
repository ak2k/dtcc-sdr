"""Shared test fixtures."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import stamina

if TYPE_CHECKING:
    from collections.abc import Iterator


# Used by pytest via autouse; basedpyright can't see that wiring.
@pytest.fixture(autouse=True)
def _instant_retries() -> Iterator[None]:  # pyright: ignore[reportUnusedFunction]
    """Make stamina retries instant in tests — no real backoff sleeps."""
    stamina.set_testing(True, attempts=5)  # retry (capped), but with no waits
    yield
    stamina.set_testing(False)
