"""Conversion tests against small DTCC-shaped ZIPs."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import TYPE_CHECKING

import duckdb
import pytest

from dtcc_sdr import convert
from dtcc_sdr.errors import ExternalServiceError
from tests.conftest import HEADER, make_zip

if TYPE_CHECKING:
    from pathlib import Path


def _read(path: Path, sql: str) -> list[tuple[object, ...]]:
    con = duckdb.connect()
    try:
        return con.execute(sql.format(path=path)).fetchall()
    finally:
        con.close()


def test_types_capped_flag_and_rows(tmp_path: Path):
    zip_path = make_zip(tmp_path / "CFTC_CUMULATIVE_RATES_2026_11_09.zip")
    out = tmp_path / "ds" / convert.partition_path("cftc", dt.date(2026, 11, 9))
    result = convert.convert_zip(zip_path, out)
    assert result.rows == 2
    assert out.name == "data.parquet"
    assert "regulator=cftc/date=2026-11-09" in str(out)

    types = dict(_read(out, "SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM '{path}')"))
    assert types["dissemination_identifier"] == "BIGINT"
    assert types["event_timestamp"] == "TIMESTAMP WITH TIME ZONE"
    assert types["amendment_indicator"] == "BOOLEAN"
    assert types["effective_date"] == "DATE"
    assert types["notional_amount_leg_1"] == "DECIMAL(38,12)"
    assert types["notional_amount_leg_1_capped"] == "BOOLEAN"
    assert types["spread_notation_leg_1"] == "INTEGER"
    # Multi-valued and unknown columns stay text.
    assert types["other_payment_amount"] == "VARCHAR"
    assert types["brand_new_column"] == "VARCHAR"

    rows = _read(
        out,
        "SELECT notional_amount_leg_1, notional_amount_leg_1_capped, amendment_indicator, "
        "fixed_rate_leg_1, other_payment_amount, effective_date FROM '{path}' "
        "ORDER BY dissemination_identifier DESC",
    )
    assert rows[0] == (
        Decimal("160000000"),
        True,
        False,
        Decimal("0.04397"),
        "5260.00000;10.5",
        dt.date(2027, 6, 30),
    )
    assert rows[1] == (Decimal("32000000"), False, True, None, None, dt.date(2026, 9, 30))


def test_bad_value_fails_instead_of_nulling(tmp_path: Path):
    bad = ("1,NEWT,not-a-time,FALSE,2027-06-30,1,0.1,,3,x",)
    zip_path = make_zip(tmp_path / "SEC_CUMULATIVE_RATES_2026_11_09.zip", bad)
    out = tmp_path / "out" / "data.parquet"
    with pytest.raises(ExternalServiceError):
        _ = convert.convert_zip(zip_path, out)
    assert not out.exists()
    assert not list(out.parent.glob(".tmp-*"))


def test_trailing_plus_outside_capped_columns_fails(tmp_path: Path):
    bad = ("1,NEWT,2026-11-09T22:51:43Z,FALSE,2027-06-30,1,0.1+,,3,x",)
    zip_path = make_zip(tmp_path / "CA_CUMULATIVE_RATES_2026_11_09.zip", bad)
    with pytest.raises(ExternalServiceError):
        _ = convert.convert_zip(zip_path, tmp_path / "o" / "data.parquet")


def test_header_collision_rejected(tmp_path: Path):
    header = HEADER + ",Action Type"
    zip_path = make_zip(tmp_path / "CA_CUMULATIVE_RATES_2026_11_09.zip", (), header)
    with pytest.raises(ExternalServiceError, match="action_type"):
        _ = convert.convert_zip(zip_path, tmp_path / "o" / "data.parquet")


def test_known_columns_have_distinct_names():
    names = [convert.column_name(c) for c in convert.COLUMN_TYPES]
    assert len(names) == len(set(names))
    assert convert.column_name("Strike price currency/currency pair") == (
        "strike_price_currency_currency_pair"
    )


def test_whitespace_padding_is_trimmed_before_casts(tmp_path: Path):
    # Canadian files pad some values: "25,000,000 " and a lone " " indicator.
    padded = ('1,NEWT,2026-11-09T22:51:43Z, ,2027-06-30,"25,000,000 ",0.1,,3,x',)
    zip_path = make_zip(tmp_path / "CA_CUMULATIVE_RATES_2025_11_01.zip", padded)
    out = tmp_path / "o" / "data.parquet"
    assert convert.convert_zip(zip_path, out).rows == 1
    assert _read(
        out,
        "SELECT amendment_indicator, notional_amount_leg_1, notional_amount_leg_1_capped "
        "FROM '{path}'",
    ) == [(None, Decimal("25000000"), False)]
