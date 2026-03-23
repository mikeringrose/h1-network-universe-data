"""Transform parsed org_facility DataFrame into org_facilities table shape."""

from __future__ import annotations

import uuid

import polars as pl

_ALL_OUTPUT_COLUMNS = [
    "id",
    "organization_id",
    "source_file_id",
    "ssa_state_county_code",
    "specialty",
    "facility_specialty_code",
    "npi",
    "number_of_beds",
    "facility_name",
    "street_address",
    "city",
    "state_code",
    "zip_code",
    "letter_of_intent",
    "accuracy_confidence",
    "latitude",
    "longitude",
]

_BOOL_COLUMNS = {"letter_of_intent"}


def _str_5(s: str) -> str:
    s = str(s).strip()
    if s.isdigit() and len(s) <= 5:
        return s.zfill(5)
    return s


def _yn_to_bool(val) -> bool | None:
    if val is None:
        return None
    v = str(val).strip().upper()
    if v in ("Y", "YES", "TRUE", "1"):
        return True
    if v in ("N", "NO", "FALSE", "0"):
        return False
    return None


def transform(df: pl.DataFrame, organization_id: str, source_file_id: str) -> pl.DataFrame:
    """Add org context, generate UUIDs, cast types. Returns rows ready for INSERT."""
    if df.is_empty():
        return pl.DataFrame(schema={
            "id": pl.Utf8,
            "organization_id": pl.Utf8,
            "source_file_id": pl.Utf8,
            "ssa_state_county_code": pl.Utf8,
            "specialty": pl.Utf8,
            "facility_specialty_code": pl.Utf8,
            "npi": pl.Utf8,
            "number_of_beds": pl.Int64,
            "facility_name": pl.Utf8,
            "street_address": pl.Utf8,
            "city": pl.Utf8,
            "state_code": pl.Utf8,
            "zip_code": pl.Utf8,
            "letter_of_intent": pl.Boolean,
            "accuracy_confidence": pl.Utf8,
            "latitude": pl.Float64,
            "longitude": pl.Float64,
        })

    out = df.with_columns(
        pl.Series("id", [str(uuid.uuid4()) for _ in range(len(df))]),
        pl.lit(organization_id).alias("organization_id"),
        pl.lit(source_file_id).alias("source_file_id"),
        pl.lit(None).cast(pl.Float64).alias("latitude"),
        pl.lit(None).cast(pl.Float64).alias("longitude"),
    )

    if "ssa_state_county_code" in out.columns:
        out = out.with_columns(
            pl.col("ssa_state_county_code").map_elements(_str_5, return_dtype=pl.Utf8)
        )

    if "number_of_beds" in out.columns:
        out = out.with_columns(
            pl.col("number_of_beds").cast(pl.Int64, strict=False)
        )

    for col in _BOOL_COLUMNS:
        if col in out.columns:
            out = out.with_columns(
                pl.col(col).map_elements(_yn_to_bool, return_dtype=pl.Boolean)
            )

    skip = {"id", "organization_id", "source_file_id", "ssa_state_county_code", "number_of_beds", "letter_of_intent", "latitude", "longitude"}
    for col in out.columns:
        if col not in skip and out.schema[col] == pl.Utf8:
            out = out.with_columns(pl.col(col).str.strip_chars())

    _NULL_DTYPE: dict[str, pl.PolarsDataType] = {
        "number_of_beds": pl.Int64,
        "letter_of_intent": pl.Boolean,
        "latitude": pl.Float64,
        "longitude": pl.Float64,
    }
    for col in _ALL_OUTPUT_COLUMNS:
        if col not in out.columns:
            dtype = _NULL_DTYPE.get(col, pl.Utf8)
            out = out.with_columns(pl.lit(None).cast(dtype).alias(col))

    return out.select(_ALL_OUTPUT_COLUMNS)
