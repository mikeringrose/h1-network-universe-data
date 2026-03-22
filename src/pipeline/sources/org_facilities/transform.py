"""Transform parsed org_facility DataFrame into org_facilities table shape."""

from __future__ import annotations

import uuid

import polars as pl

# All columns that may appear in the output table (excluding generated/server-managed ones)
_ALL_OUTPUT_COLUMNS = [
    "id",
    "organization_id",
    "source_file_id",
    "ssa_state_county_code",
    "facility_service_type",
    "facility_specialty_code",
    "npi",
    "staffed_beds",
    "facility_name",
    "street_address",
    "city",
    "state_code",
    "zip_code",
    "latitude",
    "longitude",
]


def _str_5(s: str) -> str:
    s = str(s).strip()
    if s.isdigit() and len(s) <= 5:
        return s.zfill(5)
    return s


def transform(df: pl.DataFrame, organization_id: str, source_file_id: str) -> pl.DataFrame:
    """Add org context, generate UUIDs, cast types. Returns rows ready for INSERT."""
    if df.is_empty():
        return pl.DataFrame(schema={
            "id": pl.Utf8,
            "organization_id": pl.Utf8,
            "source_file_id": pl.Utf8,
            "ssa_state_county_code": pl.Utf8,
            "facility_service_type": pl.Utf8,
            "facility_specialty_code": pl.Utf8,
            "npi": pl.Utf8,
            "staffed_beds": pl.Int64,
            "facility_name": pl.Utf8,
            "street_address": pl.Utf8,
            "city": pl.Utf8,
            "state_code": pl.Utf8,
            "zip_code": pl.Utf8,
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

    if "staffed_beds" in out.columns:
        out = out.with_columns(
            pl.col("staffed_beds").cast(pl.Int64, strict=False)
        )

    # Strip whitespace from string columns (except those already handled)
    skip = {"id", "organization_id", "source_file_id", "ssa_state_county_code", "staffed_beds", "latitude", "longitude"}
    for col in out.columns:
        if col not in skip and out.schema[col] == pl.Utf8:
            out = out.with_columns(pl.col(col).str.strip_chars())

    # Fill any missing optional columns with NULL so the INSERT always has all 15 columns
    _NULL_DTYPE: dict[str, pl.PolarsDataType] = {"staffed_beds": pl.Int64, "latitude": pl.Float64, "longitude": pl.Float64}
    for col in _ALL_OUTPUT_COLUMNS:
        if col not in out.columns:
            dtype = _NULL_DTYPE.get(col, pl.Utf8)
            out = out.with_columns(pl.lit(None).cast(dtype).alias(col))

    return out.select(_ALL_OUTPUT_COLUMNS)
