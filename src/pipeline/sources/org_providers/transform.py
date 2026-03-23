"""Transform parsed org_provider DataFrame into org_providers table shape."""

from __future__ import annotations

import uuid

import polars as pl

_ALL_OUTPUT_COLUMNS = [
    "id",
    "organization_id",
    "source_file_id",
    "ssa_state_county_code",
    "provider_name",
    "npi",
    "provider_specialty_code",
    "contract_type",
    "street_address",
    "city",
    "state_code",
    "zip_code",
    "latitude",
    "longitude",
    "medical_group_affiliation",
    "specialty",
    "accepts_new_patients",
    "uses_cms_ma_contract_amendment",
    "letter_of_intent",
    "accuracy_confidence",
]

_BOOL_COLUMNS = {"accepts_new_patients", "uses_cms_ma_contract_amendment", "letter_of_intent"}


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
            "provider_name": pl.Utf8,
            "npi": pl.Utf8,
            "provider_specialty_code": pl.Utf8,
            "contract_type": pl.Utf8,
            "street_address": pl.Utf8,
            "city": pl.Utf8,
            "state_code": pl.Utf8,
            "zip_code": pl.Utf8,
            "latitude": pl.Float64,
            "longitude": pl.Float64,
            "medical_group_affiliation": pl.Utf8,
            "specialty": pl.Utf8,
            "accepts_new_patients": pl.Boolean,
            "uses_cms_ma_contract_amendment": pl.Boolean,
            "letter_of_intent": pl.Boolean,
            "accuracy_confidence": pl.Utf8,
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

    # Convert Y/N columns to boolean
    for col in _BOOL_COLUMNS:
        if col in out.columns:
            out = out.with_columns(
                pl.col(col).map_elements(_yn_to_bool, return_dtype=pl.Boolean)
            )

    # Strip whitespace from remaining string columns
    skip = {"id", "organization_id", "source_file_id", "ssa_state_county_code", "latitude", "longitude"} | _BOOL_COLUMNS
    for col in out.columns:
        if col not in skip and out.schema[col] == pl.Utf8:
            out = out.with_columns(pl.col(col).str.strip_chars())

    # Fill any missing optional columns with NULL
    _NULL_DTYPE: dict[str, pl.PolarsDataType] = {
        "latitude": pl.Float64,
        "longitude": pl.Float64,
        "accepts_new_patients": pl.Boolean,
        "uses_cms_ma_contract_amendment": pl.Boolean,
        "letter_of_intent": pl.Boolean,
    }
    for col in _ALL_OUTPUT_COLUMNS:
        if col not in out.columns:
            dtype = _NULL_DTYPE.get(col, pl.Utf8)
            out = out.with_columns(pl.lit(None).cast(dtype).alias(col))

    return out.select(_ALL_OUTPUT_COLUMNS)
