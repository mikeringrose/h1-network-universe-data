"""Transform validated Provider/Facility HSD DataFrames: normalize dtypes, add source_year."""

from __future__ import annotations

import polars as pl

from .ingest import FACILITY_COLUMNS, PROVIDER_COLUMNS


def _str_5(s: str) -> str:
    """Pad to 5 digits for SSA code."""
    s = str(s).strip()
    if s.isdigit() and len(s) <= 5:
        return s.zfill(5)
    return s


def _str_3(s: str) -> str:
    """Pad to 3 digits for specialty code."""
    s = str(s).strip()
    if s.isdigit() and len(s) <= 3:
        return s.zfill(3)
    return s


def _yn_to_bool(val: str) -> bool | None:
    if val is None or (isinstance(val, float) and str(val) == "nan"):
        return None
    v = str(val).strip().upper()
    if not v:
        return None
    if v in ("Y", "YES", "TRUE", "1"):
        return True
    if v in ("N", "NO", "FALSE", "0"):
        return False
    return None


def transform_provider(df: pl.DataFrame, source_year: int) -> pl.DataFrame:
    """Normalize dtypes for Provider HSD; add source_year. Schema matches provider_hsd table."""
    if df.is_empty():
        schema = {c: pl.Utf8 for c in PROVIDER_COLUMNS} | {"source_year": pl.Int64}
        schema["accepts_new_patients"] = pl.Boolean
        schema["uses_cms_ma_contract_amendment"] = pl.Boolean
        return pl.DataFrame(schema=schema)

    out = df.with_columns(
        pl.col("ssa_state_county_code").map_elements(_str_5, return_dtype=pl.Utf8),
        pl.col("npi").str.strip_chars().cast(pl.Utf8),
        pl.col("specialty_code").map_elements(_str_3, return_dtype=pl.Utf8),
        pl.lit(source_year).alias("source_year"),
    )
    for c in out.columns:
        if c in ("ssa_state_county_code", "npi", "specialty_code", "source_year"):
            continue
        if out.schema[c] == pl.Utf8:
            out = out.with_columns(pl.col(c).str.strip_chars())

    if "accepts_new_patients" in out.columns:
        out = out.with_columns(
            pl.col("accepts_new_patients").map_elements(
                lambda x: _yn_to_bool(str(x).strip() if x is not None else None),
                return_dtype=pl.Boolean,
            )
        )
    if "uses_cms_ma_contract_amendment" in out.columns:
        out = out.with_columns(
            pl.col("uses_cms_ma_contract_amendment").map_elements(
                lambda x: _yn_to_bool(str(x).strip() if x is not None else None),
                return_dtype=pl.Boolean,
            )
        )
    # letter_of_intent stays string (Y/N or blank)
    return out


def transform_facility(df: pl.DataFrame, source_year: int) -> pl.DataFrame:
    """Normalize dtypes for Facility HSD; add source_year. Schema matches facility_hsd table."""
    if df.is_empty():
        schema = {c: pl.Utf8 for c in FACILITY_COLUMNS} | {"source_year": pl.Int64}
        schema["number_of_beds"] = pl.Int64
        return pl.DataFrame(schema=schema)

    out = df.with_columns(
        pl.col("ssa_state_county_code").map_elements(_str_5, return_dtype=pl.Utf8),
        pl.col("npi").str.strip_chars().cast(pl.Utf8),
        pl.col("specialty_code").map_elements(_str_3, return_dtype=pl.Utf8),
        pl.lit(source_year).alias("source_year"),
    )
    if "number_of_beds" in out.columns:
        out = out.with_columns(
            pl.col("number_of_beds").cast(pl.Int64, strict=False)
        )
    for c in out.columns:
        if c in ("ssa_state_county_code", "npi", "specialty_code", "source_year", "number_of_beds"):
            continue
        if out.schema[c] == pl.Utf8:
            out = out.with_columns(pl.col(c).str.strip_chars())

    return out