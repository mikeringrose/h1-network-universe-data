"""Transform Census ZCTA Gazetteer to canonical schema."""

import re

import polars as pl

# Census column name -> canonical name (after lowercasing)
_COLUMN_MAP = {
    "geoid": "geoid",
    "name": "name",
    "intptlat": "intptlat",
    "intptlong": "intptlong",
    "aland": "aland_sqmeters",
    "awater": "awater_sqmeters",
    "aland_sqmi": "aland_sqmi",
    "awater_sqmi": "awater_sqmi",
}


def _normalize_name(s: str) -> str:
    t = s.strip().lower()
    return re.sub(r"\s+", "_", t)


def transform(df: pl.DataFrame, source_year: int) -> pl.DataFrame:
    """Normalize columns, cast dtypes, add source_year; drop rows with null geoid or coordinates."""
    if df.is_empty():
        return df
    # Normalize column names
    rename = {c: _normalize_name(c) for c in df.columns}
    out = df.rename(rename)
    # Map to canonical names (only columns we know)
    canonical = {}
    for old, new in _COLUMN_MAP.items():
        if old in out.columns:
            canonical[old] = new
    out = out.rename(canonical)
    # Select only canonical columns that exist
    want = ["geoid", "name", "intptlat", "intptlong", "aland_sqmeters", "awater_sqmeters", "aland_sqmi", "awater_sqmi"]
    have = [c for c in want if c in out.columns]
    out = out.select(have)
    # Cast
    if "intptlat" in out.columns:
        out = out.with_columns(pl.col("intptlat").cast(pl.Float64, strict=False))
    if "intptlong" in out.columns:
        out = out.with_columns(pl.col("intptlong").cast(pl.Float64, strict=False))
    if "aland_sqmeters" in out.columns:
        out = out.with_columns(pl.col("aland_sqmeters").cast(pl.Int64, strict=False))
    if "awater_sqmeters" in out.columns:
        out = out.with_columns(pl.col("awater_sqmeters").cast(pl.Int64, strict=False))
    if "aland_sqmi" in out.columns:
        out = out.with_columns(pl.col("aland_sqmi").cast(pl.Float64, strict=False))
    if "awater_sqmi" in out.columns:
        out = out.with_columns(pl.col("awater_sqmi").cast(pl.Float64, strict=False))
    out = out.with_columns(pl.lit(source_year).alias("source_year"))
    # Drop rows with null geoid or null coordinates
    if "geoid" in out.columns:
        out = out.filter(pl.col("geoid").is_not_null() & (pl.col("geoid").str.strip_chars().str.len_chars() > 0))
    if "intptlat" in out.columns:
        out = out.filter(pl.col("intptlat").is_not_null())
    if "intptlong" in out.columns:
        out = out.filter(pl.col("intptlong").is_not_null())
    return out
