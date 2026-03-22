"""Transform raw HSD Reference sheets into normalized long-format DataFrames."""

from __future__ import annotations

import re
from typing import Any

import polars as pl


# Column layout (0-based). Data starts after header/code rows.
MIN_COUNTY_COLS = 8  # COUNTY, ST, COUNTY_STATE, SSACD, TOTAL BENEFICIARIES, COUNTY DESIGNATION, 95th..., BENEFICIARIES REQUIRED
TD_COUNTY_COLS = 5   # COUNTY, ST, COUNTY_STATE, SSACD, COUNTY DESIGNATION


def _normalize_name(s: Any) -> str:
    if s is None or (isinstance(s, float) and pl.nan_to_none(s) is None):
        return ""
    t = str(s).strip().lower()
    t = re.sub(r"\s+", "_", t)
    return re.sub(r"[^a-z0-9_]", "", t) or f"col_{hash(s) % 10000}"


def _safe_int(val: Any) -> int | None:
    if val is None:
        return None
    if isinstance(val, int) and not isinstance(val, bool):
        return val
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return None


def _header_and_codes(df: pl.DataFrame, header_row: int = 1, codes_row: int = 2) -> tuple[list[str], list[Any]]:
    headers = [df.row(header_row)[c] for c in range(df.width)]
    codes = [df.row(codes_row)[c] for c in range(df.width)] if codes_row < df.height else [None] * df.width
    return headers, codes


_ID_COL_NAMES = (
    "county",
    "st",
    "county_state",
    "ssacd",
    "total_beneficiaries",
    "county_designation",
    "pct_95_base_population_ratio",
    "beneficiaries_required_to_cover",
)


def transform_min_counts(
    df: pl.DataFrame,
    source_table: str,
    county_cols: int,
    specialty_start_col: int,
    source_year: int,
) -> pl.DataFrame:
    """Unpivot a minimum-count sheet (Provider or Facility) to long format."""
    if df.height < 3:
        return pl.DataFrame()
    _, codes = _header_and_codes(df, 1, 2)
    # Fixed id names; specialty columns get unique names from code
    col_names = list(_ID_COL_NAMES[:county_cols])
    for i in range(specialty_start_col, df.width):
        c = codes[i] if i < len(codes) else None
        col_names.append(f"spec_{str(c).strip() if c else i}")
    # Data rows from row 3 onward
    data = df.slice(3, df.height - 3).rename(
        {f"column_{i}": col_names[i] for i in range(df.width)}
    )
    id_cols = list(_ID_COL_NAMES[:county_cols])
    value_cols = [col_names[i] for i in range(specialty_start_col, df.width)]

    long = data.select(id_cols + value_cols).unpivot(
        index=id_cols,
        on=value_cols,
        variable_name="_var",
        value_name="min_count",
    )
    long = long.with_columns(
        pl.col("_var").str.replace("spec_", "").alias("specialty_code"),
        pl.col("min_count").cast(pl.Int64, strict=False).alias("min_count"),
        pl.lit(source_table).alias("source_table"),
        pl.lit(source_year).alias("source_year"),
    ).drop("_var")
    return long


def transform_time_distance(
    df: pl.DataFrame,
    source_table: str,
    county_cols: int,
    source_year: int,
) -> pl.DataFrame:
    """Unpivot a Time & Distance sheet to long format (one row per county per specialty)."""
    if df.height < 5:
        return pl.DataFrame()
    # Row 1: header, row 2: codes (at odd column indices per specialty pair), row 3: Time/Distance, row 4+: data
    headers = [df.row(1)[c] for c in range(df.width)]
    codes = [df.row(2)[c] for c in range(df.width)]
    # Specialty pairs start at column county_cols; code is at first col of each pair
    data = df.slice(4, df.height - 4)
    # Build one row per county per specialty: iterate specialty pairs
    start = county_cols
    n_pairs = (df.width - start) // 2
    out_rows = []
    for r in range(data.height):
        county_vals = [data.row(r)[c] for c in range(county_cols)]
        ssacd = county_vals[3]
        county_designation = county_vals[4]
        for p in range(n_pairs):
            c_time = start + 2 * p
            c_dist = start + 2 * p + 1
            code = codes[c_time] if c_time < len(codes) else None
            if code is None or str(code).strip() == "":
                continue
            t_val = _safe_int(data.row(r)[c_time])
            d_val = _safe_int(data.row(r)[c_dist])
            if t_val is None and d_val is None:
                continue
            out_rows.append({
                "county": county_vals[0],
                "st": county_vals[1],
                "county_state": county_vals[2],
                "ssacd": ssacd,
                "county_designation": county_designation,
                "specialty_code": str(code).strip(),
                "max_time_minutes": t_val,
                "max_distance_miles": d_val,
                "source_table": source_table,
                "source_year": source_year,
            })
    if not out_rows:
        return pl.DataFrame()
    return pl.DataFrame(out_rows)


def transform_certificate_of_need(df: pl.DataFrame, source_year: int) -> pl.DataFrame:
    """Use row 0 as header, rows 1+ as data; normalize column names."""
    if df.height < 2:
        return pl.DataFrame()
    headers = [_normalize_name(df.row(0)[c]) or f"col_{c}" for c in range(df.width)]
    data = df.slice(1, df.height - 1)
    data = data.rename({f"column_{i}": headers[i] for i in range(df.width)})
    data = data.with_columns(pl.lit(source_year).alias("source_year"))
    return data


def _normalize_county_type(raw: Any) -> str | None:
    """Map county_designation to snake_case county_type (e.g. Large Metro -> large_metro)."""
    if raw is None or (isinstance(raw, float) and pl.nan_to_none(raw) is None):
        return None
    s = str(raw).strip().lower()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_]", "", s)
    return s or None


def _build_county_df(intermediate: dict[str, pl.DataFrame]) -> pl.DataFrame:
    """One row per (ssa_county_code, contract_year) from min_provider or provider T/D."""
    mp = intermediate.get("min_provider_counts")
    ptd = intermediate.get("provider_time_distance")
    if mp is not None and not mp.is_empty():
        base = mp.unique(subset=["ssacd", "source_year"], keep="first")
    elif ptd is not None and not ptd.is_empty():
        base = ptd.unique(subset=["ssacd", "source_year"], keep="first")
    else:
        return pl.DataFrame()
    cols = [
        pl.col("ssacd").alias("ssa_county_code"),
        pl.col("source_year").alias("contract_year"),
        pl.col("county").alias("county_name") if "county" in base.columns else pl.lit(None).cast(pl.Utf8).alias("county_name"),
        pl.col("st").alias("state_code") if "st" in base.columns else pl.lit(None).cast(pl.Utf8).alias("state_code"),
        pl.col("county_designation").map_elements(_normalize_county_type, return_dtype=pl.Utf8).alias("county_type")
        if "county_designation" in base.columns
        else pl.lit(None).cast(pl.Utf8).alias("county_type"),
    ]
    if "total_beneficiaries" in base.columns:
        cols.append(pl.col("total_beneficiaries").cast(pl.Int64, strict=False).alias("total_medicare_beneficiaries"))
    else:
        cols.append(pl.lit(None).cast(pl.Int64).alias("total_medicare_beneficiaries"))
    cols.append(pl.lit(None).cast(pl.Boolean).alias("con_state"))
    return base.select(cols)


def _build_specialty_type_df(intermediate: dict[str, pl.DataFrame]) -> pl.DataFrame:
    """Distinct (specialty_code, category, contract_year); category = provider | facility."""
    parts = []
    for key, col in [("min_provider_counts", "provider"), ("min_facility_counts", "facility")]:
        df = intermediate.get(key)
        if df is None or df.is_empty():
            continue
        parts.append(
            df.select(
                pl.col("specialty_code"),
                pl.lit(col).alias("category"),
                pl.col("source_year").alias("contract_year"),
            ).unique()
        )
    if not parts:
        return pl.DataFrame()
    out = pl.concat(parts, how="vertical_relaxed").unique()
    return out.with_columns(
        pl.lit(None).cast(pl.Utf8).alias("specialty_name"),
        pl.lit(None).cast(pl.Boolean).alias("telehealth_eligible"),
    ).select(
        pl.col("specialty_code"),
        pl.col("contract_year"),
        pl.col("specialty_name"),
        pl.col("category"),
        pl.col("telehealth_eligible"),
    )


def _build_time_distance_standard_df(intermediate: dict[str, pl.DataFrame]) -> pl.DataFrame:
    """Concat provider + facility T/D; add pct_beneficiaries_required=90; is_customized from CON."""
    provider_td = intermediate.get("provider_time_distance")
    facility_td = intermediate.get("facility_time_distance")
    con_df = intermediate.get("certificate_of_need")
    parts = []
    if provider_td is not None and not provider_td.is_empty():
        parts.append(provider_td)
    if facility_td is not None and not facility_td.is_empty():
        parts.append(facility_td)
    if not parts:
        return pl.DataFrame()
    td = pl.concat(parts, how="vertical_relaxed").select(
        pl.col("ssacd").alias("ssa_county_code"),
        pl.col("specialty_code"),
        pl.col("source_year").alias("contract_year"),
        pl.col("max_time_minutes"),
        pl.col("max_distance_miles"),
        pl.lit(90).alias("pct_beneficiaries_required"),
    )
    if con_df is not None and not con_df.is_empty() and "td_customized_yn" in con_df.columns:
        ssa_col = "ssa" if "ssa" in con_df.columns else con_df.columns[0]
        con_lookup = con_df.select(
            pl.col(ssa_col).alias("ssa_county_code"),
            pl.col("specialty_code"),
            (pl.col("td_customized_yn").str.to_uppercase() == "Y").alias("_custom"),
        ).unique()
        td = td.join(con_lookup, on=["ssa_county_code", "specialty_code"], how="left").with_columns(
            pl.col("_custom").alias("is_customized")
        ).drop("_custom")
    else:
        td = td.with_columns(pl.lit(None).cast(pl.Boolean).alias("is_customized"))
    return td


def _build_minimum_number_standard_df(intermediate: dict[str, pl.DataFrame]) -> pl.DataFrame:
    """From min_provider + min_facility; specialty 040 -> min_beds_required."""
    mp = intermediate.get("min_provider_counts")
    mf = intermediate.get("min_facility_counts")
    provider_rows = (
        mp.select(
            pl.col("ssacd").alias("ssa_county_code"),
            pl.col("specialty_code"),
            pl.col("source_year").alias("contract_year"),
            pl.col("min_count").alias("min_providers_required"),
            pl.lit(None).cast(pl.Int64).alias("min_beds_required"),
            pl.lit(None).cast(pl.Float64).alias("provider_ratio"),
            pl.col("pct_95_base_population_ratio").cast(pl.Float64, strict=False).alias("base_population_ratio_95th_pct"),
        )
        if mp is not None and not mp.is_empty()
        else None
    )
    facility_rows = None
    if mf is not None and not mf.is_empty():
        facility_rows = mf.select(
            pl.col("ssacd").alias("ssa_county_code"),
            pl.col("specialty_code"),
            pl.col("source_year").alias("contract_year"),
            pl.lit(None).cast(pl.Int64).alias("min_providers_required"),
            pl.when(pl.col("specialty_code") == "040")
            .then(pl.col("min_count"))
            .otherwise(None)
            .alias("min_beds_required"),
            pl.lit(None).cast(pl.Float64).alias("provider_ratio"),
            pl.col("pct_95_base_population_ratio").cast(pl.Float64, strict=False).alias("base_population_ratio_95th_pct"),
        )
    if provider_rows is None and facility_rows is None:
        return pl.DataFrame()
    if provider_rows is not None and facility_rows is None:
        return provider_rows
    if provider_rows is None and facility_rows is not None:
        return facility_rows
    full = provider_rows.join(
        facility_rows,
        on=["ssa_county_code", "specialty_code", "contract_year"],
        how="full",
        coalesce=True,
    )
    # After full join, right-side value columns get _right suffix
    def _coalesce_pair(name: str) -> pl.Expr:
        right = f"{name}_right"
        if right in full.columns:
            return pl.coalesce(pl.col(name), pl.col(right)).alias(name)
        return pl.col(name)
    return full.with_columns([
        _coalesce_pair("min_providers_required"),
        _coalesce_pair("min_beds_required"),
        _coalesce_pair("provider_ratio"),
        _coalesce_pair("base_population_ratio_95th_pct"),
    ]).select(
        "ssa_county_code", "specialty_code", "contract_year",
        "min_providers_required", "min_beds_required", "provider_ratio", "base_population_ratio_95th_pct",
    )


def _build_con_credit_df(intermediate: dict[str, pl.DataFrame]) -> pl.DataFrame:
    """One row per (ssa_county_code, specialty_code, contract_year) from Certificate of Need."""
    con = intermediate.get("certificate_of_need")
    if con is None or con.is_empty():
        return pl.DataFrame()
    ssa_col = "ssa" if "ssa" in con.columns else [c for c in con.columns if "ssa" in c.lower() or c == "ssacd"]
    if isinstance(ssa_col, list):
        ssa_col = ssa_col[0] if ssa_col else con.columns[0]
    return con.select(
        pl.col(ssa_col).alias("ssa_county_code"),
        pl.col("specialty_code"),
        pl.col("source_year").alias("contract_year"),
    ).unique()


def transform_all(
    sheets: dict[str, pl.DataFrame],
    source_year: int,
) -> dict[str, pl.DataFrame]:
    """Transform HSD sheets into five normalized table DataFrames."""
    intermediate: dict[str, pl.DataFrame] = {}

    if "Minimum Provider #s" in sheets:
        df = sheets["Minimum Provider #s"]
        intermediate["min_provider_counts"] = transform_min_counts(
            df, "provider", MIN_COUNTY_COLS, MIN_COUNTY_COLS, source_year
        )
    if "Minimum Facility #s" in sheets:
        df = sheets["Minimum Facility #s"]
        intermediate["min_facility_counts"] = transform_min_counts(
            df, "facility", MIN_COUNTY_COLS, MIN_COUNTY_COLS, source_year
        )
    if "Provider Time & Distance" in sheets:
        intermediate["provider_time_distance"] = transform_time_distance(
            sheets["Provider Time & Distance"], "provider", TD_COUNTY_COLS, source_year
        )
    if "Facility Time & Distance" in sheets:
        intermediate["facility_time_distance"] = transform_time_distance(
            sheets["Facility Time & Distance"], "facility", TD_COUNTY_COLS, source_year
        )
    if "Certificate of Need" in sheets:
        intermediate["certificate_of_need"] = transform_certificate_of_need(
            sheets["Certificate of Need"], source_year
        )

    result: dict[str, pl.DataFrame] = {
        "county": _build_county_df(intermediate),
        "specialty_type": _build_specialty_type_df(intermediate),
        "time_distance_standard": _build_time_distance_standard_df(intermediate),
        "minimum_number_standard": _build_minimum_number_standard_df(intermediate),
        "con_credit": _build_con_credit_df(intermediate),
    }
    return result
