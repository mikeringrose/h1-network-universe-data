"""Ingest Provider or Facility HSD table from CSV or XLSX; assign columns by position."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from pipeline.config import get_settings

# Canonical column names assigned by position (CMS HSD files have a fixed column order).
PROVIDER_COLUMNS = [
    "ssa_state_county_code",
    "name_of_physician_or_mid_level_practitioner",
    "npi",
    "specialty",
    "specialty_code",
    "contract_type",
    "provider_street_address",
    "provider_city",
    "provider_state",
    "provider_zip_code",
    "accepts_new_patients",
    "medical_group_affiliation",
    "uses_cms_ma_contract_amendment",
    "letter_of_intent",
    "accuracy_confidence",
]

FACILITY_COLUMNS = [
    "ssa_state_county_code",
    "facility_name",
    "npi",
    "specialty",
    "specialty_code",
    "facility_street_address",
    "facility_city",
    "facility_state",
    "facility_zip_code",
    "number_of_beds",
    "letter_of_intent",
    "accuracy_confidence",
]

ORG_FACILITY_COLUMNS = [
    "ssa_state_county_code",      # pos 0
    "facility_name",               # pos 1
    "npi",                         # pos 2
    "facility_service_type",       # pos 3 (specialty)
    "facility_specialty_code",     # pos 4 (specialty_code)
    "street_address",              # pos 5 (facility_street_address)
    "city",                        # pos 6 (facility_city)
    "state_code",                  # pos 7 (facility_state)
    "zip_code",                    # pos 8 (facility_zip_code)
    "staffed_beds",                # pos 9 (number_of_beds)
    # pos 10 (letter_of_intent) and pos 11 (accuracy_confidence) not captured
]

ORG_PROVIDER_COLUMNS = [
    "ssa_state_county_code",
    "provider_name",
    "npi",
    "provider_specialty_code",
    "contract_type",
    "street_address",
    "city",
    "state_code",
    "zip_code",
    "medical_group_affiliation",
]

# Positional assignment list for org_provider. Placeholder names (prefixed _) mark
# positions that exist in the file but are not captured in org_providers.
_ORG_PROVIDER_POSITIONAL = [
    "ssa_state_county_code",          # pos 0
    "provider_name",                   # pos 1
    "npi",                             # pos 2
    "_specialty",                      # pos 3 (not captured)
    "provider_specialty_code",         # pos 4
    "contract_type",                   # pos 5
    "street_address",                  # pos 6
    "city",                            # pos 7
    "state_code",                      # pos 8
    "zip_code",                        # pos 9
    "_accepts_new_patients",           # pos 10 (not captured)
    "medical_group_affiliation",       # pos 11
]


def read(path: str | Path, table_type: str) -> pl.DataFrame:
    """Read an HSD file (CSV or XLSX) and assign canonical column names by position.

    Path is relative to DATA_DIR if not absolute.
    table_type must be 'provider', 'facility', 'org_facility', or 'org_provider'.
    Returns a DataFrame containing only the columns defined for the given table type.
    """
    settings = get_settings()
    resolved = Path(path) if Path(path).is_absolute() else Path(settings.data_dir) / path
    if not resolved.exists():
        raise FileNotFoundError(resolved)

    suffix = resolved.suffix.lower()
    if suffix == ".csv":
        df = pl.read_csv(resolved, infer_schema_length=0, truncate_ragged_lines=True)
    elif suffix in (".xlsx", ".xls"):
        df = pl.read_excel(resolved, sheet_id=0, engine="openpyxl")
    else:
        raise ValueError(f"Unsupported file type: {suffix}")

    if df.is_empty():
        return df

    if table_type == "facility":
        col_list, select_cols = FACILITY_COLUMNS, FACILITY_COLUMNS
    elif table_type == "org_facility":
        col_list, select_cols = ORG_FACILITY_COLUMNS, ORG_FACILITY_COLUMNS
    elif table_type == "provider":
        col_list, select_cols = PROVIDER_COLUMNS, PROVIDER_COLUMNS
    else:  # org_provider
        col_list, select_cols = _ORG_PROVIDER_POSITIONAL, ORG_PROVIDER_COLUMNS

    n = min(len(df.columns), len(col_list))
    df = df.rename({old: new for old, new in zip(df.columns[:n], col_list[:n])})
    out_cols = [c for c in select_cols if c in df.columns]
    return df.select(out_cols) if out_cols else df
