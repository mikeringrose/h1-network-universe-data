"""Ingest Provider or Facility HSD table from CSV or XLSX; normalize headers to canonical names."""

from __future__ import annotations

import re
from pathlib import Path

import polars as pl

from pipeline.config import get_settings

# Canonical column names (internal schema). CMS Appendix H/J variants normalize to these.
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

def _normalize_header(name: str) -> str:
    """Strip, lowercase, replace spaces/slashes with underscores, remove other non-alphanumeric."""
    if not name or (isinstance(name, float) and str(name) == "nan"):
        return ""
    s = str(name).strip().lower()
    s = re.sub(r"[\s/\-]+", "_", s)
    s = re.sub(r"[^a-z0-9_]", "", s)
    return s


# Normalized header -> canonical (when normalization diverges from expected name)
_HEADER_ALIASES: dict[str, str] = {
    "name_of_physician_or_midlevel_practitioner": "name_of_physician_or_mid_level_practitioner",
    "accepts_new_patients_yn": "accepts_new_patients",
    "uses_cms_ma_contract_amendment_yn": "uses_cms_ma_contract_amendment",
    "letter_of_intent_yn": "letter_of_intent",
    "national_provider_identifier": "npi",
    "national_provider_identifier_npi": "npi",
}


def read(path: str | Path, table_type: str) -> pl.DataFrame:
    """Read Provider or Facility HSD file (CSV or XLSX). Path relative to DATA_DIR if not absolute.
    Returns DataFrame with canonical column names; only columns that exist in the file are present.
    table_type must be 'provider' or 'facility'."""
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

    expected = PROVIDER_COLUMNS if table_type == "provider" else FACILITY_COLUMNS
    rename = {}
    for col in df.columns:
        norm = _normalize_header(col)
        canonical = _HEADER_ALIASES.get(norm, norm)
        if canonical in expected:
            rename[col] = canonical
    out = df.rename(rename)
    out_cols = [c for c in expected if c in out.columns]
    return out.select(out_cols) if out_cols else out
