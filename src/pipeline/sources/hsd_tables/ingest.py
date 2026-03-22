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

ORG_FACILITY_COLUMNS = [
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
    "national_provider_identifier_npi_number": "npi",
    "facility_or_service_type": "facility_service_type",
    "_of_staffed_medicare_certified_beds": "staffed_beds",
}

# Additional aliases applied only when reading org_facility files.
# Handles both "Facility *" headers (actual CMS files) and "Provider *" headers (per spec).
_ORG_FACILITY_ALIASES: dict[str, str] = {
    # "Specialty" in actual CMS files = facility service type
    "specialty": "facility_service_type",
    # "Specialty Code" in actual CMS files = facility specialty code
    "specialty_code": "facility_specialty_code",
    # address fields — "Facility *" (actual CMS files) and "Provider *" (per spec)
    "facility_street_address": "street_address",
    "facility_city": "city",
    "facility_state": "state_code",
    "facility_zip_code": "zip_code",
    "provider_street_address": "street_address",
    "provider_city": "city",
    "provider_state_code": "state_code",
    "provider_zip_code": "zip_code",
}

# Additional aliases applied only when reading org_provider files.
_ORG_PROVIDER_ALIASES: dict[str, str] = {
    # "Name of Physician..." variants → provider_name
    "name_of_physician_or_mid_level_practitioner": "provider_name",
    "name_of_physician_or_midlevel_practitioner":  "provider_name",
    # "Specialty Code" (actual files) → provider_specialty_code
    "specialty_code": "provider_specialty_code",
    # address fields
    "provider_street_address": "street_address",
    "provider_city":           "city",
    "provider_state":          "state_code",
    "provider_state_code":     "state_code",
    "provider_zip_code":       "zip_code",
}


def read(path: str | Path, table_type: str) -> pl.DataFrame:
    """Read Provider, Facility, OrgFacility, or OrgProvider HSD file (CSV or XLSX). Path relative to DATA_DIR if not absolute.
    Returns DataFrame with canonical column names; only columns that exist in the file are present.
    table_type must be 'provider', 'facility', 'org_facility', or 'org_provider'."""
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

    if table_type == "provider":
        expected = PROVIDER_COLUMNS
        aliases = _HEADER_ALIASES
    elif table_type == "org_facility":
        expected = ORG_FACILITY_COLUMNS
        aliases = _HEADER_ALIASES | _ORG_FACILITY_ALIASES
    elif table_type == "org_provider":
        expected = ORG_PROVIDER_COLUMNS
        aliases = _HEADER_ALIASES | _ORG_PROVIDER_ALIASES
    else:
        expected = FACILITY_COLUMNS
        aliases = _HEADER_ALIASES
    rename = {}
    for col in df.columns:
        norm = _normalize_header(col)
        canonical = aliases.get(norm, norm)
        if canonical in expected:
            rename[col] = canonical
    out = df.rename(rename)
    out_cols = [c for c in expected if c in out.columns]
    return out.select(out_cols) if out_cols else out
