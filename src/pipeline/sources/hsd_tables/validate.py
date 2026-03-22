"""Validate Provider/Facility HSD DataFrames against CMS specs and HSD Reference. Returns structured errors/warnings."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Literal

import polars as pl
from sqlalchemy import text
from sqlalchemy.engine import create_engine

# CMS specialty code ranges (§422.116(b)(1) and (b)(2)). Fallback when DB has no rows.
PROVIDER_SPECIALTY_CODES = {f"{i:03d}" for i in range(1, 30)}  # 001-029
FACILITY_SPECIALTY_CODES = {f"{i:03d}" for i in range(40, 54)}  # 040-053

ACCURACY_CONFIDENCE_VALUES = {"high", "medium", "low"}
PO_BOX_PATTERN = re.compile(r"\b(?:p\.?\s*o\.?\s*box|post\s*office\s*box|po\s*box)\b", re.I)


@dataclass
class ValidationError:
    """One validation issue: row (1-based), field name, message."""

    row_index: int
    field: str
    message: str


@dataclass
class ValidationResult:
    """Errors (blocking) and warnings (non-blocking)."""

    errors: list[ValidationError] = field(default_factory=list)
    warnings: list[ValidationError] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0


def _valid_ssa_codes(engine, contract_year: int) -> set[str]:
    """Set of valid 5-digit SSA county codes for the given year from county table."""
    with engine.connect() as conn:
        r = conn.execute(
            text("SELECT DISTINCT ssa_county_code FROM county WHERE contract_year = :y"),
            {"y": contract_year},
        )
        return {str(row[0]).strip().zfill(5) for row in r if row[0] is not None}


def _valid_specialty_codes(engine, contract_year: int, category: Literal["provider", "facility"]) -> set[str]:
    """Set of valid specialty codes for the given year and category from specialty_type table."""
    with engine.connect() as conn:
        r = conn.execute(
            text(
                "SELECT DISTINCT specialty_code FROM specialty_type WHERE contract_year = :y AND category = :cat"
            ),
            {"y": contract_year, "cat": category},
        )
        codes = {str(row[0]).strip() for row in r if row[0] is not None}
    if codes:
        return codes
    return PROVIDER_SPECIALTY_CODES if category == "provider" else FACILITY_SPECIALTY_CODES


def _safe_str(val: any) -> str:
    if val is None or (isinstance(val, float) and str(val) == "nan"):
        return ""
    return str(val).strip()


def _validate_npi(val: str) -> bool:
    """NPI must be 10 digits (numeric)."""
    s = _safe_str(val)
    return len(s) == 10 and s.isdigit()


def _validate_ssa_code(val: str, valid_set: set[str]) -> bool:
    """5-digit SSA code, leading zeros preserved; must be in valid_set."""
    s = _safe_str(val)
    if len(s) <= 5:
        s = s.zfill(5)
    return len(s) == 5 and s.isdigit() and s in valid_set


def _validate_address(val: str) -> tuple[bool, str | None]:
    """Non-empty and not P.O. Box. Returns (ok, error_message)."""
    s = _safe_str(val)
    if not s:
        return False, "Address is required"
    if PO_BOX_PATTERN.search(s):
        return False, "P.O. Box is not allowed; geocodable street address required"
    return True, None


def _validate_accuracy_confidence(val: str) -> bool:
    if not _safe_str(val):
        return True
    return _safe_str(val).lower() in ACCURACY_CONFIDENCE_VALUES


REQUIRED_PROVIDER_COLUMNS = [
    "ssa_state_county_code",
    "name_of_physician_or_mid_level_practitioner",
    "npi",
    "specialty_code",
    "provider_street_address",
    "provider_city",
    "provider_state",
    "provider_zip_code",
]

REQUIRED_FACILITY_COLUMNS = [
    "ssa_state_county_code",
    "facility_name",
    "npi",
    "specialty_code",
    "facility_street_address",
    "facility_city",
    "facility_state",
    "facility_zip_code",
]


def validate_provider(
    df: pl.DataFrame,
    source_year: int,
    database_url: str,
    *,
    applicant: bool = False,
    valid_ssa_codes: set[str] | None = None,
    valid_specialty_codes: set[str] | None = None,
) -> ValidationResult:
    """Validate Provider HSD DataFrame. If applicant=False, LOI must be blank.
    Optional valid_ssa_codes/valid_specialty_codes override DB lookup (for testing)."""
    result = ValidationResult()
    missing = [c for c in REQUIRED_PROVIDER_COLUMNS if c not in df.columns]
    if missing:
        result.errors.append(
            ValidationError(0, "schema", f"Missing required columns: {', '.join(missing)}")
        )
        return result
    if df.is_empty():
        return result

    if valid_ssa_codes is not None:
        valid_ssa = {s.zfill(5) if len(s) <= 5 and s.isdigit() else s for s in valid_ssa_codes}
    else:
        engine = create_engine(database_url)
        valid_ssa = _valid_ssa_codes(engine, source_year)
    if valid_specialty_codes is not None:
        valid_specialty = {s.zfill(3) if len(s) <= 3 and s.isdigit() else s for s in valid_specialty_codes}
    else:
        engine = create_engine(database_url)
        valid_specialty = _valid_specialty_codes(engine, source_year, "provider")

    ssa_col = "ssa_state_county_code"
    npi_col = "npi"
    specialty_code_col = "specialty_code"
    addr_col = "provider_street_address"
    loi_col = "letter_of_intent"
    acc_col = "accuracy_confidence"

    rows = df.to_dicts()
    seen: set[tuple[str, str, str]] = set()

    for i, row in enumerate(rows):
        row_1 = i + 1
        # SSA
        if ssa_col in row:
            v = _safe_str(row.get(ssa_col))
            if len(v) <= 5:
                v = v.zfill(5)
            if not _validate_ssa_code(v, valid_ssa):
                result.errors.append(
                    ValidationError(row_1, ssa_col, f"Invalid or unknown SSA State/County code: {row.get(ssa_col)}")
                )
        # NPI
        if npi_col in row:
            npi_val = _safe_str(row.get(npi_col))
            if not _validate_npi(npi_val):
                result.errors.append(
                    ValidationError(row_1, npi_col, "NPI must be 10 digits")
                )
        # Specialty code
        if specialty_code_col in row:
            sc = _safe_str(row.get(specialty_code_col))
            if len(sc) <= 3:
                sc = sc.zfill(3)
            if sc not in valid_specialty:
                result.errors.append(
                    ValidationError(row_1, specialty_code_col, f"Invalid provider specialty code: {row.get(specialty_code_col)}")
                )
        # Address
        if addr_col in row:
            ok, err = _validate_address(_safe_str(row.get(addr_col)))
            if not ok:
                result.errors.append(ValidationError(row_1, addr_col, err or "Invalid address"))
        # LOI: must be blank for non-applicant
        if not applicant and loi_col in row:
            if _safe_str(row.get(loi_col)):
                result.errors.append(
                    ValidationError(row_1, loi_col, "Letter of Intent must be blank for non-applicant submissions")
                )
        # Accuracy confidence
        if acc_col in row and _safe_str(row.get(acc_col)):
            if not _validate_accuracy_confidence(_safe_str(row.get(acc_col))):
                result.errors.append(
                    ValidationError(row_1, acc_col, "Accuracy Confidence must be High, Medium, Low, or blank")
                )
        # Duplicate warning
        npi_s = _safe_str(row.get(npi_col, ""))
        ssa_s = _safe_str(row.get(ssa_col, ""))
        if len(ssa_s) <= 5:
            ssa_s = ssa_s.zfill(5)
        spec_s = _safe_str(row.get(specialty_code_col, ""))
        if len(spec_s) <= 3:
            spec_s = spec_s.zfill(3)
        key = (npi_s, ssa_s, spec_s)
        if npi_s and ssa_s and spec_s:
            if key in seen:
                result.warnings.append(
                    ValidationError(row_1, npi_col, f"Duplicate row (NPI + SSA code + specialty code)"))
            seen.add(key)

    return result


def validate_facility(
    df: pl.DataFrame,
    source_year: int,
    database_url: str,
    *,
    applicant: bool = False,
    valid_ssa_codes: set[str] | None = None,
    valid_specialty_codes: set[str] | None = None,
) -> ValidationResult:
    """Validate Facility HSD DataFrame. If applicant=False, LOI must be blank. Code 040 requires number_of_beds > 0.
    Optional valid_ssa_codes/valid_specialty_codes override DB lookup (for testing)."""
    result = ValidationResult()
    missing = [c for c in REQUIRED_FACILITY_COLUMNS if c not in df.columns]
    if missing:
        result.errors.append(
            ValidationError(0, "schema", f"Missing required columns: {', '.join(missing)}")
        )
        return result
    if df.is_empty():
        return result

    if valid_ssa_codes is not None:
        valid_ssa = {s.zfill(5) if len(s) <= 5 and s.isdigit() else s for s in valid_ssa_codes}
    else:
        engine = create_engine(database_url)
        valid_ssa = _valid_ssa_codes(engine, source_year)
    if valid_specialty_codes is not None:
        valid_specialty = {s.zfill(3) if len(s) <= 3 and s.isdigit() else s for s in valid_specialty_codes}
    else:
        engine = create_engine(database_url)
        valid_specialty = _valid_specialty_codes(engine, source_year, "facility")

    ssa_col = "ssa_state_county_code"
    npi_col = "npi"
    specialty_code_col = "specialty_code"
    addr_col = "facility_street_address"
    beds_col = "number_of_beds"
    loi_col = "letter_of_intent"
    acc_col = "accuracy_confidence"

    rows = df.to_dicts()
    seen: set[tuple[str, str, str]] = set()

    for i, row in enumerate(rows):
        row_1 = i + 1
        if ssa_col in row:
            v = _safe_str(row.get(ssa_col))
            if len(v) <= 5:
                v = v.zfill(5)
            if not _validate_ssa_code(v, valid_ssa):
                result.errors.append(
                    ValidationError(row_1, ssa_col, f"Invalid or unknown SSA State/County code: {row.get(ssa_col)}")
                )
        if npi_col in row:
            npi_val = _safe_str(row.get(npi_col))
            if not _validate_npi(npi_val):
                result.errors.append(
                    ValidationError(row_1, npi_col, "NPI must be 10 digits")
                )
        if specialty_code_col in row:
            sc = _safe_str(row.get(specialty_code_col))
            if len(sc) <= 3:
                sc = sc.zfill(3)
            if sc not in valid_specialty:
                result.errors.append(
                    ValidationError(row_1, specialty_code_col, f"Invalid facility specialty code: {row.get(specialty_code_col)}")
                )
        if addr_col in row:
            ok, err = _validate_address(_safe_str(row.get(addr_col)))
            if not ok:
                result.errors.append(ValidationError(row_1, addr_col, err or "Invalid address"))
        # Acute Inpatient Hospitals (040): number_of_beds required and > 0
        if specialty_code_col in row and beds_col in row:
            sc = _safe_str(row.get(specialty_code_col))
            if len(sc) <= 3:
                sc = sc.zfill(3)
            if sc == "040":
                beds_val = row.get(beds_col)
                try:
                    b = int(float(beds_val)) if beds_val is not None else None
                except (TypeError, ValueError):
                    b = None
                if b is None or b <= 0:
                    result.errors.append(
                        ValidationError(row_1, beds_col, "Number of Beds is required and must be > 0 for Acute Inpatient Hospitals (040)")
                    )
        if not applicant and loi_col in row:
            if _safe_str(row.get(loi_col)):
                result.errors.append(
                    ValidationError(row_1, loi_col, "Letter of Intent must be blank for non-applicant submissions")
                )
        if acc_col in row and _safe_str(row.get(acc_col)):
            if not _validate_accuracy_confidence(_safe_str(row.get(acc_col))):
                result.errors.append(
                    ValidationError(row_1, acc_col, "Accuracy Confidence must be High, Medium, Low, or blank")
                )
        npi_s = _safe_str(row.get(npi_col, ""))
        ssa_s = _safe_str(row.get(ssa_col, ""))
        if len(ssa_s) <= 5:
            ssa_s = ssa_s.zfill(5)
        spec_s = _safe_str(row.get(specialty_code_col, ""))
        if len(spec_s) <= 3:
            spec_s = spec_s.zfill(3)
        key = (npi_s, ssa_s, spec_s)
        if npi_s and ssa_s and spec_s:
            if key in seen:
                result.warnings.append(
                    ValidationError(row_1, npi_col, "Duplicate row (NPI + SSA code + specialty code)"))
            seen.add(key)

    return result
