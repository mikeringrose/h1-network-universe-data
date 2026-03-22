"""Tests for Provider/Facility HSD table ingest, validation, transform."""

import tempfile
from pathlib import Path

import polars as pl
import pytest

from pipeline.sources.hsd_tables.ingest import read, _normalize_header, PROVIDER_COLUMNS, FACILITY_COLUMNS
from pipeline.sources.hsd_tables.validate import (
    ValidationResult,
    validate_provider,
    validate_facility,
    REQUIRED_PROVIDER_COLUMNS,
    REQUIRED_FACILITY_COLUMNS,
)
from pipeline.sources.hsd_tables.transform import transform_provider, transform_facility

# Valid SSA and specialty codes for tests (no DB)
VALID_SSA = {"01001", "01003", "01010"}
VALID_PROVIDER_SPECIALTY = {"001", "003", "006"}
VALID_FACILITY_SPECIALTY = {"040", "041", "053"}


@pytest.fixture
def provider_df_valid() -> pl.DataFrame:
    return pl.DataFrame({
        "ssa_state_county_code": ["01001", "01003"],
        "name_of_physician_or_mid_level_practitioner": ["Jane Doe", "John Smith"],
        "npi": ["1234567890", "0987654321"],
        "specialty": ["Primary Care", "Cardiology"],
        "specialty_code": ["001", "003"],
        "provider_street_address": ["100 Main St", "200 Oak Ave"],
        "provider_city": ["Boston", "Cambridge"],
        "provider_state": ["MA", "MA"],
        "provider_zip_code": ["02101", "02139"],
        "letter_of_intent": ["", ""],
    })


@pytest.fixture
def facility_df_valid() -> pl.DataFrame:
    return pl.DataFrame({
        "ssa_state_county_code": ["01001", "01003"],
        "facility_name": ["City Hospital", "Community SNF"],
        "npi": ["1122334455", "5544332211"],
        "specialty": ["Acute Inpatient Hospitals", "Skilled Nursing Facilities"],
        "specialty_code": ["040", "053"],
        "facility_street_address": ["500 Hospital Dr", "600 Care Ln"],
        "facility_city": ["Boston", "Cambridge"],
        "facility_state": ["MA", "MA"],
        "facility_zip_code": ["02102", "02140"],
        "number_of_beds": [200, None],
        "letter_of_intent": ["", ""],
    })


class TestHeaderNormalization:
    def test_normalize_header(self):
        assert _normalize_header("SSA State/County Code") == "ssa_state_county_code"
        assert _normalize_header("  Name of Physician or Mid-Level Practitioner  ") == "name_of_physician_or_mid_level_practitioner"
        assert _normalize_header("NPI") == "npi"


class TestValidateProvider:
    def test_valid_provider_passes(self, provider_df_valid):
        result = validate_provider(
            provider_df_valid,
            2025,
            "postgresql://localhost/dummy",
            valid_ssa_codes=VALID_SSA,
            valid_specialty_codes=VALID_PROVIDER_SPECIALTY,
        )
        assert result.is_valid
        assert len(result.errors) == 0

    def test_missing_required_columns(self):
        df = pl.DataFrame({"npi": ["1234567890"], "ssa_state_county_code": ["01001"]})
        result = validate_provider(
            df, 2025, "postgresql://localhost/dummy",
            valid_ssa_codes=VALID_SSA,
            valid_specialty_codes=VALID_PROVIDER_SPECIALTY,
        )
        assert not result.is_valid
        assert any("Missing required columns" in e.message for e in result.errors)

    def test_invalid_npi(self, provider_df_valid):
        df = provider_df_valid.with_columns(pl.lit("123").alias("npi"))
        result = validate_provider(
            df, 2025, "postgresql://localhost/dummy",
            valid_ssa_codes=VALID_SSA,
            valid_specialty_codes=VALID_PROVIDER_SPECIALTY,
        )
        assert not result.is_valid
        assert any("NPI must be 10 digits" in e.message for e in result.errors)

    def test_invalid_ssa_code(self, provider_df_valid):
        df = provider_df_valid.with_columns(pl.lit("99999").alias("ssa_state_county_code"))
        result = validate_provider(
            df, 2025, "postgresql://localhost/dummy",
            valid_ssa_codes=VALID_SSA,
            valid_specialty_codes=VALID_PROVIDER_SPECIALTY,
        )
        assert not result.is_valid
        assert any("SSA" in e.message for e in result.errors)

    def test_po_box_address_rejected(self, provider_df_valid):
        df = provider_df_valid.with_columns(pl.lit("P.O. Box 123").alias("provider_street_address"))
        result = validate_provider(
            df, 2025, "postgresql://localhost/dummy",
            valid_ssa_codes=VALID_SSA,
            valid_specialty_codes=VALID_PROVIDER_SPECIALTY,
        )
        assert not result.is_valid
        assert any("P.O. Box" in e.message for e in result.errors)

    def test_loi_must_be_blank_non_applicant(self, provider_df_valid):
        df = provider_df_valid.with_columns(pl.lit("Y").alias("letter_of_intent"))
        result = validate_provider(
            df, 2025, "postgresql://localhost/dummy",
            valid_ssa_codes=VALID_SSA,
            valid_specialty_codes=VALID_PROVIDER_SPECIALTY,
            applicant=False,
        )
        assert not result.is_valid
        assert any("Letter of Intent" in e.message for e in result.errors)

    def test_duplicate_warning(self, provider_df_valid):
        df = pl.concat([provider_df_valid, provider_df_valid])
        result = validate_provider(
            df, 2025, "postgresql://localhost/dummy",
            valid_ssa_codes=VALID_SSA,
            valid_specialty_codes=VALID_PROVIDER_SPECIALTY,
        )
        assert result.is_valid
        assert len(result.warnings) >= 1
        assert any("Duplicate" in w.message for w in result.warnings)


class TestValidateFacility:
    def test_valid_facility_passes(self, facility_df_valid):
        result = validate_facility(
            facility_df_valid,
            2025,
            "postgresql://localhost/dummy",
            valid_ssa_codes=VALID_SSA,
            valid_specialty_codes=VALID_FACILITY_SPECIALTY,
        )
        assert result.is_valid

    def test_040_requires_beds(self):
        df = pl.DataFrame({
            "ssa_state_county_code": ["01001"],
            "facility_name": ["Hospital"],
            "npi": ["1122334455"],
            "specialty_code": ["040"],
            "facility_street_address": ["100 Main St"],
            "facility_city": ["Boston"],
            "facility_state": ["MA"],
            "facility_zip_code": ["02101"],
            "number_of_beds": [0],
            "letter_of_intent": [""],
        })
        result = validate_facility(
            df, 2025, "postgresql://localhost/dummy",
            valid_ssa_codes=VALID_SSA,
            valid_specialty_codes=VALID_FACILITY_SPECIALTY,
        )
        assert not result.is_valid
        assert any("Number of Beds" in e.message for e in result.errors)


class TestTransform:
    def test_transform_provider_adds_source_year(self, provider_df_valid):
        out = transform_provider(provider_df_valid, 2025)
        assert "source_year" in out.columns
        assert out["source_year"][0] == 2025

    def test_transform_provider_ssa_padded(self, provider_df_valid):
        df = provider_df_valid.with_columns(pl.lit("1001").alias("ssa_state_county_code"))
        out = transform_provider(df, 2025)
        assert out["ssa_state_county_code"][0] == "01001"

    def test_transform_facility_beds_int(self, facility_df_valid):
        out = transform_facility(facility_df_valid, 2025)
        assert out.schema["number_of_beds"] == pl.Int64


class TestIngest:
    def test_read_provider_csv(self, provider_df_valid):
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            path = Path(f.name)
        try:
            provider_df_valid.write_csv(path)
            out = read(path, "provider")
            assert "ssa_state_county_code" in out.columns
            assert out.height == provider_df_valid.height
        finally:
            path.unlink(missing_ok=True)

    def test_read_provider_csv_normalizes_headers(self, provider_df_valid):
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
            path = Path(f.name)
        try:
            # Header with spaces/slashes
            path.write_text(
                "SSA State/County Code,Name of Physician or Mid-Level Practitioner,NPI,Specialty Code,"
                "Provider Street Address,Provider City,Provider State,Provider ZIP Code\n"
                "01001,Jane Doe,1234567890,001,100 Main St,Boston,MA,02101\n"
            )
            out = read(path, "provider")
            assert "ssa_state_county_code" in out.columns
            assert out.height == 1
        finally:
            path.unlink(missing_ok=True)
