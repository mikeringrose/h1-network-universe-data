"""Orchestrate ingest -> validate -> transform -> load for Provider or Facility HSD table."""

from pathlib import Path
import sys

from pipeline.config import get_settings
from pipeline.load import load as load_df

from .ingest import read
from .transform import transform_provider, transform_facility
from .validate import ValidationResult, validate_provider, validate_facility

TABLE_NAMES = {"provider": "provider_hsd", "facility": "facility_hsd"}


def _emit_validation_report(result: ValidationResult) -> None:
    """Print errors and warnings to stderr."""
    for e in result.errors:
        print(f"Error row {e.row_index}, {e.field}: {e.message}", file=sys.stderr)
    for w in result.warnings:
        print(f"Warning row {w.row_index}, {w.field}: {w.message}", file=sys.stderr)


def run(
    file_path: str | Path,
    source_year: int,
    table_type: str,
    *,
    if_table_exists: str = "replace",
    applicant: bool = False,
) -> None:
    """Ingest Provider or Facility HSD file, validate, transform, load. Exits with 1 on validation failure."""
    if table_type not in ("provider", "facility"):
        raise ValueError("table_type must be 'provider' or 'facility'")

    settings = get_settings()
    df = read(file_path, table_type)

    if table_type == "provider":
        result: ValidationResult = validate_provider(
            df, source_year, settings.database_url, applicant=applicant
        )
    else:
        result = validate_facility(
            df, source_year, settings.database_url, applicant=applicant
        )

    _emit_validation_report(result)
    if not result.is_valid:
        sys.exit(1)

    if table_type == "provider":
        out = transform_provider(df, source_year)
    else:
        out = transform_facility(df, source_year)

    table_name = TABLE_NAMES[table_type]
    load_df(out, table_name, settings.database_url, if_table_exists)
