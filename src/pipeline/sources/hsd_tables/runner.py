"""Safe wrapper: converts sys.exit(1) from validation failure into ValidationFailed."""
from pathlib import Path


class ValidationFailed(Exception):
    """Raised when hsd_tables pipeline validation fails."""


def run_safe(
    file_path: str | Path,
    source_year: int,
    table_type: str,
    *,
    if_table_exists: str = "replace",
    applicant: bool = False,
) -> None:
    from pipeline.sources.hsd_tables.pipeline import run
    try:
        run(file_path, source_year, table_type, if_table_exists=if_table_exists, applicant=applicant)
    except SystemExit as exc:
        if exc.code == 1:
            raise ValidationFailed(
                f"Validation failed for {table_type} file"
            ) from exc
        raise
