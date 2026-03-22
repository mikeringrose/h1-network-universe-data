"""Orchestrate ingest -> transform -> load for CMS HSD Reference File (five-table schema)."""

from pathlib import Path

import polars as pl

from pipeline.config import get_settings
from pipeline.load import load as load_df

from .ingest import read_all_sheets
from .transform import transform_all

# Table names (must match models.py __tablename__); load order respects FK dependencies
TABLES = ["county", "specialty_type", "time_distance_standard", "minimum_number_standard", "con_credit"]


def run(
    file_path: str | Path,
    source_year: int,
    *,
    if_table_exists: str = "replace",
) -> None:
    """Ingest HSD Reference XLSX, transform to five normalized tables, load into PostgreSQL."""
    settings = get_settings()
    sheets = read_all_sheets(file_path)
    transformed = transform_all(sheets, source_year)

    for table_name in TABLES:
        df = transformed.get(table_name, pl.DataFrame())
        if df.is_empty():
            continue
        load_df(df, table_name, settings.database_url, if_table_exists)
