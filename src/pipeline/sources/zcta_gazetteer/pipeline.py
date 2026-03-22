"""Orchestrate ingest -> transform -> load for Census ZCTA Gazetteer."""

from pathlib import Path

from pipeline.config import get_settings
from pipeline.load import load as load_df

from .ingest import read
from .post_load import ensure_geom_column
from .transform import transform

TABLE_NAME = "zcta_gazetteer"


def run(
    file_path: str | Path,
    source_year: int,
    *,
    if_table_exists: str = "replace",
) -> None:
    """Ingest ZCTA gazetteer tab-delimited file, transform to canonical schema, load into PostgreSQL.
    After load, adds and populates PostGIS geom column (geography Point, SRID 4326) when PostGIS is enabled."""
    settings = get_settings()
    df = read(file_path)
    df = transform(df, source_year)
    if not df.is_empty():
        load_df(df, TABLE_NAME, settings.database_url, if_table_exists)
        try:
            ensure_geom_column(settings.database_url)
        except Exception as e:
            raise RuntimeError(
                "ZCTA load succeeded but PostGIS geom column step failed. "
                "Ensure PostGIS is enabled: CREATE EXTENSION IF NOT EXISTS postgis;"
            ) from e
