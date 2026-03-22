"""Orchestrate ingest -> transform -> load for Census County Cartographic Boundary."""

from pathlib import Path

from sqlalchemy import create_engine

from pipeline.config import get_settings

from .ingest import read
from .transform import transform

TABLE_NAME = "county_boundaries"


def run(
    file_path: str | Path,
    source_year: int,
    *,
    if_table_exists: str = "replace",
) -> None:
    """Ingest county boundary shapefile or GeoPackage, transform to canonical schema, load into PostGIS."""
    settings = get_settings()
    gdf = read(file_path)
    gdf = transform(gdf, source_year)
    if not gdf.empty:
        engine = create_engine(settings.database_url)
        gdf.to_postgis(TABLE_NAME, engine, if_exists=if_table_exists, index=False)
