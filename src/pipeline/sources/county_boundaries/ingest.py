"""Ingest Census County Cartographic Boundary file (shapefile or GeoPackage)."""

from pathlib import Path

import geopandas as gpd

from pipeline.config import get_settings


def read(path: str | Path) -> gpd.GeoDataFrame:
    """Read county boundary file (.shp, .gpkg, or directory with .shp).
    Path resolved relative to DATA_DIR if not absolute."""
    settings = get_settings()
    resolved = Path(path) if Path(path).is_absolute() else Path(settings.data_dir) / path
    if not resolved.exists():
        raise FileNotFoundError(resolved)
    return gpd.read_file(resolved)
