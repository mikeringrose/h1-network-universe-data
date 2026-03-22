"""Transform Census County Cartographic Boundary to canonical schema."""

import geopandas as gpd


def transform(gdf: gpd.GeoDataFrame, source_year: int) -> gpd.GeoDataFrame:
    """Keep geoid, name; ensure WGS84; add source_year; geometry column as geom."""
    if gdf.empty:
        return gdf
    # Normalize column names (Census uses GEOID, NAME)
    cols = {c.upper(): c for c in gdf.columns if c != gdf.geometry.name}
    geoid_col = cols.get("GEOID") or cols.get("geoid")
    name_col = cols.get("NAME") or cols.get("name")
    if geoid_col is None:
        raise ValueError("County boundary file must contain GEOID column")
    out = gpd.GeoDataFrame(
        gdf[[geoid_col]].rename(columns={geoid_col: "geoid"}),
        geometry=gdf.geometry.values,
        crs=gdf.crs,
    )
    out["name"] = gdf[name_col].values if name_col else None
    out = out.rename_geometry("geom")
    out["source_year"] = source_year
    out = out.to_crs(4326)
    return out[["geoid", "name", "source_year", "geom"]]
