"""Post-load step: add and populate PostGIS geography point column for zcta_gazetteer."""

from sqlalchemy import text
from sqlalchemy import create_engine


def ensure_geom_column(connection: str) -> None:
    """Add geom geography(Point, 4326) if missing and populate from intptlong, intptlat."""
    engine = create_engine(connection)
    with engine.connect() as conn:
        conn.execute(text(
            "ALTER TABLE zcta_gazetteer ADD COLUMN IF NOT EXISTS geom geography(Point, 4326);"
        ))
        # Use quoted identifiers so column names match what polars/ADBC wrote (case-sensitive).
        conn.execute(text("""
            UPDATE zcta_gazetteer
            SET geom = ST_SetSRID(ST_MakePoint("intptlong"::double precision, "intptlat"::double precision), 4326)::geography
            WHERE "intptlat" IS NOT NULL AND "intptlong" IS NOT NULL;
        """))
        conn.commit()
