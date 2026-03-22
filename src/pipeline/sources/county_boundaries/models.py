"""SQLAlchemy table definition for County Cartographic Boundary load target."""

from geoalchemy2 import Geometry
from sqlalchemy import Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for county boundary models."""


class CountyBoundary(Base):
    """Census county cartographic boundary (20m). One row per (geoid, source_year). Joins to HSD via geoid = ssacd."""

    __tablename__ = "county_boundaries"

    geoid: Mapped[str] = mapped_column(String(16), primary_key=True, nullable=False)
    source_year: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    geom: Mapped[object] = mapped_column(
        Geometry(geometry_type="MULTIPOLYGON", srid=4326),
        nullable=True,
    )
