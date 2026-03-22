"""SQLAlchemy table definition for ZCTA Gazetteer load target."""

from sqlalchemy import BigInteger, Float, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for ZCTA models."""


class ZCTAGazetteer(Base):
    """Census ZCTA gazetteer: centroid (lat/long) and area by ZCTA. One row per (geoid, source_year)."""

    __tablename__ = "zcta_gazetteer"

    geoid: Mapped[str] = mapped_column(String(16), primary_key=True, nullable=False)
    source_year: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    intptlat: Mapped[float] = mapped_column(Float, nullable=False)
    intptlong: Mapped[float] = mapped_column(Float, nullable=False)
    aland_sqmeters: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    awater_sqmeters: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    aland_sqmi: Mapped[float | None] = mapped_column(Float, nullable=True)
    awater_sqmi: Mapped[float | None] = mapped_column(Float, nullable=True)
