"""SQLAlchemy table definitions for HSD Reference (five-table normalized schema)."""

from sqlalchemy import Boolean, Float, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for HSD models."""


class County(Base):
    """One row per county. ssa_county_code = SSACD (5-digit)."""

    __tablename__ = "county"

    ssa_county_code: Mapped[str] = mapped_column(String(16), primary_key=True, nullable=False)
    contract_year: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    county_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    state_code: Mapped[str | None] = mapped_column(String(8), nullable=True)
    county_type: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    total_medicare_beneficiaries: Mapped[int | None] = mapped_column(Integer, nullable=True)
    con_state: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    # contract_year above


class SpecialtyType(Base):
    """Reference table of evaluated specialties (provider or facility)."""

    __tablename__ = "specialty_type"

    specialty_code: Mapped[str] = mapped_column(String(16), primary_key=True, nullable=False)
    contract_year: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    specialty_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    category: Mapped[str | None] = mapped_column(String(32), nullable=True)  # provider | facility
    telehealth_eligible: Mapped[bool | None] = mapped_column(Boolean, nullable=True)


class TimeDistanceStandard(Base):
    """County x specialty time/distance standards (flattened provider + facility)."""

    __tablename__ = "time_distance_standard"

    ssa_county_code: Mapped[str] = mapped_column(String(16), primary_key=True, nullable=False)
    specialty_code: Mapped[str] = mapped_column(String(16), primary_key=True, nullable=False)
    contract_year: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    max_time_minutes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_distance_miles: Mapped[int | None] = mapped_column(Integer, nullable=True)
    pct_beneficiaries_required: Mapped[int | None] = mapped_column(Integer, nullable=True)
    is_customized: Mapped[bool | None] = mapped_column(Boolean, nullable=True)


class MinimumNumberStandard(Base):
    """County x specialty minimum provider/bed counts."""

    __tablename__ = "minimum_number_standard"

    ssa_county_code: Mapped[str] = mapped_column(String(16), primary_key=True, nullable=False)
    specialty_code: Mapped[str] = mapped_column(String(16), primary_key=True, nullable=False)
    contract_year: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    min_providers_required: Mapped[int | None] = mapped_column(Integer, nullable=True)
    min_beds_required: Mapped[int | None] = mapped_column(Integer, nullable=True)
    provider_ratio: Mapped[float | None] = mapped_column(Float, nullable=True)
    base_population_ratio_95th_pct: Mapped[float | None] = mapped_column(Float, nullable=True)


class ConCredit(Base):
    """Counties/specialties eligible for the CON 10% credit."""

    __tablename__ = "con_credit"

    ssa_county_code: Mapped[str] = mapped_column(String(16), primary_key=True, nullable=False)
    specialty_code: Mapped[str] = mapped_column(String(16), primary_key=True, nullable=False)
    contract_year: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
