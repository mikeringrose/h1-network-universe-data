"""SQLAlchemy table definitions for Provider HSD and Facility HSD (CMS-standard + platform-extended)."""

from sqlalchemy import Boolean, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for HSD table models."""


class ProviderHsd(Base):
    """Provider HSD Table: individual practitioners (Type 1 NPI). CMS Appendix H + accuracy_confidence."""

    __tablename__ = "provider_hsd"

    ssa_state_county_code: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    name_of_physician_or_mid_level_practitioner: Mapped[str] = mapped_column(String(256), nullable=False)
    npi: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    specialty: Mapped[str | None] = mapped_column(String(128), nullable=True)
    specialty_code: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    contract_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    provider_street_address: Mapped[str] = mapped_column(String(256), nullable=False)
    provider_city: Mapped[str] = mapped_column(String(128), nullable=False)
    provider_state: Mapped[str] = mapped_column(String(16), nullable=False)
    provider_zip_code: Mapped[str] = mapped_column(String(16), nullable=False)
    accepts_new_patients: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    medical_group_affiliation: Mapped[str | None] = mapped_column(String(256), nullable=True)
    uses_cms_ma_contract_amendment: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    letter_of_intent: Mapped[str | None] = mapped_column(String(8), nullable=True)
    accuracy_confidence: Mapped[str | None] = mapped_column(String(16), nullable=True)
    source_year: Mapped[int | None] = mapped_column(Integer, nullable=True)


class FacilityHsd(Base):
    """Facility HSD Table: organizations (Type 2 NPI). CMS Appendix J + accuracy_confidence."""

    __tablename__ = "facility_hsd"

    ssa_state_county_code: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    facility_name: Mapped[str] = mapped_column(String(256), nullable=False)
    npi: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    specialty: Mapped[str | None] = mapped_column(String(128), nullable=True)
    specialty_code: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    facility_street_address: Mapped[str] = mapped_column(String(256), nullable=False)
    facility_city: Mapped[str] = mapped_column(String(128), nullable=False)
    facility_state: Mapped[str] = mapped_column(String(16), nullable=False)
    facility_zip_code: Mapped[str] = mapped_column(String(16), nullable=False)
    number_of_beds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    letter_of_intent: Mapped[str | None] = mapped_column(String(8), nullable=True)
    accuracy_confidence: Mapped[str | None] = mapped_column(String(16), nullable=True)
    source_year: Mapped[int | None] = mapped_column(Integer, nullable=True)
