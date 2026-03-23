ALTER TABLE org_facilities
    RENAME COLUMN facility_service_type TO specialty;

ALTER TABLE org_facilities
    RENAME COLUMN staffed_beds TO number_of_beds;

ALTER TABLE org_facilities
    ADD COLUMN IF NOT EXISTS letter_of_intent   BOOLEAN,
    ADD COLUMN IF NOT EXISTS accuracy_confidence TEXT;
