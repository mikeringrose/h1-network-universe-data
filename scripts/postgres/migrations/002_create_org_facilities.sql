CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS org_facilities (
    id                      TEXT PRIMARY KEY,
    organization_id         TEXT NOT NULL,
    source_file_id          TEXT NOT NULL,
    ssa_state_county_code   TEXT,
    facility_service_type   TEXT,
    facility_specialty_code TEXT,
    npi                     TEXT,
    staffed_beds            INTEGER,
    facility_name           TEXT,
    street_address          TEXT,
    city                    TEXT,
    state_code              TEXT,
    zip_code                TEXT,
    latitude                DOUBLE PRECISION,
    longitude               DOUBLE PRECISION,
    location                geography(Point, 4326)
        GENERATED ALWAYS AS (
            CASE
                WHEN latitude IS NOT NULL AND longitude IS NOT NULL
                THEN ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography
                ELSE NULL
            END
        ) STORED,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_org_facilities_source_file_id
    ON org_facilities (source_file_id);

CREATE INDEX IF NOT EXISTS idx_org_facilities_organization_id
    ON org_facilities (organization_id);

CREATE INDEX IF NOT EXISTS idx_org_facilities_npi
    ON org_facilities (npi);
