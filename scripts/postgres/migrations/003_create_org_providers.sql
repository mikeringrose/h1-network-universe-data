CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS org_providers (
    id                        TEXT PRIMARY KEY,
    organization_id           TEXT NOT NULL,
    source_file_id            TEXT NOT NULL,
    ssa_state_county_code     TEXT,
    provider_name             TEXT,
    npi                       TEXT,
    provider_specialty_code   TEXT,
    contract_type             TEXT,
    street_address            TEXT,
    city                      TEXT,
    state_code                TEXT,
    zip_code                  TEXT,
    medical_group_affiliation TEXT,
    latitude                  DOUBLE PRECISION,
    longitude                 DOUBLE PRECISION,
    location                  geography(Point, 4326)
        GENERATED ALWAYS AS (
            CASE
                WHEN latitude IS NOT NULL AND longitude IS NOT NULL
                THEN ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography
                ELSE NULL
            END
        ) STORED,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_org_providers_source_file_id ON org_providers (source_file_id);
CREATE INDEX IF NOT EXISTS idx_org_providers_organization_id ON org_providers (organization_id);
CREATE INDEX IF NOT EXISTS idx_org_providers_npi ON org_providers (npi);
