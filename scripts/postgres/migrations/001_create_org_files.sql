-- Migration 001: org_files table
-- Run: psql -U user -d pipeline -h localhost -f scripts/postgres/migrations/001_create_org_files.sql

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'file_type_enum') THEN
        CREATE TYPE file_type_enum AS ENUM ('PROVIDER', 'FACILITY', 'BENEFICIARY');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'file_status_enum') THEN
        CREATE TYPE file_status_enum AS ENUM ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED');
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS org_files (
    id              TEXT PRIMARY KEY,
    file_type       file_type_enum NOT NULL,
    organization_id TEXT NOT NULL,
    uploaded_by_id  TEXT NOT NULL,
    original_name   TEXT NOT NULL,
    mime_type       TEXT,
    size_bytes      BIGINT,
    s3_bucket       TEXT NOT NULL,
    s3_key          TEXT NOT NULL,
    status          file_status_enum NOT NULL DEFAULT 'PENDING',
    error_message   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_org_files_status_pending
    ON org_files (id)
    WHERE status = 'PENDING';
