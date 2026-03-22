# File Processing Worker — Technical Specification

This document describes the contract between the Node.js API and the Python worker responsible for processing uploaded organization files. The API handles file intake and queuing; the worker handles all downstream processing.

---

## Overview

```
User upload
    │
    ▼
Node.js API
  1. Validates and buffers file
  2. Uploads raw file to S3
  3. Creates org_files record (status = PENDING)
  4. Publishes { fileId } to SQS queue
    │
    ▼
SQS Queue
    │
    ▼
Python Worker
  1. Receives SQS message
  2. Looks up org_files record by fileId
  3. Updates status → PROCESSING
  4. Downloads file from S3
  5. Processes file contents
  6. Updates status → COMPLETED or FAILED
  7. Deletes SQS message
```

---

## SQS Message

### Queue type

Standard queue (not FIFO). Messages may be delivered more than once — the worker must be idempotent (see [Idempotency](#idempotency)).

### Message body

```json
{ "fileId": "550e8400-e29b-41d4-a716-446655440000" }
```

`fileId` is the UUID primary key of the `org_files` row. All other context (S3 location, file type, organization) is fetched from the database after receiving the message. This avoids stale data in the message payload.

### Environment variables

| Variable        | Description                                                                             |
| --------------- | --------------------------------------------------------------------------------------- |
| `SQS_QUEUE_URL` | Full queue URL, e.g. `https://sqs.us-east-1.amazonaws.com/123456789012/org-files-queue` |
| `SQS_ENDPOINT`  | Optional. Set to `http://localhost:4566` when using LocalStack locally.                 |

---

## Shared Database Table

The `org_files` table (PostgreSQL) is the communication channel between the API and worker.

### Table: `org_files`

| Column            | Type                 | Notes                                                                                                          |
| ----------------- | -------------------- | -------------------------------------------------------------------------------------------------------------- |
| `id`              | `TEXT` (UUID)        | Primary key. Matches `fileId` in SQS message.                                                                  |
| `file_type`       | `OrgFileType` enum   | `PROVIDER`, `FACILITY`, or `BENEFICIARY`                                                                       |
| `organization_id` | `TEXT` (UUID)        | FK → `organizations.id`                                                                                        |
| `uploaded_by_id`  | `TEXT` (CUID)        | FK → `users.id`                                                                                                |
| `original_name`   | `TEXT`               | Original filename as uploaded                                                                                  |
| `mime_type`       | `TEXT`               | `text/csv`, `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet`, or `application/vnd.ms-excel` |
| `size_bytes`      | `INTEGER`            | File size in bytes                                                                                             |
| `s3_bucket`       | `TEXT`               | S3 bucket name                                                                                                 |
| `s3_key`          | `TEXT`               | Full S3 object key                                                                                             |
| `status`          | `OrgFileStatus` enum | See lifecycle below                                                                                            |
| `error_message`   | `TEXT` (nullable)    | Populated on `FAILED`; null otherwise                                                                          |
| `created_at`      | `TIMESTAMP`          | Set by API on upload                                                                                           |
| `updated_at`      | `TIMESTAMP`          | Updated on every status change                                                                                 |

### Status lifecycle

```
PENDING → PROCESSING → COMPLETED
                     ↘ FAILED
```

| Status       | Set by | When                                  |
| ------------ | ------ | ------------------------------------- |
| `PENDING`    | API    | Row created after S3 upload succeeds  |
| `PROCESSING` | Worker | Immediately after claiming the job    |
| `COMPLETED`  | Worker | Processing finished successfully      |
| `FAILED`     | Worker | Unrecoverable error during processing |

The worker **must** transition through `PROCESSING` before doing any work. This provides a visible in-progress state and is required for idempotency checks.

---

## S3 File Access

Files are stored in S3 with AES256 server-side encryption. Read the `s3_bucket` and `s3_key` columns from the database row — do not reconstruct the S3 key yourself.

### Key structure (for reference only)

```
{file_type_prefix}/{organization_id}/{file_id}/{original_name}

# Examples:
provider-files/org-uuid/file-uuid/providers.csv
facility-files/org-uuid/file-uuid/facilities.xlsx
beneficiary-files/org-uuid/file-uuid/members.csv
```

### IAM permissions required

The worker process needs an IAM role/policy with at minimum:

```json
{
  "Effect": "Allow",
  "Action": ["s3:GetObject"],
  "Resource": "arn:aws:s3:::your-org-files-bucket/*"
}
```

---

## Worker Processing Contract

### Step 1 — Receive message

Poll the SQS queue using long polling (recommended: `WaitTimeSeconds=20`). Parse `fileId` from `MessageBody`.

### Step 2 — Claim the job

Before doing any work, attempt to transition the row to `PROCESSING` using an atomic conditional update:

```sql
UPDATE org_files
SET    status = 'PROCESSING', updated_at = NOW()
WHERE  id = $1
AND    status = 'PENDING'
RETURNING *;
```

- If the `UPDATE` returns a row: proceed with processing.
- If it returns no rows: the record is already `PROCESSING`, `COMPLETED`, or `FAILED`. Delete the SQS message and stop — another worker instance already claimed it.

This `WHERE status = 'PENDING'` guard is the primary idempotency mechanism.

### Step 3 — Download and process

Download the file from S3 using the `s3_bucket` and `s3_key` from the returned row. Process according to `file_type`:

| `file_type`   | Expected content        |
| ------------- | ----------------------- |
| `PROVIDER`    | Provider network data   |
| `FACILITY`    | Facility/location data  |
| `BENEFICIARY` | Beneficiary/member data |

Accepted formats: CSV (`.csv`), Excel 2007+ (`.xlsx`), Excel 97-2003 (`.xls`). The `mime_type` column identifies the format.

See [File Type Processing Details](#file-type-processing-details) for per-type schemas and SQL.

### Step 4 — Mark complete or failed

On success:

```sql
UPDATE org_files
SET    status = 'COMPLETED', error_message = NULL, updated_at = NOW()
WHERE  id = $1;
```

On unrecoverable error:

```sql
UPDATE org_files
SET    status = 'FAILED',
       error_message = $2,  -- human-readable description, max ~500 chars
       updated_at = NOW()
WHERE  id = $1;
```

### Step 5 — Delete the SQS message

Always delete the message after reaching a terminal state (`COMPLETED` or `FAILED`). Leaving it in the queue will cause redelivery.

---

## Idempotency

Because SQS standard queues guarantee at-least-once delivery, the worker may receive the same message more than once. The conditional `UPDATE ... WHERE status = 'PENDING'` in Step 2 ensures only one worker instance processes each file. Any duplicate delivery will find the row already in `PROCESSING` or a terminal state and will safely skip it.

If the worker crashes after setting `PROCESSING` but before finishing, the row will be stuck in `PROCESSING` indefinitely. The recommended recovery strategy is a separate periodic sweep:

```sql
-- Find jobs stuck in PROCESSING for more than N minutes
SELECT id FROM org_files
WHERE  status = 'PROCESSING'
AND    updated_at < NOW() - INTERVAL '15 minutes';
```

Reset or re-enqueue these rows depending on your retry policy. Alternatively, configure an SQS Dead Letter Queue (DLQ) with a `maxReceiveCount` appropriate for your expected processing time.

---

## Error Handling Guidance

| Scenario                                          | Recommended action                                                      |
| ------------------------------------------------- | ----------------------------------------------------------------------- |
| File not found in database                        | Log warning, delete SQS message (stale or test message)                 |
| Row already `PROCESSING` / `COMPLETED` / `FAILED` | Delete SQS message, skip                                                |
| S3 download fails (transient)                     | Do not delete message; allow SQS visibility timeout to expire for retry |
| File parse error (bad format)                     | Set `FAILED` with descriptive `error_message`, delete message           |
| DB connection lost mid-job                        | Do not update status; allow SQS visibility timeout to expire for retry  |

---

## Local Development with LocalStack

Configure the worker to point at LocalStack:

```
SQS_ENDPOINT=http://localhost:4566
SQS_QUEUE_URL=http://localhost:4566/000000000000/org-files-queue
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_DEFAULT_REGION=us-east-1
```

Create the local queue (run once):

```bash
aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name org-files-queue
```

The API's `docker-compose.yml` runs LocalStack; the worker can connect to it on the same network or via `localhost:4566` from the host.

---

## File Type Processing Details

### FACILITY files

#### Source format

HSD Facility files are delivered as CSV or Excel. The first row is a header row. Column order is not guaranteed — match by header name (case-insensitive comparison recommended).

| HSD Header                               | DB Column                | Type              | Notes                                          |
| ---------------------------------------- | ------------------------ | ----------------- | ---------------------------------------------- |
| `SSA State/County Code`                  | `ssa_state_county_code`  | `TEXT`            |                                                |
| `Facility or Service Type`               | `facility_service_type`  | `TEXT`            |                                                |
| `Facility Specialty Code`                | `facility_specialty_code`| `TEXT`            |                                                |
| `National Provider Identifier (NPI) Number` | `npi`                 | `TEXT`            | 10-digit string; preserve leading zeros        |
| `# of Staffed, Medicare-Certified Beds`  | `staffed_beds`           | `INTEGER` / `NULL`| Parse as integer; null if blank or non-numeric |
| `Facility Name`                          | `facility_name`          | `TEXT`            |                                                |
| `Provider Street Address`                | `street_address`         | `TEXT`            |                                                |
| `Provider City`                          | `city`                   | `TEXT`            |                                                |
| `Provider State Code`                    | `state_code`             | `TEXT`            | 2-letter abbreviation                          |
| `Provider Zip Code`                      | `zip_code`               | `TEXT`            | Preserve leading zeros (e.g. `06001`)          |

All data columns are nullable — treat empty or missing cells as `NULL` rather than raising an error.

#### Geocoding

After parsing each row, geocode the address (`street_address`, `city`, `state_code`, `zip_code`) to obtain `latitude` and `longitude`. Store them as `DOUBLE PRECISION`. Leave both `NULL` if geocoding fails — do not fail the entire file.

The database automatically maintains a PostGIS `geography(Point, 4326)` column (`location`) as a generated column computed from `latitude` and `longitude`. **The worker must not write to `location` directly.**

#### Destination table: `org_facilities`

| Column                  | Type              | Source                                   |
| ----------------------- | ----------------- | ---------------------------------------- |
| `id`                    | `TEXT` (UUID)     | Generate with `uuid_generate_v4()` or equivalent |
| `organization_id`       | `TEXT` (UUID)     | `org_files.organization_id`              |
| `source_file_id`        | `TEXT` (UUID)     | `org_files.id` (the `fileId`)            |
| `ssa_state_county_code` | `TEXT`            | Parsed from file                         |
| `facility_service_type` | `TEXT`            | Parsed from file                         |
| `facility_specialty_code` | `TEXT`          | Parsed from file                         |
| `npi`                   | `TEXT`            | Parsed from file                         |
| `staffed_beds`          | `INTEGER`         | Parsed from file                         |
| `facility_name`         | `TEXT`            | Parsed from file                         |
| `street_address`        | `TEXT`            | Parsed from file                         |
| `city`                  | `TEXT`            | Parsed from file                         |
| `state_code`            | `TEXT`            | Parsed from file                         |
| `zip_code`              | `TEXT`            | Parsed from file                         |
| `latitude`              | `DOUBLE PRECISION`| From geocoder; `NULL` if unavailable     |
| `longitude`             | `DOUBLE PRECISION`| From geocoder; `NULL` if unavailable     |
| `location`              | `geography`       | **Auto-generated by DB** — do not insert |
| `created_at`            | `TIMESTAMP`       | Set by DB default                        |
| `updated_at`            | `TIMESTAMP`       | Set by DB default                        |

#### Idempotency for facility rows

Before inserting rows, delete any existing `org_facilities` rows for the same `source_file_id`. This makes reprocessing safe if a job is retried after a crash mid-insert.

```sql
-- Step A: clear any previously inserted rows for this file
DELETE FROM org_facilities WHERE source_file_id = $1;

-- Step B: bulk insert all parsed rows
INSERT INTO org_facilities (
    id, organization_id, source_file_id,
    ssa_state_county_code, facility_service_type, facility_specialty_code,
    npi, staffed_beds, facility_name,
    street_address, city, state_code, zip_code,
    latitude, longitude,
    updated_at
)
VALUES
    ($2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, NOW()),
    -- ... one tuple per row ...
;
```

Use a batch insert rather than row-by-row inserts.

---

### PROVIDER files

#### Source format

HSD Provider files are delivered as CSV or Excel. The first row is a header row. Column order is not guaranteed — match by header name (case-insensitive comparison recommended).

| HSD Header                                  | DB Column                    | Type              | Notes                                          |
| ------------------------------------------- | ---------------------------- | ----------------- | ---------------------------------------------- |
| `SSA State/County Code`                     | `ssa_state_county_code`      | `TEXT`            |                                                |
| `Name of Physician or Mid-Level Practitioner` | `provider_name`            | `TEXT`            |                                                |
| `National Provider Identifier (NPI) Number` | `npi`                        | `TEXT`            | 10-digit string; preserve leading zeros        |
| `Provider Specialty Code`                   | `provider_specialty_code`    | `TEXT`            |                                                |
| `Contract Type`                             | `contract_type`              | `TEXT`            |                                                |
| `Provider Street Address`                   | `street_address`             | `TEXT`            |                                                |
| `Provider City`                             | `city`                       | `TEXT`            |                                                |
| `Provider State Code`                       | `state_code`                 | `TEXT`            | 2-letter abbreviation                          |
| `Provider Zip Code`                         | `zip_code`                   | `TEXT`            | Preserve leading zeros (e.g. `06001`)          |
| `Medical Group Affiliation`                 | `medical_group_affiliation`  | `TEXT`            |                                                |

All data columns are nullable — treat empty or missing cells as `NULL` rather than raising an error.

#### Geocoding

After parsing each row, geocode the address (`street_address`, `city`, `state_code`, `zip_code`) to obtain `latitude` and `longitude`. Store them as `DOUBLE PRECISION`. Leave both `NULL` if geocoding fails — do not fail the entire file.

The database automatically maintains a PostGIS `geography(Point, 4326)` column (`location`) as a generated column computed from `latitude` and `longitude`. **The worker must not write to `location` directly.**

#### Destination table: `org_providers`

| Column                       | Type              | Source                                            |
| ---------------------------- | ----------------- | ------------------------------------------------- |
| `id`                         | `TEXT` (UUID)     | Generate with `uuid_generate_v4()` or equivalent  |
| `organization_id`            | `TEXT` (UUID)     | `org_files.organization_id`                       |
| `source_file_id`             | `TEXT` (UUID)     | `org_files.id` (the `fileId`)                     |
| `ssa_state_county_code`      | `TEXT`            | Parsed from file                                  |
| `provider_name`              | `TEXT`            | Parsed from file                                  |
| `npi`                        | `TEXT`            | Parsed from file                                  |
| `provider_specialty_code`    | `TEXT`            | Parsed from file                                  |
| `contract_type`              | `TEXT`            | Parsed from file                                  |
| `street_address`             | `TEXT`            | Parsed from file                                  |
| `city`                       | `TEXT`            | Parsed from file                                  |
| `state_code`                 | `TEXT`            | Parsed from file                                  |
| `zip_code`                   | `TEXT`            | Parsed from file                                  |
| `latitude`                   | `DOUBLE PRECISION`| From geocoder; `NULL` if unavailable              |
| `longitude`                  | `DOUBLE PRECISION`| From geocoder; `NULL` if unavailable              |
| `medical_group_affiliation`  | `TEXT`            | Parsed from file                                  |
| `location`                   | `geography`       | **Auto-generated by DB** — do not insert          |
| `created_at`                 | `TIMESTAMP`       | Set by DB default                                 |
| `updated_at`                 | `TIMESTAMP`       | Set by DB default                                 |

#### Idempotency for provider rows

Before inserting rows, delete any existing `org_providers` rows for the same `source_file_id`. This makes reprocessing safe if a job is retried after a crash mid-insert.

```sql
-- Step A: clear any previously inserted rows for this file
DELETE FROM org_providers WHERE source_file_id = $1;

-- Step B: bulk insert all parsed rows
INSERT INTO org_providers (
    id, organization_id, source_file_id,
    ssa_state_county_code, provider_name, npi,
    provider_specialty_code, contract_type,
    street_address, city, state_code, zip_code,
    medical_group_affiliation,
    latitude, longitude,
    updated_at
)
VALUES
    ($2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, NOW()),
    -- ... one tuple per row ...
;
```

Use a batch insert rather than row-by-row inserts.
