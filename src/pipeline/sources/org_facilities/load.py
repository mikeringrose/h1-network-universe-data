"""DELETE + bulk INSERT org_facilities rows for a given source_file_id."""

import psycopg2.extras

_INSERT_SQL = """
    INSERT INTO org_facilities (
        id, organization_id, source_file_id,
        ssa_state_county_code, facility_service_type, facility_specialty_code,
        npi, staffed_beds, facility_name,
        street_address, city, state_code, zip_code,
        latitude, longitude, updated_at
    ) VALUES (
        %(id)s, %(organization_id)s, %(source_file_id)s,
        %(ssa_state_county_code)s, %(facility_service_type)s, %(facility_specialty_code)s,
        %(npi)s, %(staffed_beds)s, %(facility_name)s,
        %(street_address)s, %(city)s, %(state_code)s, %(zip_code)s,
        %(latitude)s, %(longitude)s, NOW()
    )
"""


def load(conn, df, source_file_id: str) -> int:
    """Delete existing rows for source_file_id and bulk insert df. Returns inserted row count."""
    rows = df.to_dicts()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM org_facilities WHERE source_file_id = %s", (source_file_id,))
        if rows:
            psycopg2.extras.execute_batch(cur, _INSERT_SQL, rows, page_size=500)
    conn.commit()
    return len(rows)
