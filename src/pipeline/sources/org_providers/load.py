"""DELETE + bulk INSERT org_providers rows for a given source_file_id."""

import psycopg2.extras

_INSERT_SQL = """
    INSERT INTO org_providers (
        id, organization_id, source_file_id,
        ssa_state_county_code, provider_name, npi,
        provider_specialty_code, contract_type,
        street_address, city, state_code, zip_code,
        medical_group_affiliation,
        latitude, longitude, updated_at
    ) VALUES (
        %(id)s, %(organization_id)s, %(source_file_id)s,
        %(ssa_state_county_code)s, %(provider_name)s, %(npi)s,
        %(provider_specialty_code)s, %(contract_type)s,
        %(street_address)s, %(city)s, %(state_code)s, %(zip_code)s,
        %(medical_group_affiliation)s,
        %(latitude)s, %(longitude)s, NOW()
    )
"""


def load(conn, df, source_file_id: str) -> int:
    """Delete existing rows for source_file_id and bulk insert df. Returns inserted row count."""
    rows = df.to_dicts()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM org_providers WHERE source_file_id = %s", (source_file_id,))
        if rows:
            psycopg2.extras.execute_batch(cur, _INSERT_SQL, rows, page_size=500)
    conn.commit()
    return len(rows)
