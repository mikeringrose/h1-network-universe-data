import psycopg2
import psycopg2.extras
from pipeline.worker.models import OrgFile


def get_connection(database_url: str) -> psycopg2.extensions.connection:
    conn = psycopg2.connect(database_url)
    conn.autocommit = False
    return conn


def claim_job(conn: psycopg2.extensions.connection, file_id: str) -> OrgFile | None:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            UPDATE org_files
            SET status = 'PROCESSING', updated_at = NOW()
            WHERE id = %(id)s AND status = 'PENDING'
            RETURNING *
            """,
            {"id": file_id},
        )
        row = cur.fetchone()
        conn.commit()
    if row is None:
        return None
    return OrgFile(**dict(row))


def mark_completed(conn: psycopg2.extensions.connection, file_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE org_files SET status = 'COMPLETED', error_message = NULL, updated_at = NOW() WHERE id = %(id)s",
            {"id": file_id},
        )
    conn.commit()


def mark_failed(conn: psycopg2.extensions.connection, file_id: str, error_message: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE org_files SET status = 'FAILED', error_message = %(msg)s, updated_at = NOW() WHERE id = %(id)s",
            {"id": file_id, "msg": error_message[:500]},
        )
    conn.commit()
