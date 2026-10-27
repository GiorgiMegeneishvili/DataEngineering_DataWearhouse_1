import psycopg2
from utils.config import POSTGRES_PG_CONFIG


def call_postgres_procedure(proc_name: str, batch_id: int | None = None):
    conn = psycopg2.connect(**POSTGRES_PG_CONFIG)
    try:
        with conn.cursor() as cur:
            if batch_id is None:
                cur.execute(f"CALL {proc_name}();")
            else:
                cur.execute(f"CALL {proc_name}(%s);", (batch_id,))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
