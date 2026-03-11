"""
Lakebase (pgvector) database layer.
Connects to Databricks Lakebase PostgreSQL with pgvector for similarity search.
"""
import os
import json
import subprocess
import psycopg2
import psycopg2.extras
from contextlib import contextmanager

LAKEBASE_INSTANCE = os.environ.get("LAKEBASE_INSTANCE", "akash-finance-app")
LAKEBASE_HOST = os.environ.get(
    "LAKEBASE_HOST",
    "instance-383773af-2ab5-4bfd-971d-9dba95011ab4.database.cloud.databricks.com",
)
LAKEBASE_DB = os.environ.get("LAKEBASE_DB", "audit_platform")
LAKEBASE_USER = os.environ.get("LAKEBASE_USER", "akash.s@databricks.com")
LAKEBASE_PORT = int(os.environ.get("LAKEBASE_PORT", "5432"))
DB_PROFILE = os.environ.get("DATABRICKS_PROFILE", "fevm-akash-finance-app")


def _get_token() -> str:
    """Generate OAuth token for Lakebase via Databricks CLI or SDK."""
    # Try Databricks SDK first (for Databricks Apps runtime)
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        import requests
        resp = requests.post(
            f"{w.config.host}/api/2.0/database/credentials",
            headers={"Authorization": f"Bearer {w.config.authenticate().get('Authorization', '').replace('Bearer ', '')}"},
            json={"request_id": "app", "instance_names": [LAKEBASE_INSTANCE]},
        )
        if resp.ok:
            return resp.json()["token"]
    except Exception:
        pass

    # Fallback: Databricks CLI
    try:
        result = subprocess.run(
            [
                "databricks", "api", "post", "/api/2.0/database/credentials",
                "-p", DB_PROFILE,
                "--json", json.dumps({"request_id": "app", "instance_names": [LAKEBASE_INSTANCE]}),
            ],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode == 0:
            return json.loads(result.stdout)["token"]
    except Exception:
        pass

    raise RuntimeError("Cannot generate Lakebase token. Check DATABRICKS_PROFILE or SDK auth.")


@contextmanager
def get_pg_connection():
    """Get a PostgreSQL connection to Lakebase."""
    token = _get_token()
    conn = psycopg2.connect(
        host=LAKEBASE_HOST,
        port=LAKEBASE_PORT,
        database=LAKEBASE_DB,
        user=LAKEBASE_USER,
        password=token,
        sslmode="require",
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def pg_execute(sql: str, params: tuple | list | dict | None = None):
    """Execute SQL statement on Lakebase."""
    with get_pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)


def pg_fetch(sql: str, params: tuple | list | dict | None = None) -> list[dict]:
    """Execute SQL query and return list of dicts."""
    with get_pg_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            results = []
            for row in rows:
                d = {}
                for key, val in dict(row).items():
                    if hasattr(val, "isoformat"):
                        d[key] = val.isoformat()
                    elif isinstance(val, (list,)):
                        d[key] = val
                    else:
                        d[key] = val
                results.append(d)
            return results


def pg_fetch_one(sql: str, params: tuple | list | dict | None = None) -> dict | None:
    """Execute SQL query and return single dict or None."""
    rows = pg_fetch(sql, params)
    return rows[0] if rows else None


def similarity_search(query_embedding: list[float], audit_id: str, top_k: int = 15, threshold: float = 0.4) -> list[dict]:
    """
    Perform cosine similarity search using pgvector.
    Returns top_k most similar document chunks for the given embedding.
    """
    embedding_str = "[" + ",".join(str(f) for f in query_embedding) + "]"
    sql = """
        SELECT
            dc.chunk_id::text,
            dc.document_id::text,
            dc.audit_id,
            dc.chunk_text,
            dc.chunk_index,
            dc.start_char,
            dc.end_char,
            ed.original_filename,
            1 - (dc.embedding <=> %s::vector) AS similarity_score
        FROM document_chunks dc
        JOIN evidence_documents ed ON dc.document_id = ed.document_id
        WHERE dc.audit_id = %s
          AND dc.embedding IS NOT NULL
          AND 1 - (dc.embedding <=> %s::vector) >= %s
        ORDER BY dc.embedding <=> %s::vector
        LIMIT %s
    """
    return pg_fetch(sql, (embedding_str, audit_id, embedding_str, threshold, embedding_str, top_k))
