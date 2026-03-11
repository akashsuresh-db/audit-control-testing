"""
Lakebase database layer using SQL Statement API.
No native PostgreSQL driver needed - uses Databricks REST API to query Lakebase
via Unity Catalog federated access.
Falls back to direct pg8000 connection if available.
"""
import os
import json
import subprocess
from contextlib import contextmanager
from decimal import Decimal
from typing import Any

# Try pg8000, but fall back to REST API
try:
    import pg8000
    import ssl
    HAS_PG8000 = True
except ImportError:
    HAS_PG8000 = False

LAKEBASE_INSTANCE = os.environ.get("LAKEBASE_INSTANCE", "akash-finance-app")
LAKEBASE_HOST = os.environ.get(
    "LAKEBASE_HOST",
    "instance-383773af-2ab5-4bfd-971d-9dba95011ab4.database.cloud.databricks.com",
)
LAKEBASE_DB = os.environ.get("LAKEBASE_DB", "audit_platform")
LAKEBASE_USER = os.environ.get("LAKEBASE_USER", "akash.s@databricks.com")
LAKEBASE_PORT = int(os.environ.get("LAKEBASE_PORT", "5432"))
DB_PROFILE = os.environ.get("DATABRICKS_PROFILE", "fevm-akash-finance-app")


def _get_auth_headers():
    """Get auth headers for Databricks API calls."""
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        return w.config.authenticate(), w.config.host
    except Exception:
        pass

    # Fallback: try CLI token
    try:
        result = subprocess.run(
            ["databricks", "auth", "token", "-p", DB_PROFILE],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            data = json.loads(result.stdout)
            token = data.get("access_token", "")
            host_result = subprocess.run(
                ["databricks", "auth", "describe", "-p", DB_PROFILE],
                capture_output=True, text=True, timeout=10,
            )
            host = f"https://{LAKEBASE_HOST.split('.')[0].replace('instance-', '')}.cloud.databricks.com"
            return {"Authorization": f"Bearer {token}"}, host
    except Exception:
        pass

    raise RuntimeError("Cannot authenticate to Databricks")


def _get_db_token():
    """Get Lakebase database credential token."""
    import requests as _requests
    auth_headers, host = _get_auth_headers()
    if host and not host.startswith("http"):
        host = f"https://{host}"
    resp = _requests.post(
        f"{host}/api/2.0/database/credentials",
        headers=auth_headers,
        json={"request_id": "app", "instance_names": [LAKEBASE_INSTANCE]},
    )
    if resp.ok:
        return resp.json()["token"]

    # CLI fallback
    try:
        result = subprocess.run(
            ["databricks", "api", "post", "/api/2.0/database/credentials",
             "-p", DB_PROFILE,
             "--json", json.dumps({"request_id": "app", "instance_names": [LAKEBASE_INSTANCE]})],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode == 0:
            return json.loads(result.stdout)["token"]
    except Exception:
        pass

    raise RuntimeError("Cannot get Lakebase token")


def _serialize_value(val):
    """Convert database values to JSON-serializable types."""
    if val is None:
        return None
    if hasattr(val, "isoformat"):
        return val.isoformat()
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="replace")
    return val


# ---- pg8000 direct connection (used when driver is available) ----

if HAS_PG8000:
    @contextmanager
    def get_pg_connection():
        token = _get_db_token()
        ctx = ssl.create_default_context()
        conn = pg8000.connect(
            host=LAKEBASE_HOST, port=LAKEBASE_PORT,
            database=LAKEBASE_DB, user=LAKEBASE_USER,
            password=token, ssl_context=ctx,
        )
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def pg_execute(sql, params=None):
        with get_pg_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, params or ())

    def pg_fetch(sql, params=None):
        with get_pg_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, params or ())
            if cur.description is None:
                return []
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            return [{col: _serialize_value(val) for col, val in zip(columns, row)} for row in rows]

    def pg_fetch_one(sql, params=None):
        rows = pg_fetch(sql, params)
        return rows[0] if rows else None

else:
    # ---- REST API fallback (no PostgreSQL driver needed) ----
    # Uses Databricks SQL warehouse to query Lakebase via Unity Catalog

    def _run_sql(sql, params=None):
        """Execute SQL via Databricks SQL Statement API."""
        import requests as _requests
        auth_headers, host = _get_auth_headers()
        if host and not host.startswith("http"):
            host = f"https://{host}"
        warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "1b1d59e180e4ac26")

        # Replace %s placeholders with actual values for REST API
        if params:
            processed_sql = sql
            for p in params:
                if isinstance(p, str):
                    processed_sql = processed_sql.replace("%s", f"'{p.replace(chr(39), chr(39)+chr(39))}'", 1)
                elif isinstance(p, (int, float)):
                    processed_sql = processed_sql.replace("%s", str(p), 1)
                else:
                    processed_sql = processed_sql.replace("%s", f"'{str(p)}'", 1)
        else:
            processed_sql = sql

        resp = _requests.post(
            f"{host}/api/2.0/sql/statements",
            headers=auth_headers,
            json={
                "warehouse_id": warehouse_id,
                "statement": processed_sql,
                "wait_timeout": "30s",
            },
        )
        return resp.json()

    def pg_execute(sql, params=None):
        result = _run_sql(sql, params)
        if result.get("status", {}).get("state") != "SUCCEEDED":
            msg = result.get("status", {}).get("error", {}).get("message", str(result))
            raise RuntimeError(f"SQL execution failed: {msg}")

    def pg_fetch(sql, params=None):
        result = _run_sql(sql, params)
        if result.get("status", {}).get("state") != "SUCCEEDED":
            msg = result.get("status", {}).get("error", {}).get("message", str(result))
            raise RuntimeError(f"SQL query failed: {msg}")

        manifest = result.get("manifest", {})
        columns = [col["name"] for col in manifest.get("schema", {}).get("columns", [])]
        data_array = result.get("result", {}).get("data_array", [])

        rows = []
        for row_data in data_array:
            row = {}
            for i, col in enumerate(columns):
                val = row_data[i] if i < len(row_data) else None
                row[col] = val
            rows.append(row)
        return rows

    def pg_fetch_one(sql, params=None):
        rows = pg_fetch(sql, params)
        return rows[0] if rows else None


def similarity_search(query_embedding, audit_id, top_k=15, threshold=0.4):
    """Perform cosine similarity search using pgvector."""
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
