"""
Database layer for Databricks SQL queries.
Uses connection pooling and TTL caching for performance.
"""
import os
import time
import threading
from databricks.sdk import WorkspaceClient
from databricks import sql as databricks_sql

WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "1b1d59e180e4ac26")

# Connection pool: reuse connections instead of creating new ones per request
_conn_pool = []
_pool_lock = threading.Lock()
_workspace_client = None
_ws_lock = threading.Lock()

# Simple TTL cache for read queries
_cache = {}
_cache_lock = threading.Lock()
CACHE_TTL = 30  # seconds


def _get_ws():
    global _workspace_client
    with _ws_lock:
        if _workspace_client is None:
            _workspace_client = WorkspaceClient()
        return _workspace_client


def get_connection():
    """Get a Databricks SQL connection, reusing from pool if available."""
    with _pool_lock:
        if _conn_pool:
            conn = _conn_pool.pop()
            try:
                # Quick health check
                conn.cursor().execute("SELECT 1")
                return conn
            except Exception:
                pass  # Connection stale, create new one

    w = _get_ws()
    host = (w.config.host or "").replace("https://", "").replace("http://", "")
    auth_headers = w.config.authenticate()
    token = None
    if isinstance(auth_headers, dict):
        auth_val = auth_headers.get("Authorization", "")
        if auth_val.startswith("Bearer "):
            token = auth_val.replace("Bearer ", "")

    if token:
        return databricks_sql.connect(
            server_hostname=host,
            http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
            access_token=token,
        )
    return databricks_sql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        credentials_provider=lambda: w.config.authenticate,
    )


def _return_conn(conn):
    """Return connection to pool for reuse."""
    with _pool_lock:
        if len(_conn_pool) < 3:  # Max pool size
            _conn_pool.append(conn)
        else:
            try:
                conn.close()
            except Exception:
                pass


def execute_sql(statement, params=None):
    """Execute a SQL statement."""
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(statement, params or {})
        # Invalidate cache on writes
        with _cache_lock:
            _cache.clear()
    finally:
        _return_conn(conn)


def fetch_sql(statement, params=None, cache_key=None):
    """Execute a SQL query and return results as list of dicts. Supports TTL caching."""
    # Check cache
    if cache_key:
        with _cache_lock:
            if cache_key in _cache:
                val, ts = _cache[cache_key]
                if time.time() - ts < CACHE_TTL:
                    return val

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(statement, params or {})
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            results = []
            for row in rows:
                d = {}
                for col, val in zip(columns, row):
                    if isinstance(val, (list, bytes)):
                        d[col] = str(val)
                    elif hasattr(val, 'isoformat'):
                        d[col] = val.isoformat()
                    else:
                        d[col] = val
                results.append(d)
    finally:
        _return_conn(conn)

    # Store in cache
    if cache_key:
        with _cache_lock:
            _cache[cache_key] = (results, time.time())

    return results
