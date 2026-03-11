"""
Database layer for Databricks SQL queries.
Uses the Databricks SDK for authentication in Databricks Apps.
"""
import os
from databricks.sdk import WorkspaceClient
from databricks import sql as databricks_sql

WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "1b1d59e180e4ac26")


def get_connection():
    """Get a Databricks SQL connection using SDK auth."""
    w = WorkspaceClient()
    host = (w.config.host or "").replace("https://", "").replace("http://", "")

    # Get access token from SDK auth headers
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
    else:
        # Fallback: try credentials_provider
        return databricks_sql.connect(
            server_hostname=host,
            http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
            credentials_provider=lambda: w.config.authenticate,
        )


def execute_sql(statement, params=None):
    """Execute a SQL statement with optional named parameters."""
    with get_connection() as conn:
        with conn.cursor() as cursor:
            if params:
                cursor.execute(statement, params)
            else:
                cursor.execute(statement)


def fetch_sql(statement, params=None):
    """Execute a SQL query and return results as list of dicts."""
    with get_connection() as conn:
        with conn.cursor() as cursor:
            if params:
                cursor.execute(statement, params)
            else:
                cursor.execute(statement)
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
            return results
