# Databricks notebook source
# Test what works on serverless for Vector Search

# COMMAND ----------

print("Test 1: Import requests")
import requests
print("  OK")

# COMMAND ----------

print("Test 2: Databricks SDK")
try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    print(f"  Host: {w.config.host}")
    print(f"  Auth type: {w.config.auth_type}")
    headers = {}
    w.config.authenticate(headers)
    print(f"  Auth header keys: {list(headers.keys())}")
    print(f"  OK")
except Exception as e:
    print(f"  FAILED: {e}")

# COMMAND ----------

print("Test 3: VS query via SDK")
try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    host = w.config.host
    if not host.startswith("http"):
        host = f"https://{host}"
    headers = {}
    w.config.authenticate(headers)
    headers["Content-Type"] = "application/json"

    # Get an embedding from controls
    emb_row = spark.sql("""
        SELECT CAST(embedding AS ARRAY<FLOAT>) AS emb
        FROM main.audit_schema.controls
        WHERE embedding IS NOT NULL LIMIT 1
    """).collect()[0]
    emb = [float(v) for v in emb_row["emb"]]
    print(f"  Embedding dim: {len(emb)}")

    resp = requests.post(
        f"{host}/api/2.0/vector-search/indexes/main.audit_schema.evidence_chunk_index/query",
        headers=headers,
        json={"query_vector": emb, "columns": ["chunk_id", "audit_id"], "num_results": 3}
    )
    print(f"  Status: {resp.status_code}")
    print(f"  Results: {len(resp.json().get('result',{}).get('data_array',[]))}")
except Exception as e:
    print(f"  FAILED: {e}")

# COMMAND ----------

print("All tests done")
