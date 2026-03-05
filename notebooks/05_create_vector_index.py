# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Create / Sync Vector Search Index
# MAGIC Set up Databricks Vector Search endpoint and Delta Sync index on document_chunks.

# COMMAND ----------

CATALOG = "main"
SCHEMA = "audit_schema"
FQ = f"{CATALOG}.{SCHEMA}"

VS_ENDPOINT = "audit_vs_endpoint"
VS_INDEX = f"{FQ}.chunk_vector_index"
SOURCE_TABLE = f"{FQ}.document_chunks"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Vector Search Endpoint

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()

# Create endpoint (idempotent)
try:
    endpoint = vsc.get_endpoint(VS_ENDPOINT)
    print(f"Endpoint '{VS_ENDPOINT}' already exists. Status: {endpoint.get('endpoint_status', {}).get('state')}")
except Exception:
    print(f"Creating Vector Search endpoint: {VS_ENDPOINT}")
    vsc.create_endpoint(name=VS_ENDPOINT, endpoint_type="STANDARD")
    print("Endpoint creation initiated. This may take a few minutes.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wait for Endpoint to be Ready

# COMMAND ----------

import time

for i in range(30):  # Wait up to 5 minutes
    endpoint = vsc.get_endpoint(VS_ENDPOINT)
    state = endpoint.get("endpoint_status", {}).get("state", "UNKNOWN")
    if state == "ONLINE":
        print(f"Endpoint is ONLINE.")
        break
    print(f"Endpoint state: {state}. Waiting...")
    time.sleep(10)
else:
    print("WARNING: Endpoint not yet ONLINE after 5 minutes. Index creation may still succeed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Delta Sync Index

# COMMAND ----------

try:
    index = vsc.get_index(VS_ENDPOINT, VS_INDEX)
    print(f"Index '{VS_INDEX}' already exists.")
    # Trigger a sync
    index.sync()
    print("Sync triggered.")
except Exception as e:
    if "NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
        print(f"Creating Delta Sync index: {VS_INDEX}")
        vsc.create_delta_sync_index(
            endpoint_name=VS_ENDPOINT,
            index_name=VS_INDEX,
            source_table_name=SOURCE_TABLE,
            pipeline_type="TRIGGERED",
            primary_key="chunk_id",
            embedding_dimension=1024,
            embedding_vector_column="embedding",
            columns_to_sync=[
                "chunk_id", "document_id", "audit_id",
                "chunk_text", "chunk_index"
            ]
        )
        print("Index creation initiated.")
    else:
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wait for Index to be Ready

# COMMAND ----------

for i in range(60):  # Wait up to 10 minutes
    try:
        index = vsc.get_index(VS_ENDPOINT, VS_INDEX)
        status = index.describe().get("status", {})
        state = status.get("ready", False)
        if state:
            print("Index is READY.")
            num_rows = status.get("num_rows_indexed", "unknown")
            print(f"Rows indexed: {num_rows}")
            break
        print(f"Index not ready yet. Status: {status}. Waiting...")
    except Exception as e:
        print(f"Checking... {e}")
    time.sleep(10)
else:
    print("WARNING: Index not ready after 10 minutes. It may still be syncing.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Vector Search

# COMMAND ----------

# Quick test query
try:
    index = vsc.get_index(VS_ENDPOINT, VS_INDEX)
    results = index.similarity_search(
        query_text="user access provisioning and approval process",
        num_results=3,
        columns=["chunk_id", "document_id", "chunk_text"]
    )
    print("Test query results:")
    for row in results.get("result", {}).get("data_array", []):
        print(f"  chunk_id={row[0]}, doc_id={row[1]}, score={row[-1]:.4f}")
        print(f"  text: {row[2][:100]}...")
        print()
except Exception as e:
    print(f"Test query failed (index may still be syncing): {e}")

print("Vector Search index setup complete.")
