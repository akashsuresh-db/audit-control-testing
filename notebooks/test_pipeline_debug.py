# Databricks notebook source
# Debug: run each step of the pipeline to find what fails

# COMMAND ----------

audit_id = "AUD-2026-001"
FQ = "main.audit_schema"
print(f"Pipeline for: {audit_id}")

# COMMAND ----------

print("Step 1: Check pending docs")
try:
    pending = spark.sql(f"""
        SELECT document_id, original_filename, file_type, file_path, extracted_text, parse_status
        FROM {FQ}.evidence_documents
        WHERE audit_id = '{audit_id}'
          AND (parse_status IS NULL OR parse_status != 'COMPLETED')
    """).collect()
    print(f"  Pending: {len(pending)}")
    for p in pending:
        print(f"  - {p['original_filename']} status={p['parse_status']} has_text={bool(p['extracted_text'])}")
except Exception as e:
    print(f"  ERROR: {e}")

# COMMAND ----------

print("Step 2: Check chunks needing processing")
try:
    docs = spark.sql(f"""
        SELECT ed.document_id, LENGTH(ed.extracted_text) AS text_len
        FROM {FQ}.evidence_documents ed
        LEFT JOIN (SELECT DISTINCT document_id FROM {FQ}.document_chunks WHERE audit_id = '{audit_id}') dc
            ON ed.document_id = dc.document_id
        WHERE ed.audit_id = '{audit_id}'
          AND ed.extracted_text IS NOT NULL AND LENGTH(ed.extracted_text) > 10
          AND dc.document_id IS NULL
    """).collect()
    print(f"  Docs needing chunking: {len(docs)}")
except Exception as e:
    print(f"  ERROR: {e}")

# COMMAND ----------

print("Step 3: Check embeddings")
try:
    for t in ["controls", "document_chunks"]:
        r = spark.sql(f"SELECT COUNT(*) AS total, COUNT(embedding) AS embedded FROM {FQ}.{t} WHERE audit_id = '{audit_id}'").collect()[0]
        print(f"  {t}: {r['embedded']}/{r['total']} embedded")
except Exception as e:
    print(f"  ERROR: {e}")

# COMMAND ----------

print("Step 4: Test WorkspaceClient + requests")
try:
    from databricks.sdk import WorkspaceClient
    import requests
    w = WorkspaceClient()
    host = w.config.host
    if not host.startswith("http"):
        host = f"https://{host}"
    headers = {"Content-Type": "application/json"}
    w.config.authenticate(headers)
    print(f"  Host: {host}")
    print(f"  Headers: {list(headers.keys())}")

    # Quick VS query
    emb = spark.sql(f"SELECT CAST(embedding AS ARRAY<FLOAT>) AS emb FROM {FQ}.controls WHERE audit_id = '{audit_id}' AND embedding IS NOT NULL LIMIT 1").collect()[0]["emb"]
    emb_list = [float(v) for v in emb]

    resp = requests.post(
        f"{host}/api/2.0/vector-search/indexes/{FQ}.evidence_chunk_index/query",
        headers=headers,
        json={"query_vector": emb_list, "columns": ["chunk_id", "audit_id"], "filters": {"audit_id": audit_id}, "num_results": 3}
    )
    print(f"  VS response: {resp.status_code}")
    data = resp.json().get("result", {}).get("data_array", [])
    print(f"  Matches: {len(data)}")
    for d in data:
        print(f"    audit_id={d[1]} score={d[-1]:.4f}")
except Exception as e:
    print(f"  ERROR: {e}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

print("All debug steps done!")
