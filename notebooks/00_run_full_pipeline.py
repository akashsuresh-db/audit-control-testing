# Databricks notebook source
# MAGIC %md
# MAGIC # Full Pipeline - Audit Control Evaluation
# MAGIC ## v2.0 — AI Parse + Vector Search + Chain-of-Thought LLM

# COMMAND ----------

dbutils.widgets.text("audit_id", "AUD-2026-001", "Audit ID")
audit_id = dbutils.widgets.get("audit_id")
FQ = "main.audit_schema"
EMBEDDING_MODEL = "databricks-bge-large-en"
LLM_MODEL = "databricks-meta-llama-3-3-70b-instruct"
VS_INDEX = f"{FQ}.evidence_chunk_index"
print(f"Pipeline v2.0 for: {audit_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: AI Parse Documents (PDFs, Images, DOCX)

# COMMAND ----------

import requests, time, uuid, json, re
from databricks.sdk import WorkspaceClient

# Parse documents that need AI parsing (PDF, DOCX, images)
pending_parse = spark.sql(f"""
    SELECT document_id, file_path, file_type, original_filename
    FROM {FQ}.evidence_documents
    WHERE audit_id = '{audit_id}'
      AND parse_status IN ('PENDING', 'PENDING_AI_PARSE')
      AND file_type IN ('pdf', 'png', 'jpg', 'jpeg', 'doc', 'docx', 'ppt', 'pptx')
""").collect()
print(f"Documents needing AI parse: {len(pending_parse)}")

if pending_parse:
    ct_map = {
        "pdf": "application/pdf", "png": "image/png", "jpg": "image/jpeg",
        "jpeg": "image/jpeg", "doc": "application/msword",
        "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "ppt": "application/vnd.ms-powerpoint",
        "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    }

    # ai_parse_document runs on SQL Warehouse, not serverless notebook runtime
    # So we call it via the SQL Statement API
    w = WorkspaceClient()
    host = w.config.host if w.config.host.startswith("http") else f"https://{w.config.host}"
    parse_headers = {"Content-Type": "application/json"}
    auth_result = w.config.authenticate()
    if isinstance(auth_result, dict):
        parse_headers.update(auth_result)
    else:
        auth_result(parse_headers)

    WAREHOUSE_ID = "1b1d59e180e4ac26"

    for doc in pending_parse:
        try:
            ct = ct_map.get(doc["file_type"], "application/octet-stream")
            fp = doc["file_path"]
            print(f"  Parsing {doc['original_filename']}...")

            # Call ai_parse_document via SQL Statement API (runs on SQL Warehouse)
            stmt = f"SELECT ai_parse_document(content, map('contentType', '{ct}')) AS parsed FROM read_files('{fp}', format => 'BINARYFILE')"
            resp = requests.post(
                f"{host}/api/2.0/sql/statements",
                headers=parse_headers,
                json={"warehouse_id": WAREHOUSE_ID, "statement": stmt, "wait_timeout": "50s"}
            )
            resp_data = resp.json()
            state = resp_data.get("status", {}).get("state")

            if state == "SUCCEEDED":
                parsed = json.loads(resp_data["result"]["data_array"][0][0])
                errors = parsed.get("error_status", [])
                if errors:
                    print(f"    Parse warnings: {[e.get('error_message','')[:80] for e in errors]}")
                elements = parsed.get("document", {}).get("elements", [])
                extracted = "\n".join(
                    e.get("content", "") for e in elements if e.get("content")
                )
                if extracted and len(extracted) > 10:
                    safe_text = extracted.replace("'", "''")
                    spark.sql(f"UPDATE {FQ}.evidence_documents SET extracted_text = '{safe_text}', parse_status = 'COMPLETED' WHERE document_id = '{doc['document_id']}'")
                    print(f"    OK: {len(extracted)} chars, {len(elements)} elements")
                else:
                    spark.sql(f"UPDATE {FQ}.evidence_documents SET parse_status = 'NO_CONTENT' WHERE document_id = '{doc['document_id']}'")
                    print(f"    No content extracted")
            else:
                err_msg = resp_data.get("status", {}).get("error", {}).get("message", "Unknown error")
                print(f"    SQL failed ({state}): {err_msg[:150]}")
                spark.sql(f"UPDATE {FQ}.evidence_documents SET parse_status = 'FAILED' WHERE document_id = '{doc['document_id']}'")

        except Exception as e:
            print(f"    FAILED: {type(e).__name__}: {e}")
            try:
                spark.sql(f"UPDATE {FQ}.evidence_documents SET parse_status = 'FAILED' WHERE document_id = '{doc['document_id']}'")
            except Exception:
                pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Parse Text Docs & Chunk

# COMMAND ----------

from pyspark.sql import functions as F

# Update text doc parse status
pending_text = spark.sql(f"""
    SELECT document_id, extracted_text
    FROM {FQ}.evidence_documents
    WHERE audit_id = '{audit_id}'
      AND parse_status NOT IN ('COMPLETED', 'NO_CONTENT', 'FAILED')
      AND file_type IN ('txt', 'csv', 'rtf')
""").collect()
print(f"Text docs to update: {len(pending_text)}")
for doc in pending_text:
    if doc["extracted_text"] and len(doc["extracted_text"]) > 10:
        spark.sql(f"UPDATE {FQ}.evidence_documents SET parse_status = 'COMPLETED' WHERE document_id = '{doc['document_id']}'")
    else:
        spark.sql(f"UPDATE {FQ}.evidence_documents SET parse_status = 'NO_CONTENT' WHERE document_id = '{doc['document_id']}'")

# COMMAND ----------

# Paragraph-aware chunking
docs_to_chunk = spark.sql(f"""
    SELECT ed.document_id, ed.extracted_text
    FROM {FQ}.evidence_documents ed
    LEFT JOIN (SELECT DISTINCT document_id FROM {FQ}.document_chunks WHERE audit_id = '{audit_id}') dc
        ON ed.document_id = dc.document_id
    WHERE ed.audit_id = '{audit_id}'
      AND ed.extracted_text IS NOT NULL AND LENGTH(ed.extracted_text) > 10
      AND dc.document_id IS NULL
""").collect()
print(f"Docs to chunk: {len(docs_to_chunk)}")

def paragraph_aware_chunk(text, max_size=1500, overlap=200):
    """Split text by paragraphs, then combine into chunks respecting boundaries."""
    paragraphs = re.split(r'\n\s*\n|\n(?=[A-Z0-9][\.\)]|\d+\.|\-\s)', text)
    paragraphs = [p.strip() for p in paragraphs if p.strip()]

    chunks = []
    current = ""
    for para in paragraphs:
        if len(current) + len(para) + 1 > max_size and current:
            chunks.append(current.strip())
            overlap_text = current[-overlap:] if len(current) > overlap else current
            current = overlap_text + "\n" + para
        else:
            current = current + "\n" + para if current else para
    if current.strip():
        chunks.append(current.strip())

    if len(chunks) <= 1 and len(text) > max_size:
        chunks = []
        start = 0
        while start < len(text):
            end = min(start + max_size, len(text))
            ct = text[start:end].strip()
            if ct:
                chunks.append(ct)
            start = end - overlap if end < len(text) else len(text)

    return chunks

if docs_to_chunk:
    all_chunks = []
    for doc in docs_to_chunk:
        text = doc["extracted_text"]
        doc_chunks = paragraph_aware_chunk(text)
        for idx, ct in enumerate(doc_chunks):
            all_chunks.append({
                "chunk_id": str(uuid.uuid4()),
                "document_id": doc["document_id"],
                "audit_id": audit_id,
                "chunk_index": idx,
                "chunk_text": ct,
            })
    if all_chunks:
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        chunk_schema = StructType([
            StructField("chunk_id", StringType()), StructField("document_id", StringType()),
            StructField("audit_id", StringType()), StructField("chunk_index", IntegerType()),
            StructField("chunk_text", StringType())
        ])
        spark.createDataFrame(all_chunks, schema=chunk_schema).write.mode("append").saveAsTable(f"{FQ}.document_chunks")
        print(f"Created {len(all_chunks)} chunks from {len(docs_to_chunk)} docs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Generate Embeddings

# COMMAND ----------

for table, col, key in [("controls", "control_description", "control_id"), ("document_chunks", "chunk_text", "chunk_id")]:
    cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {FQ}.{table} WHERE audit_id = '{audit_id}' AND embedding IS NULL").collect()[0]['c']
    print(f"{table}: {cnt} need embeddings")
    if cnt > 0:
        spark.sql(f"""
            MERGE INTO {FQ}.{table} AS t
            USING (SELECT {key}, CAST(ai_query('{EMBEDDING_MODEL}', {col}) AS ARRAY<FLOAT>) AS embedding FROM {FQ}.{table} WHERE embedding IS NULL AND audit_id = '{audit_id}') AS s
            ON t.{key} = s.{key} WHEN MATCHED THEN UPDATE SET t.embedding = s.embedding
        """)
        print(f"  Done")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Vector Search Semantic Matching

# COMMAND ----------

w = WorkspaceClient()
host = w.config.host if w.config.host.startswith("http") else f"https://{w.config.host}"
vs_headers = {"Content-Type": "application/json"}
auth_result = w.config.authenticate()
if isinstance(auth_result, dict):
    vs_headers.update(auth_result)
else:
    auth_result(vs_headers)
print(f"API host: {host}, Auth headers: {list(vs_headers.keys())}")

# Sync index
print("Syncing VS index...")
requests.post(f"{host}/api/2.0/vector-search/indexes/{VS_INDEX}/sync", headers=vs_headers)
for i in range(60):
    r = requests.get(f"{host}/api/2.0/vector-search/indexes/{VS_INDEX}", headers=vs_headers).json()
    if r.get("status", {}).get("ready"):
        print(f"  Ready ({r['status'].get('indexed_row_count','?')} rows)")
        break
    if i % 6 == 0:
        print(f"  Waiting... {r.get('status',{}).get('detailed_state','?')}")
    time.sleep(10)

# COMMAND ----------

# Query Vector Search for each control
controls = spark.sql(f"SELECT control_id, control_code, CAST(embedding AS ARRAY<FLOAT>) AS embedding FROM {FQ}.controls WHERE audit_id = '{audit_id}' AND embedding IS NOT NULL").collect()
print(f"Matching {len(controls)} controls...")
spark.sql(f"DELETE FROM {FQ}.control_evidence_matches WHERE audit_id = '{audit_id}'")

matches = []
for row in controls:
    try:
        qv = [float(v) for v in row["embedding"]]
        resp = requests.post(f"{host}/api/2.0/vector-search/indexes/{VS_INDEX}/query", headers=vs_headers,
            json={"query_vector": qv, "columns": ["chunk_id", "document_id", "audit_id", "chunk_text"], "filters": {"audit_id": audit_id}, "num_results": 15})
        data = resp.json().get("result", {}).get("data_array", [])
        for rank, m in enumerate(data, 1):
            matches.append({"match_id": str(uuid.uuid4()), "control_id": row["control_id"], "chunk_id": m[0], "document_id": m[1], "audit_id": audit_id, "similarity_score": float(m[-1]), "match_rank": rank})
        best = f"{data[0][-1]:.4f}" if data else "none"
        print(f"  {row['control_code']}: {len(data)} matches (best={best})")
    except Exception as e:
        print(f"  {row['control_code']}: ERROR - {e}")

if matches:
    from pyspark.sql.functions import current_timestamp
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
    schema = StructType([
        StructField("match_id", StringType()), StructField("control_id", StringType()),
        StructField("chunk_id", StringType()), StructField("document_id", StringType()),
        StructField("audit_id", StringType()), StructField("similarity_score", DoubleType()),
        StructField("match_rank", IntegerType())
    ])
    spark.createDataFrame(matches, schema=schema).withColumn("_matched_at", current_timestamp()).write.mode("append").saveAsTable(f"{FQ}.control_evidence_matches")
    print(f"Saved {len(matches)} matches")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: LLM Evaluation (Chain-of-Thought)

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW eval_input AS
    SELECT c.control_id, c.audit_id, c.control_code, c.control_description, c.risk_level,
        CONCAT_WS('\n---\n', COLLECT_LIST(CONCAT('Source: ', ed.original_filename, ' (similarity: ', ROUND(m.similarity_score, 3), ')\n', dc.chunk_text))) AS evidence_text,
        COLLECT_LIST(dc.chunk_id) AS chunk_ids, COLLECT_LIST(DISTINCT m.document_id) AS doc_ids, COUNT(m.chunk_id) AS match_count
    FROM {FQ}.controls c
    LEFT JOIN {FQ}.control_evidence_matches m ON c.control_id = m.control_id AND m.similarity_score >= 0.4 AND m.match_rank <= 8
    LEFT JOIN {FQ}.document_chunks dc ON m.chunk_id = dc.chunk_id
    LEFT JOIN {FQ}.evidence_documents ed ON m.document_id = ed.document_id
    WHERE c.audit_id = '{audit_id}'
    GROUP BY c.control_id, c.audit_id, c.control_code, c.control_description, c.risk_level
""")
display(spark.sql("SELECT control_code, match_count FROM eval_input ORDER BY control_code"))

# COMMAND ----------

print("Running LLM evaluations with chain-of-thought...")
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW llm_results AS
    SELECT control_id, audit_id, control_code, evidence_text, chunk_ids, doc_ids,
        ai_query('{LLM_MODEL}', CONCAT(
            'You are a senior internal auditor with expertise in SOX, PCI-DSS, and ISO 27001 frameworks. ',
            'Evaluate whether the provided evidence satisfies the given control requirement. ',
            'Use ONLY the evidence provided — do not assume or infer anything not explicitly stated. ',
            '',
            '## Control Under Test ',
            'Code: ', control_code, ' ',
            'Description: ', control_description, ' ',
            'Risk Level: ', risk_level, ' ',
            '',
            '## Evidence Provided ',
            COALESCE(evidence_text, '[NO EVIDENCE PROVIDED]'), ' ',
            '',
            '## Evaluation Instructions ',
            'Step 1: Identify what the control requires (specific criteria). ',
            'Step 2: For each criterion, check if the evidence directly addresses it. ',
            'Step 3: Note any gaps where evidence is missing or insufficient. ',
            'Step 4: Determine verdict based on coverage. ',
            '',
            '## Verdict Criteria ',
            '- PASS: Evidence directly and sufficiently demonstrates ALL key aspects of the control. ',
            '- FAIL: Evidence exists but contradicts the control or shows non-compliance. ',
            '- INSUFFICIENT_EVIDENCE: Evidence is missing, partial, or does not clearly address the control. ',
            '',
            'Respond with ONLY valid JSON (no markdown): ',
            '{{"verdict": "PASS|FAIL|INSUFFICIENT_EVIDENCE", "confidence": <0.0-1.0>, ',
            '"reasoning": "<3-5 sentences explaining your step-by-step evaluation>", ',
            '"key_findings": ["<finding1>", "<finding2>"], ',
            '"gaps_identified": ["<gap1>", "<gap2>"], ',
            '"evidence_summary": "<1 sentence summarizing what evidence was reviewed>"}}'
        )) AS llm_response
    FROM eval_input
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Save Results

# COMMAND ----------

from pyspark.sql import functions as F
df = spark.sql("SELECT * FROM llm_results")
df_out = (df
    .withColumn("parsed", F.from_json("llm_response", "verdict STRING, confidence DOUBLE, reasoning STRING, key_findings ARRAY<STRING>, gaps_identified ARRAY<STRING>, evidence_summary STRING"))
    .withColumn("evaluation_id", F.expr("uuid()"))
    .withColumn("ai_verdict", F.coalesce("parsed.verdict", F.lit("INSUFFICIENT_EVIDENCE")))
    .withColumn("ai_confidence", F.coalesce("parsed.confidence", F.lit(0.0)))
    .withColumn("ai_reasoning", F.coalesce("parsed.reasoning", "llm_response"))
    .withColumn("evidence_summary", F.col("parsed.evidence_summary"))
    .withColumn("matched_document_ids", F.col("doc_ids")).withColumn("matched_chunk_ids", F.col("chunk_ids"))
    .withColumn("auditor_verdict", F.lit(None).cast("string")).withColumn("auditor_notes", F.lit(None).cast("string"))
    .withColumn("auditor_id", F.lit(None).cast("string")).withColumn("reviewed_at", F.lit(None).cast("timestamp"))
    .withColumn("model_used", F.lit(LLM_MODEL)).withColumn("prompt_version", F.lit("v4.0-cot"))
    .withColumn("evaluated_at", F.current_timestamp()).withColumn("_created_at", F.current_timestamp())
    .select("evaluation_id", "control_id", "audit_id", "ai_verdict", "ai_confidence", "ai_reasoning", "evidence_summary",
            "matched_document_ids", "matched_chunk_ids", "auditor_verdict", "auditor_notes", "auditor_id", "reviewed_at",
            "model_used", "prompt_version", "evaluated_at", "_created_at"))

spark.sql(f"DELETE FROM {FQ}.evaluation_results WHERE audit_id = '{audit_id}'")
df_out.write.mode("append").saveAsTable(f"{FQ}.evaluation_results")
print(f"Saved {df_out.count()} evaluations")

# COMMAND ----------

display(spark.sql(f"""
    SELECT c.control_code, c.risk_level, er.ai_verdict, ROUND(er.ai_confidence, 2) AS confidence,
           er.ai_reasoning, SIZE(er.matched_document_ids) AS docs
    FROM {FQ}.evaluation_results er JOIN {FQ}.controls c ON er.control_id = c.control_id
    WHERE er.audit_id = '{audit_id}'
    ORDER BY CASE er.ai_verdict WHEN 'FAIL' THEN 1 WHEN 'INSUFFICIENT_EVIDENCE' THEN 2 ELSE 3 END
"""))

# COMMAND ----------

print("Pipeline v2.0 complete!")
