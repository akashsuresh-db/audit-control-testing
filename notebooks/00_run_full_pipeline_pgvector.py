# Databricks notebook source
# MAGIC %md
# MAGIC # Audit Control Evaluation Pipeline (pgvector / Lakebase)
# MAGIC
# MAGIC End-to-end pipeline replacing Databricks Vector Search with pgvector on Lakebase.
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. Parse evidence documents (ai_parse_document)
# MAGIC 2. Chunk documents with char offsets
# MAGIC 3. Generate embeddings (BGE-Large-EN)
# MAGIC 4. Store embeddings in Lakebase pgvector
# MAGIC 5. Similarity search via pgvector
# MAGIC 6. LLM evaluation + annotation generation

# COMMAND ----------

# Parameters
dbutils.widgets.text("audit_id", "", "Audit ID")
audit_id = dbutils.widgets.get("audit_id")
assert audit_id, "audit_id is required"

CATALOG = "main"
SCHEMA = "audit_schema"
FQ = f"{CATALOG}.{SCHEMA}"

# Lakebase config
LAKEBASE_INSTANCE = "akash-finance-app"
LAKEBASE_HOST = "instance-383773af-2ab5-4bfd-971d-9dba95011ab4.database.cloud.databricks.com"
LAKEBASE_DB = "audit_platform"
LAKEBASE_PORT = 5432

print(f"Pipeline started for audit: {audit_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Lakebase Connection

# COMMAND ----------

import json
import uuid
import re
import time
import requests
import psycopg2
import psycopg2.extras

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
host = w.config.host

def get_lakebase_conn():
    """Get authenticated connection to Lakebase."""
    token_resp = requests.post(
        f"{host}/api/2.0/database/credentials",
        headers=w.config.authenticate(),
        json={"request_id": "pipeline", "instance_names": [LAKEBASE_INSTANCE]}
    )
    token = token_resp.json()["token"]
    return psycopg2.connect(
        host=LAKEBASE_HOST,
        port=LAKEBASE_PORT,
        database=LAKEBASE_DB,
        user=w.config.authenticate().get("Authorization", "").replace("Bearer ", "").split("@")[0] + "@databricks.com",
        password=token,
        sslmode="require"
    )

# Quick test
conn = get_lakebase_conn()
cur = conn.cursor()
cur.execute("SELECT 1 AS test")
print(f"Lakebase connection OK: {cur.fetchone()}")
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Parse Evidence Documents

# COMMAND ----------

# Get unparsed documents from Databricks (source of truth for files)
unparsed = spark.sql(f"""
    SELECT document_id, original_filename, file_type, file_path
    FROM {FQ}.evidence_documents
    WHERE audit_id = '{audit_id}' AND parse_status = 'PENDING_AI_PARSE'
""").collect()

print(f"Documents to parse: {len(unparsed)}")

warehouse_id = spark.conf.get("spark.databricks.warehouse.id", "1b1d59e180e4ac26")

for doc in unparsed:
    doc_id = doc["document_id"]
    fpath = doc["file_path"]
    fname = doc["original_filename"]
    ftype = doc["file_type"]

    print(f"  Parsing: {fname} ({ftype})")

    content_type_map = {
        "pdf": "application/pdf", "png": "image/png", "jpg": "image/jpeg",
        "jpeg": "image/jpeg", "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    }
    ct = content_type_map.get(ftype, "application/octet-stream")

    try:
        sql_stmt = f"""
            SELECT ai_parse_document(
                content, map('contentType', '{ct}')
            ) AS parsed
            FROM read_files('{fpath}')
        """
        resp = requests.post(
            f"{host}/api/2.0/sql/statements",
            headers=w.config.authenticate(),
            json={
                "warehouse_id": warehouse_id,
                "statement": sql_stmt,
                "wait_timeout": "120s",
            },
        )
        result = resp.json()

        if result.get("status", {}).get("state") == "SUCCEEDED":
            parsed_json = json.loads(result["result"]["data_array"][0][0])
            elements = parsed_json.get("document", {}).get("elements", [])
            full_text = "\n\n".join(el.get("content", "") for el in elements if el.get("content"))

            spark.sql(f"""
                UPDATE {FQ}.evidence_documents
                SET extracted_text = '{full_text.replace("'", "''")}',
                    parse_status = 'COMPLETED'
                WHERE document_id = '{doc_id}'
            """)
            print(f"    OK: {len(full_text)} chars extracted")
        else:
            print(f"    WARN: Parse returned state {result.get('status', {}).get('state')}")

    except Exception as e:
        print(f"    ERROR: {e}")
        spark.sql(f"""
            UPDATE {FQ}.evidence_documents
            SET parse_status = 'FAILED', parse_error = '{str(e)[:500].replace("'", "''")}'
            WHERE document_id = '{doc_id}'
        """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Chunk Documents with Character Offsets

# COMMAND ----------

def paragraph_aware_chunk_with_offsets(text, max_size=1500, overlap=200):
    """Chunk text preserving paragraph boundaries AND tracking char offsets."""
    paragraphs = re.split(r'\n\s*\n', text)
    chunks = []
    current_chunk = ""
    current_start = 0
    pos = 0  # Track position in original text

    for para in paragraphs:
        # Find actual position of this paragraph in original text
        para_start = text.find(para, pos)
        if para_start == -1:
            para_start = pos
        para_end = para_start + len(para)

        if len(current_chunk) + len(para) + 2 <= max_size:
            if current_chunk:
                current_chunk += "\n\n" + para
            else:
                current_chunk = para
                current_start = para_start
        else:
            if current_chunk:
                chunk_end = current_start + len(current_chunk)
                chunks.append({
                    "text": current_chunk,
                    "start_char": current_start,
                    "end_char": chunk_end,
                })
            # Start new chunk with overlap
            if overlap > 0 and current_chunk:
                overlap_text = current_chunk[-overlap:]
                current_chunk = overlap_text + "\n\n" + para
                current_start = chunk_end - overlap
            else:
                current_chunk = para
                current_start = para_start

        pos = para_end

    if current_chunk:
        chunks.append({
            "text": current_chunk,
            "start_char": current_start,
            "end_char": current_start + len(current_chunk),
        })

    return chunks

# Chunk all parsed documents
docs = spark.sql(f"""
    SELECT document_id, extracted_text
    FROM {FQ}.evidence_documents
    WHERE audit_id = '{audit_id}' AND parse_status = 'COMPLETED' AND extracted_text IS NOT NULL
""").collect()

# Clear existing chunks for re-run
conn = get_lakebase_conn()
cur = conn.cursor()
cur.execute("DELETE FROM document_chunks WHERE audit_id = %s", (audit_id,))
conn.commit()

total_chunks = 0
for doc in docs:
    text = doc["extracted_text"]
    if not text or len(text.strip()) < 10:
        continue

    chunks = paragraph_aware_chunk_with_offsets(text)

    for i, chunk in enumerate(chunks):
        chunk_id = str(uuid.uuid4())
        cur.execute(
            """INSERT INTO document_chunks (chunk_id, document_id, audit_id, chunk_index, chunk_text, start_char, end_char, token_count)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (chunk_id, doc["document_id"], audit_id, i, chunk["text"],
             chunk["start_char"], chunk["end_char"], len(chunk["text"].split())),
        )
        total_chunks += 1

conn.commit()
conn.close()
print(f"Created {total_chunks} chunks from {len(docs)} documents (with char offsets)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Generate Embeddings (BGE-Large-EN via ai_query)

# COMMAND ----------

# Generate embeddings for chunks using SQL ai_query
# We batch them through Spark SQL for efficiency

# First, get chunk texts from Lakebase
conn = get_lakebase_conn()
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
cur.execute("SELECT chunk_id::text, chunk_text FROM document_chunks WHERE audit_id = %s AND embedding IS NULL", (audit_id,))
chunks_to_embed = cur.fetchall()
conn.close()

print(f"Generating embeddings for {len(chunks_to_embed)} chunks...")

# Use ai_query via SQL Statement API for embedding generation
batch_size = 10
for i in range(0, len(chunks_to_embed), batch_size):
    batch = chunks_to_embed[i:i+batch_size]
    for chunk in batch:
        clean_text = chunk["chunk_text"].replace("'", "''")[:4000]
        try:
            sql = f"""
                SELECT CAST(ai_query('databricks-bge-large-en', '{clean_text}') AS ARRAY<FLOAT>) AS emb
            """
            resp = requests.post(
                f"{host}/api/2.0/sql/statements",
                headers=w.config.authenticate(),
                json={"warehouse_id": warehouse_id, "statement": sql, "wait_timeout": "60s"},
            )
            result = resp.json()
            if result.get("status", {}).get("state") == "SUCCEEDED":
                emb_str = result["result"]["data_array"][0][0]
                # Parse the embedding array
                emb_vals = json.loads(emb_str)

                # Store in Lakebase pgvector
                conn2 = get_lakebase_conn()
                cur2 = conn2.cursor()
                emb_pg = "[" + ",".join(str(v) for v in emb_vals) + "]"
                cur2.execute(
                    "UPDATE document_chunks SET embedding = %s::vector WHERE chunk_id = %s",
                    (emb_pg, chunk["chunk_id"]),
                )
                conn2.commit()
                conn2.close()
        except Exception as e:
            print(f"  Embed error for {chunk['chunk_id'][:8]}: {e}")

    print(f"  Embedded batch {i//batch_size + 1}/{(len(chunks_to_embed)-1)//batch_size + 1}")

# Also generate embeddings for controls
controls = spark.sql(f"""
    SELECT control_id, control_description
    FROM {FQ}.controls
    WHERE audit_id = '{audit_id}'
""").collect()

print(f"Generating embeddings for {len(controls)} controls...")
control_embeddings = {}

for ctrl in controls:
    clean = ctrl["control_description"].replace("'", "''")[:4000]
    try:
        sql = f"SELECT CAST(ai_query('databricks-bge-large-en', '{clean}') AS ARRAY<FLOAT>) AS emb"
        resp = requests.post(
            f"{host}/api/2.0/sql/statements",
            headers=w.config.authenticate(),
            json={"warehouse_id": warehouse_id, "statement": sql, "wait_timeout": "60s"},
        )
        result = resp.json()
        if result.get("status", {}).get("state") == "SUCCEEDED":
            emb_vals = json.loads(result["result"]["data_array"][0][0])
            control_embeddings[ctrl["control_id"]] = emb_vals

            # Also store in Lakebase
            conn2 = get_lakebase_conn()
            cur2 = conn2.cursor()
            emb_pg = "[" + ",".join(str(v) for v in emb_vals) + "]"
            cur2.execute(
                "UPDATE controls SET embedding = %s::vector WHERE control_id = %s",
                (emb_pg, ctrl["control_id"]),
            )
            conn2.commit()
            conn2.close()
    except Exception as e:
        print(f"  Control embed error: {e}")

print(f"Embeddings generated: {len(control_embeddings)} controls")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: pgvector Similarity Search (replaces Databricks Vector Search)

# COMMAND ----------

# Clear existing matches for re-run
conn = get_lakebase_conn()
cur = conn.cursor()
cur.execute("DELETE FROM control_evidence_matches WHERE audit_id = %s", (audit_id,))
conn.commit()

total_matches = 0

for ctrl in controls:
    ctrl_id = ctrl["control_id"]
    if ctrl_id not in control_embeddings:
        continue

    emb = control_embeddings[ctrl_id]
    emb_str = "[" + ",".join(str(v) for v in emb) + "]"

    # pgvector cosine similarity search
    cur2 = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur2.execute("""
        SELECT
            chunk_id::text, document_id::text, audit_id, chunk_text,
            chunk_index, start_char, end_char,
            1 - (embedding <=> %s::vector) AS similarity_score
        FROM document_chunks
        WHERE audit_id = %s AND embedding IS NOT NULL
        ORDER BY embedding <=> %s::vector
        LIMIT 15
    """, (emb_str, audit_id, emb_str))

    matches = cur2.fetchall()

    for rank, match in enumerate(matches, 1):
        score = float(match["similarity_score"])
        if score < 0.4:
            continue

        match_id = str(uuid.uuid4())
        cur.execute(
            """INSERT INTO control_evidence_matches
               (match_id, control_id, chunk_id, document_id, audit_id, similarity_score, match_rank)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (match_id, ctrl_id, match["chunk_id"], match["document_id"], audit_id, score, rank),
        )
        total_matches += 1

conn.commit()
conn.close()
print(f"pgvector similarity matching complete: {total_matches} matches stored")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: LLM Evaluation (Chain-of-Thought)

# COMMAND ----------

# Build evaluation input: controls + their matched evidence
conn = get_lakebase_conn()
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# Get controls with their top evidence
cur.execute("""
    SELECT c.control_id::text, c.control_code, c.control_description, c.risk_level,
           STRING_AGG(
               'Source: ' || ed.original_filename || ' (similarity: ' || ROUND(m.similarity_score::numeric, 3)::text || ')'
               || E'\n' || dc.chunk_text,
               E'\n---\n' ORDER BY m.match_rank
           ) AS evidence_text
    FROM controls c
    LEFT JOIN control_evidence_matches m ON c.control_id = m.control_id
        AND m.similarity_score >= 0.4 AND m.match_rank <= 8
    LEFT JOIN document_chunks dc ON m.chunk_id = dc.chunk_id
    LEFT JOIN evidence_documents ed ON m.document_id = ed.document_id
    WHERE c.audit_id = %s
    GROUP BY c.control_id, c.control_code, c.control_description, c.risk_level
""", (audit_id,))

eval_inputs = cur.fetchall()
conn.close()

print(f"Evaluating {len(eval_inputs)} controls...")

# Clear existing results
conn = get_lakebase_conn()
cur = conn.cursor()
cur.execute("DELETE FROM evaluation_results WHERE audit_id = %s", (audit_id,))
cur.execute("DELETE FROM annotations WHERE audit_id = %s", (audit_id,))
conn.commit()

for ctrl in eval_inputs:
    evidence_text = ctrl["evidence_text"] or "No evidence found."

    prompt = f"""You are a senior internal auditor evaluating controls.

## Control Under Test
Code: {ctrl['control_code']}
Description: {ctrl['control_description']}
Risk Level: {ctrl['risk_level']}

## Evidence Provided
{evidence_text}

## Evaluation Instructions
Step 1: Identify what the control requires.
Step 2: For each evidence source, assess whether it demonstrates compliance.
Step 3: Note any gaps or violations found.
Step 4: Determine your verdict.

## Verdict Criteria
PASS: Evidence directly demonstrates the control is operating effectively.
FAIL: Evidence shows the control is not followed or has gaps.
INSUFFICIENT_EVIDENCE: Not enough evidence to make a determination.

Respond ONLY with valid JSON (no markdown):
{{"verdict": "PASS|FAIL|INSUFFICIENT_EVIDENCE", "confidence": 0.0-1.0, "reasoning": "3-5 sentence explanation", "key_findings": ["finding1", "finding2"], "gaps_identified": ["gap1"], "evidence_summary": "1-sentence summary", "violation_quotes": ["exact quote from evidence showing violation"]}}"""

    clean_prompt = prompt.replace("'", "''")

    try:
        sql = f"SELECT ai_query('databricks-meta-llama-3-3-70b-instruct', '{clean_prompt}') AS response"
        resp = requests.post(
            f"{host}/api/2.0/sql/statements",
            headers=w.config.authenticate(),
            json={"warehouse_id": warehouse_id, "statement": sql, "wait_timeout": "120s"},
        )
        result = resp.json()

        if result.get("status", {}).get("state") == "SUCCEEDED":
            raw = result["result"]["data_array"][0][0]
            # Parse JSON from LLM response
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                # Try to extract JSON from response
                match = re.search(r'\{.*\}', raw, re.DOTALL)
                if match:
                    parsed = json.loads(match.group())
                else:
                    parsed = {"verdict": "INSUFFICIENT_EVIDENCE", "confidence": 0.3,
                              "reasoning": raw[:500], "evidence_summary": "Could not parse LLM response"}

            eval_id = str(uuid.uuid4())
            verdict = parsed.get("verdict", "INSUFFICIENT_EVIDENCE")
            confidence = float(parsed.get("confidence", 0.5))
            reasoning = parsed.get("reasoning", "")
            evidence_summary = parsed.get("evidence_summary", "")

            cur.execute(
                """INSERT INTO evaluation_results
                   (evaluation_id, control_id, audit_id, ai_verdict, ai_confidence,
                    ai_reasoning, evidence_summary, model_used, prompt_version)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (eval_id, ctrl["control_id"], audit_id, verdict, confidence,
                 reasoning, evidence_summary,
                 "databricks-meta-llama-3-3-70b-instruct", "v5.0-pgvector"),
            )

            # Generate annotations for violations
            if verdict == "FAIL":
                violation_quotes = parsed.get("violation_quotes", [])
                _create_annotations(conn, cur, ctrl, audit_id, violation_quotes, confidence)

            print(f"  {ctrl['control_code']}: {verdict} ({confidence:.0%})")
        else:
            print(f"  {ctrl['control_code']}: LLM eval failed")

    except Exception as e:
        print(f"  {ctrl['control_code']}: ERROR - {e}")

conn.commit()
conn.close()
print("LLM evaluation complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Generate Annotations for Evidence Highlighting

# COMMAND ----------

def _create_annotations(conn, cur, ctrl, audit_id, violation_quotes, confidence):
    """Create annotations linking violations to exact document locations."""
    # Get the matched chunks for this control
    cur2 = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur2.execute("""
        SELECT dc.chunk_id::text, dc.document_id::text, dc.chunk_text,
               dc.start_char, dc.end_char, m.similarity_score,
               ed.original_filename
        FROM control_evidence_matches m
        JOIN document_chunks dc ON m.chunk_id = dc.chunk_id
        JOIN evidence_documents ed ON m.document_id = ed.document_id
        WHERE m.control_id = %s AND m.audit_id = %s AND m.similarity_score >= 0.5
        ORDER BY m.match_rank LIMIT 5
    """, (ctrl["control_id"], audit_id))

    matched_chunks = cur2.fetchall()

    # For each violation quote, find where it appears in the document
    for quote in violation_quotes:
        if not quote or len(quote) < 10:
            continue
        for chunk in matched_chunks:
            chunk_text = chunk["chunk_text"]
            # Try to find the quote in the chunk
            idx = chunk_text.lower().find(quote.lower()[:50])
            if idx >= 0:
                # Calculate position in original document
                abs_start = chunk["start_char"] + idx
                abs_end = abs_start + min(len(quote), len(chunk_text) - idx)

                ann_id = str(uuid.uuid4())
                cur.execute(
                    """INSERT INTO annotations
                       (annotation_id, control_id, document_id, chunk_id, audit_id,
                        start_char, end_char, similarity_score, explanation_text,
                        control_code, control_title, violation_type)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'VIOLATION')""",
                    (ann_id, ctrl["control_id"], chunk["document_id"], chunk["chunk_id"],
                     audit_id, abs_start, abs_end, float(chunk["similarity_score"]),
                     f"Control {ctrl['control_code']}: {quote[:200]}",
                     ctrl["control_code"], ctrl.get("control_title", ctrl["control_code"])),
                )
                break  # Found match, move to next quote

    # Also create annotations for top matched chunks (even without exact quotes)
    for chunk in matched_chunks[:3]:
        ann_id = str(uuid.uuid4())
        cur.execute(
            """INSERT INTO annotations
               (annotation_id, control_id, document_id, chunk_id, audit_id,
                start_char, end_char, similarity_score, explanation_text,
                control_code, control_title, violation_type)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'POTENTIAL_VIOLATION')""",
            (ann_id, ctrl["control_id"], chunk["document_id"], chunk["chunk_id"],
             audit_id, chunk["start_char"], chunk["end_char"],
             float(chunk["similarity_score"]),
             f"High similarity match for {ctrl['control_code']} from {chunk['original_filename']}",
             ctrl["control_code"], ctrl.get("control_title", ctrl["control_code"])),
        )

# COMMAND ----------

# Update audit status
spark.sql(f"""
    UPDATE {FQ}.audit_engagements
    SET status = 'COMPLETED', updated_at = current_timestamp()
    WHERE audit_id = '{audit_id}'
""")

# Also update in Lakebase
conn = get_lakebase_conn()
cur = conn.cursor()
cur.execute("UPDATE audit_engagements SET status = 'COMPLETED', updated_at = NOW() WHERE audit_id = %s", (audit_id,))
conn.commit()
conn.close()

print(f"Pipeline complete for audit {audit_id}!")
print("Vector search: pgvector (Lakebase)")
print("Annotations: Generated with char offsets for evidence highlighting")
