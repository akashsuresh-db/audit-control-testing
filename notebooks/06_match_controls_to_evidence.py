# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Match Controls to Evidence
# MAGIC Use Vector Search to find the most relevant evidence chunks for each control.

# COMMAND ----------

dbutils.widgets.text("audit_id", "", "Audit ID")
audit_id = dbutils.widgets.get("audit_id")

CATALOG = "main"
SCHEMA = "audit_schema"
FQ = f"{CATALOG}.{SCHEMA}"

VS_ENDPOINT = "audit_vs_endpoint"
VS_INDEX = f"{FQ}.chunk_vector_index"
TOP_K = 20  # Retrieve top 20 chunks per control

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Controls

# COMMAND ----------

from pyspark.sql import functions as F
from databricks.vector_search.client import VectorSearchClient
import uuid
from datetime import datetime

vsc = VectorSearchClient()
index = vsc.get_index(VS_ENDPOINT, VS_INDEX)

# Get controls for this audit
controls = (
    spark.table(f"{FQ}.controls")
    .filter(F.col("audit_id") == audit_id if audit_id else F.lit(True))
    .collect()
)

print(f"Controls to match: {len(controls)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Vector Similarity Search

# COMMAND ----------

all_matches = []

for row in controls:
    control_id = row["control_id"]
    aud_id = row["audit_id"]
    control_desc = row["control_description"]

    # Use text-based similarity search (embedding is done server-side)
    try:
        results = index.similarity_search(
            query_text=control_desc,
            num_results=TOP_K,
            columns=["chunk_id", "document_id", "audit_id", "chunk_text"],
            filters={"audit_id": aud_id}  # Scope to same audit engagement
        )

        data_array = results.get("result", {}).get("data_array", [])
        for rank, match in enumerate(data_array, 1):
            chunk_id = match[0]
            document_id = match[1]
            similarity_score = float(match[-1])

            all_matches.append({
                "match_id": str(uuid.uuid4()),
                "control_id": control_id,
                "chunk_id": chunk_id,
                "document_id": document_id,
                "audit_id": aud_id,
                "similarity_score": similarity_score,
                "match_rank": rank,
                "_matched_at": datetime.utcnow(),
            })

        print(f"  {row['control_code']}: {len(data_array)} matches (best: {data_array[0][-1]:.4f})" if data_array else f"  {row['control_code']}: 0 matches")

    except Exception as e:
        print(f"  {row['control_code']}: ERROR - {e}")

print(f"\nTotal matches: {len(all_matches)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Matches to Delta

# COMMAND ----------

if all_matches:
    df_matches = spark.createDataFrame(all_matches)

    # Clear previous matches for this audit (re-running is idempotent)
    if audit_id:
        spark.sql(f"DELETE FROM {FQ}.control_evidence_matches WHERE audit_id = '{audit_id}'")

    df_matches.write.mode("append").saveAsTable(f"{FQ}.control_evidence_matches")
    print(f"Saved {len(all_matches)} matches.")
else:
    print("No matches found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Match Quality Summary

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            c.control_code,
            c.control_title,
            COUNT(m.match_id) AS total_matches,
            COUNT(CASE WHEN m.similarity_score >= 0.8 THEN 1 END) AS strong_matches,
            COUNT(CASE WHEN m.similarity_score >= 0.6 AND m.similarity_score < 0.8 THEN 1 END) AS moderate_matches,
            COUNT(CASE WHEN m.similarity_score < 0.6 THEN 1 END) AS weak_matches,
            ROUND(MAX(m.similarity_score), 4) AS best_score,
            ROUND(AVG(m.similarity_score), 4) AS avg_score,
            COUNT(DISTINCT m.document_id) AS distinct_documents
        FROM {FQ}.controls c
        LEFT JOIN {FQ}.control_evidence_matches m ON c.control_id = m.control_id
        WHERE c.audit_id = '{audit_id}' OR '{audit_id}' = ''
        GROUP BY c.control_code, c.control_title
        ORDER BY c.control_code
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Missing Evidence Detection

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            c.control_code,
            c.control_title,
            c.risk_level,
            COALESCE(MAX(m.similarity_score), 0) AS best_match_score,
            COUNT(m.match_id) AS match_count,
            CASE
                WHEN COUNT(m.match_id) = 0 THEN 'NO EVIDENCE'
                WHEN MAX(m.similarity_score) < 0.5 THEN 'WEAK EVIDENCE'
                WHEN COUNT(CASE WHEN m.similarity_score >= 0.7 THEN 1 END) < 2 THEN 'PARTIAL EVIDENCE'
                ELSE 'EVIDENCE AVAILABLE'
            END AS evidence_status
        FROM {FQ}.controls c
        LEFT JOIN {FQ}.control_evidence_matches m ON c.control_id = m.control_id
        WHERE c.audit_id = '{audit_id}' OR '{audit_id}' = ''
        GROUP BY c.control_code, c.control_title, c.risk_level
        HAVING evidence_status != 'EVIDENCE AVAILABLE'
        ORDER BY
            CASE evidence_status
                WHEN 'NO EVIDENCE' THEN 1
                WHEN 'WEAK EVIDENCE' THEN 2
                ELSE 3
            END,
            c.control_code
    """)
)

print("Control-evidence matching complete.")
