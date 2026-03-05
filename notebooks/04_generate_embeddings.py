# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Generate Embeddings
# MAGIC Generate vector embeddings for document chunks and control descriptions
# MAGIC using Databricks Foundation Model API (databricks-bge-large-en).

# COMMAND ----------

dbutils.widgets.text("audit_id", "", "Audit ID")
audit_id = dbutils.widgets.get("audit_id")

CATALOG = "main"
SCHEMA = "audit_schema"
FQ = f"{CATALOG}.{SCHEMA}"
EMBEDDING_MODEL = "databricks-bge-large-en"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Embed Document Chunks
# MAGIC Use `ai_query` with the Foundation Model embedding endpoint.

# COMMAND ----------

from pyspark.sql import functions as F

# Count chunks needing embeddings
unembedded_chunks = spark.table(f"{FQ}.document_chunks").filter(
    (F.col("embedding").isNull()) &
    (F.col("audit_id") == audit_id if audit_id else F.lit(True))
).count()

print(f"Chunks needing embeddings: {unembedded_chunks}")

# COMMAND ----------

if unembedded_chunks > 0:
    # Generate embeddings for chunks using ai_query
    audit_filter = f"AND audit_id = '{audit_id}'" if audit_id else ""

    spark.sql(f"""
        MERGE INTO {FQ}.document_chunks AS target
        USING (
            SELECT
                chunk_id,
                CAST(ai_query(
                    '{EMBEDDING_MODEL}',
                    chunk_text
                ) AS ARRAY<FLOAT>) AS embedding
            FROM {FQ}.document_chunks
            WHERE embedding IS NULL
            {audit_filter}
        ) AS source
        ON target.chunk_id = source.chunk_id
        WHEN MATCHED THEN UPDATE SET
            target.embedding = source.embedding
    """)

    print("Chunk embeddings generated.")
else:
    print("All chunks already have embeddings.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Embed Control Descriptions

# COMMAND ----------

unembedded_controls = spark.table(f"{FQ}.controls").filter(
    (F.col("embedding").isNull()) &
    (F.col("audit_id") == audit_id if audit_id else F.lit(True))
).count()

print(f"Controls needing embeddings: {unembedded_controls}")

# COMMAND ----------

if unembedded_controls > 0:
    audit_filter = f"AND audit_id = '{audit_id}'" if audit_id else ""

    spark.sql(f"""
        MERGE INTO {FQ}.controls AS target
        USING (
            SELECT
                control_id,
                CAST(ai_query(
                    '{EMBEDDING_MODEL}',
                    control_description
                ) AS ARRAY<FLOAT>) AS embedding
            FROM {FQ}.controls
            WHERE embedding IS NULL
            {audit_filter}
        ) AS source
        ON target.control_id = source.control_id
        WHEN MATCHED THEN UPDATE SET
            target.embedding = source.embedding
    """)

    print("Control embeddings generated.")
else:
    print("All controls already have embeddings.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Embeddings

# COMMAND ----------

# Check embedding coverage
for table_name in ["document_chunks", "controls"]:
    df = spark.table(f"{FQ}.{table_name}").filter(
        F.col("audit_id") == audit_id if audit_id else F.lit(True)
    )
    total = df.count()
    embedded = df.filter(F.col("embedding").isNotNull()).count()
    print(f"{table_name}: {embedded}/{total} embedded ({embedded/total*100:.1f}%)" if total > 0 else f"{table_name}: 0 rows")

print("\nEmbedding generation complete.")
