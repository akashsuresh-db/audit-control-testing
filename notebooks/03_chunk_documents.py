# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Chunk Documents
# MAGIC Split extracted text into overlapping chunks for embedding and retrieval.

# COMMAND ----------

dbutils.widgets.text("audit_id", "", "Audit ID")
audit_id = dbutils.widgets.get("audit_id")

CATALOG = "main"
SCHEMA = "audit_schema"
FQ = f"{CATALOG}.{SCHEMA}"

CHUNK_SIZE = 512      # tokens per chunk
CHUNK_OVERLAP = 64    # overlap tokens between chunks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Chunking Logic

# COMMAND ----------

import tiktoken
import uuid
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType

encoding = tiktoken.get_encoding("cl100k_base")


def chunk_text(text, chunk_size=CHUNK_SIZE, overlap=CHUNK_OVERLAP):
    """Split text into overlapping token-based chunks."""
    if not text or len(text.strip()) == 0:
        return []

    tokens = encoding.encode(text)
    chunks = []
    start = 0
    idx = 0

    while start < len(tokens):
        end = min(start + chunk_size, len(tokens))
        chunk_tokens = tokens[start:end]
        chunk_str = encoding.decode(chunk_tokens)

        chunks.append({
            "chunk_id": str(uuid.uuid4()),
            "chunk_index": idx,
            "chunk_text": chunk_str,
            "token_count": len(chunk_tokens)
        })

        if end >= len(tokens):
            break

        start += chunk_size - overlap
        idx += 1

    return chunks


# Register as Spark UDF
chunk_schema = ArrayType(StructType([
    StructField("chunk_id", StringType()),
    StructField("chunk_index", IntegerType()),
    StructField("chunk_text", StringType()),
    StructField("token_count", IntegerType()),
]))

chunk_udf = F.udf(chunk_text, chunk_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identify Documents to Chunk

# COMMAND ----------

# Get parsed documents
df_parsed = (
    spark.table(f"{FQ}.evidence_documents")
    .filter(F.col("parse_status") == "PARSED")
    .filter(F.col("extracted_text").isNotNull())
    .filter(F.col("audit_id") == audit_id if audit_id else F.lit(True))
)

# Exclude already-chunked documents
df_existing = spark.table(f"{FQ}.document_chunks").select("document_id").distinct()
df_to_chunk = df_parsed.join(df_existing, "document_id", "left_anti")

to_chunk_count = df_to_chunk.count()
print(f"Documents to chunk: {to_chunk_count}")

if to_chunk_count == 0:
    print("No new documents to chunk. Exiting.")
    dbutils.notebook.exit("No documents to chunk")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Chunks

# COMMAND ----------

df_chunks = (
    df_to_chunk
    .withColumn("chunks", chunk_udf(F.col("extracted_text")))
    .select(
        "document_id", "audit_id",
        F.explode("chunks").alias("chunk")
    )
    .select(
        F.col("chunk.chunk_id").alias("chunk_id"),
        "document_id",
        "audit_id",
        F.col("chunk.chunk_index").alias("chunk_index"),
        F.col("chunk.chunk_text").alias("chunk_text"),
        F.col("chunk.token_count").alias("token_count"),
        # Approximate page numbers based on chunk position
        F.array(
            (F.col("chunk.chunk_index") + 1).cast("int")
        ).alias("page_numbers"),
        F.lit(None).cast("array<float>").alias("embedding"),
        F.current_timestamp().alias("_created_at"),
    )
)

# Write chunks to Delta table
df_chunks.write.mode("append").saveAsTable(f"{FQ}.document_chunks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

total_chunks = spark.table(f"{FQ}.document_chunks").filter(
    F.col("audit_id") == audit_id if audit_id else F.lit(True)
).count()

print(f"Total chunks in table: {total_chunks}")

display(
    spark.table(f"{FQ}.document_chunks")
    .filter(F.col("audit_id") == audit_id if audit_id else F.lit(True))
    .groupBy("document_id")
    .agg(
        F.count("*").alias("chunk_count"),
        F.sum("token_count").alias("total_tokens"),
        F.avg("token_count").cast("int").alias("avg_tokens_per_chunk")
    )
    .orderBy("document_id")
)

print("Chunking complete.")
