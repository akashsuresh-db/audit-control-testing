# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Ingest Evidence Documents
# MAGIC Ingest evidence files (PDFs, images, logs) from Unity Catalog Volumes using Auto Loader.

# COMMAND ----------

dbutils.widgets.text("audit_id", "", "Audit ID")
audit_id = dbutils.widgets.get("audit_id")

CATALOG = "main"
SCHEMA = "audit_schema"
FQ = f"{CATALOG}.{SCHEMA}"

EVIDENCE_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/evidence_raw"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/evidence_ingest"

print(f"Audit ID: {audit_id}")
print(f"Evidence Volume: {EVIDENCE_VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader Ingestion - Binary Files

# COMMAND ----------

from pyspark.sql import functions as F

# Read binary files with Auto Loader
load_path = f"{EVIDENCE_VOLUME}/{audit_id}" if audit_id else EVIDENCE_VOLUME

df_evidence = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/schema")
    .option("cloudFiles.includeExistingFiles", "true")
    .load(load_path)
)

# Transform: extract metadata from file path
df_transformed = (
    df_evidence
    .withColumn("document_id", F.expr("uuid()"))
    .withColumn("audit_id",
        F.lit(audit_id) if audit_id
        else F.element_at(F.split(F.input_file_name(), "/"), -2)
    )
    .withColumn("original_filename",
        F.element_at(F.split(F.input_file_name(), "/"), -1)
    )
    .withColumn("file_type",
        F.lower(F.element_at(F.split(
            F.element_at(F.split(F.input_file_name(), "/"), -1), "\\."
        ), -1))
    )
    .withColumn("file_path", F.input_file_name())
    .withColumn("file_size_bytes", F.col("length"))
    .withColumn("page_count", F.lit(None).cast("int"))
    .withColumn("extracted_text", F.lit(None).cast("string"))
    .withColumn("parse_status", F.lit("PENDING"))
    .withColumn("parse_error", F.lit(None).cast("string"))
    .withColumn("ocr_applied", F.lit(False))
    .withColumn("uploaded_by", F.lit("autoloader"))
    .withColumn("uploaded_at", F.current_timestamp())
    .withColumn("_ingested_at", F.current_timestamp())
    .select(
        "document_id", "audit_id", "original_filename", "file_type",
        "file_path", "file_size_bytes", "page_count", "extracted_text",
        "parse_status", "parse_error", "ocr_applied", "uploaded_by",
        "uploaded_at", "_ingested_at"
    )
)

# Write to Delta table
(
    df_transformed.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .outputMode("append")
    .toTable(f"{FQ}.evidence_documents")
)

print("Evidence ingestion complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Ingestion

# COMMAND ----------

df_docs = spark.table(f"{FQ}.evidence_documents").filter(
    F.col("audit_id") == audit_id if audit_id else F.lit(True)
)
print(f"Total documents ingested: {df_docs.count()}")
print(f"Parse status breakdown:")
display(df_docs.groupBy("parse_status", "file_type").count().orderBy("parse_status"))
