# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Ingest Controls
# MAGIC Ingest control files (CSV/Excel) from Unity Catalog Volumes using Auto Loader.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("audit_id", "", "Audit ID")
audit_id = dbutils.widgets.get("audit_id")

CATALOG = "main"
SCHEMA = "audit_schema"
FQ = f"{CATALOG}.{SCHEMA}"

CONTROLS_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/controls_raw"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/controls_ingest"

print(f"Audit ID: {audit_id}")
print(f"Controls Volume: {CONTROLS_VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader Ingestion - CSV Controls

# COMMAND ----------

from pyspark.sql import functions as F

# Read CSV control files with Auto Loader
df_controls = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaHints", """
        control_code STRING, framework STRING, control_title STRING,
        control_description STRING, control_category STRING, risk_level STRING,
        frequency STRING, control_owner STRING
    """)
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/schema")
    .option("header", "true")
    .load(CONTROLS_VOLUME if not audit_id else f"{CONTROLS_VOLUME}/{audit_id}")
)

# Transform and add metadata
df_transformed = (
    df_controls
    .withColumn("control_id", F.expr("uuid()"))
    .withColumn("audit_id",
        F.lit(audit_id) if audit_id
        else F.element_at(F.split(F.input_file_name(), "/"), -2)
    )
    .withColumn("embedding", F.lit(None).cast("array<float>"))
    .withColumn("uploaded_by", F.lit("autoloader"))
    .withColumn("uploaded_at", F.current_timestamp())
    .withColumn("source_file", F.input_file_name())
    .withColumn("_ingested_at", F.current_timestamp())
    .select(
        "control_id", "audit_id", "control_code", "framework",
        "control_title", "control_description", "control_category",
        "risk_level", "frequency", "control_owner", "embedding",
        "uploaded_by", "uploaded_at", "source_file", "_ingested_at"
    )
)

# Write to Delta table
(
    df_transformed.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .outputMode("append")
    .toTable(f"{FQ}.controls")
)

print("Controls ingestion complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Ingestion

# COMMAND ----------

count = spark.table(f"{FQ}.controls").filter(
    F.col("audit_id") == audit_id if audit_id else F.lit(True)
).count()
print(f"Total controls ingested: {count}")

display(
    spark.table(f"{FQ}.controls")
    .filter(F.col("audit_id") == audit_id if audit_id else F.lit(True))
    .select("control_code", "framework", "control_title", "risk_level")
    .orderBy("control_code")
)
