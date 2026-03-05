# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Parse Documents
# MAGIC Extract text from evidence documents using Databricks AI Parse.
# MAGIC Handles PDFs, images (with OCR), and text-based files.

# COMMAND ----------

dbutils.widgets.text("audit_id", "", "Audit ID")
audit_id = dbutils.widgets.get("audit_id")

CATALOG = "main"
SCHEMA = "audit_schema"
FQ = f"{CATALOG}.{SCHEMA}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse Documents with ai_parse

# COMMAND ----------

from pyspark.sql import functions as F

# Get documents pending parsing
filter_condition = (
    (F.col("parse_status") == "PENDING") &
    (F.col("audit_id") == audit_id if audit_id else F.lit(True))
)

df_pending = spark.table(f"{FQ}.evidence_documents").filter(filter_condition)
pending_count = df_pending.count()
print(f"Documents to parse: {pending_count}")

if pending_count == 0:
    print("No documents to parse. Exiting.")
    dbutils.notebook.exit("No documents to parse")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parse PDFs and images using ai_parse
# MAGIC The `ai_parse` function extracts text from binary content including:
# MAGIC - Native PDFs (text layer extraction)
# MAGIC - Scanned PDFs (OCR)
# MAGIC - Images (PNG, JPG, TIFF) via OCR
# MAGIC - Office documents

# COMMAND ----------

# For PDFs and images: use ai_parse on the binary content
# Create temp view for SQL-based parsing
df_pending.createOrReplaceTempView("pending_docs")

# Parse using ai_parse - this handles PDFs and images with built-in OCR
parsed_df = spark.sql(f"""
    SELECT
        pd.document_id,
        pd.audit_id,
        pd.file_type,
        CASE
            WHEN pd.file_type IN ('pdf', 'png', 'jpg', 'jpeg', 'tiff', 'bmp')
            THEN ai_parse(read_files(pd.file_path, format => 'binaryFile').content)
            WHEN pd.file_type IN ('txt', 'log', 'csv')
            THEN named_struct(
                'text', cast(read_files(pd.file_path, format => 'text').value AS STRING),
                'num_pages', 1
            )
            ELSE named_struct('text', NULL, 'num_pages', NULL)
        END AS parsed_result
    FROM pending_docs pd
""")

# Extract text and metadata from parsed results
results_df = (
    parsed_df
    .withColumn("extracted_text", F.col("parsed_result.text"))
    .withColumn("page_count", F.col("parsed_result.num_pages"))
    .withColumn("parse_status",
        F.when(F.col("extracted_text").isNotNull(), "PARSED")
        .otherwise("FAILED")
    )
    .withColumn("ocr_applied",
        F.col("file_type").isin("png", "jpg", "jpeg", "tiff", "bmp")
    )
    .select("document_id", "extracted_text", "page_count", "parse_status", "ocr_applied")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update evidence_documents with parsed text

# COMMAND ----------

# Merge parsed results back into evidence_documents
results_df.createOrReplaceTempView("parsed_results")

spark.sql(f"""
    MERGE INTO {FQ}.evidence_documents AS target
    USING parsed_results AS source
    ON target.document_id = source.document_id
    WHEN MATCHED THEN UPDATE SET
        target.extracted_text = source.extracted_text,
        target.page_count = source.page_count,
        target.parse_status = source.parse_status,
        target.ocr_applied = source.ocr_applied
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fallback: Handle parse failures with enhanced preprocessing

# COMMAND ----------

# Check for any failures
failed_count = spark.table(f"{FQ}.evidence_documents").filter(
    (F.col("parse_status") == "FAILED") &
    (F.col("audit_id") == audit_id if audit_id else F.lit(True))
).count()

if failed_count > 0:
    print(f"WARNING: {failed_count} documents failed parsing.")
    print("These may need manual text extraction or re-upload in a supported format.")

    # Update failed documents with error message
    spark.sql(f"""
        UPDATE {FQ}.evidence_documents
        SET parse_error = 'ai_parse returned null - document may be corrupted or in unsupported format'
        WHERE parse_status = 'FAILED'
        AND parse_error IS NULL
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

display(
    spark.table(f"{FQ}.evidence_documents")
    .filter(F.col("audit_id") == audit_id if audit_id else F.lit(True))
    .groupBy("parse_status", "file_type", "ocr_applied")
    .agg(
        F.count("*").alias("doc_count"),
        F.avg(F.length("extracted_text")).cast("int").alias("avg_text_length"),
        F.sum("page_count").alias("total_pages")
    )
    .orderBy("parse_status")
)

print("Document parsing complete.")
