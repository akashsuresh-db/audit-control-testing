# Databricks notebook source
# MAGIC %md
# MAGIC # Test Notebook

# COMMAND ----------

print("Step 1: Basic Spark")
result = spark.sql("SELECT 1 + 1 AS answer").collect()
print(f"Answer: {result[0]['answer']}")

# COMMAND ----------

print("Step 2: Read table")
count = spark.sql("SELECT COUNT(*) AS cnt FROM main.audit_schema.controls").collect()[0]["cnt"]
print(f"Controls: {count}")

# COMMAND ----------

print("Step 3: ai_query embedding")
result = spark.sql("SELECT CAST(ai_query('databricks-bge-large-en', 'test') AS ARRAY<FLOAT>) AS emb").collect()
print(f"Embedding dim: {len(result[0]['emb'])}")

# COMMAND ----------

print("Step 4: MERGE embeddings (1 row)")
spark.sql("""
    MERGE INTO main.audit_schema.controls AS target
    USING (
        SELECT control_id, CAST(ai_query('databricks-bge-large-en', control_description) AS ARRAY<FLOAT>) AS embedding
        FROM main.audit_schema.controls
        WHERE embedding IS NULL AND audit_id = 'AUD-2026-001'
        LIMIT 1
    ) AS source
    ON target.control_id = source.control_id
    WHEN MATCHED THEN UPDATE SET target.embedding = source.embedding
""")
print("MERGE done!")

# COMMAND ----------

print("All tests passed!")
