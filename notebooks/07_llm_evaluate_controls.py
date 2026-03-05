# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - LLM Evaluate Controls
# MAGIC Use an LLM to evaluate whether each control is satisfied by the matched evidence.
# MAGIC Produces: verdict (PASS/FAIL/INSUFFICIENT_EVIDENCE), confidence, reasoning.

# COMMAND ----------

dbutils.widgets.text("audit_id", "", "Audit ID")
audit_id = dbutils.widgets.get("audit_id")

CATALOG = "main"
SCHEMA = "audit_schema"
FQ = f"{CATALOG}.{SCHEMA}"

MODEL_NAME = "databricks-meta-llama-3-3-70b-instruct"
PROMPT_VERSION = "v2.1"
SIMILARITY_THRESHOLD = 0.60  # Minimum similarity to include evidence
MAX_EVIDENCE_CHUNKS = 10      # Cap evidence per control

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gather Evidence Per Control

# COMMAND ----------

from pyspark.sql import functions as F

# Join controls with their matched evidence
df_eval_input = spark.sql(f"""
    SELECT
        c.control_id,
        c.audit_id,
        c.control_code,
        c.control_description,
        c.control_category,
        c.risk_level,
        c.framework,
        COLLECT_LIST(
            NAMED_STRUCT(
                'chunk_id', dc.chunk_id,
                'document_id', dc.document_id,
                'chunk_text', dc.chunk_text,
                'original_filename', ed.original_filename,
                'similarity_score', m.similarity_score
            )
        ) AS evidence_items,
        COUNT(m.match_id) AS match_count,
        MAX(m.similarity_score) AS best_score
    FROM {FQ}.controls c
    LEFT JOIN {FQ}.control_evidence_matches m
        ON c.control_id = m.control_id
        AND m.similarity_score >= {SIMILARITY_THRESHOLD}
        AND m.match_rank <= {MAX_EVIDENCE_CHUNKS}
    LEFT JOIN {FQ}.document_chunks dc
        ON m.chunk_id = dc.chunk_id
    LEFT JOIN {FQ}.evidence_documents ed
        ON m.document_id = ed.document_id
    WHERE (c.audit_id = '{audit_id}' OR '{audit_id}' = '')
    GROUP BY c.control_id, c.audit_id, c.control_code,
             c.control_description, c.control_category, c.risk_level, c.framework
    ORDER BY c.control_code
""")

print(f"Controls to evaluate: {df_eval_input.count()}")
display(df_eval_input.select("control_code", "match_count", "best_score"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Evaluation Prompts

# COMMAND ----------

SYSTEM_PROMPT = """You are an expert internal auditor AI assistant. Your role is to evaluate whether
audit control requirements are satisfied by the provided evidence documents.

CRITICAL RULES:
1. Base your assessment ONLY on the provided evidence. Do NOT assume or infer information not present.
2. If the evidence does not directly address the control, mark as INSUFFICIENT_EVIDENCE.
3. Quote specific details from the evidence to support your verdict.
4. Be precise about what the evidence does and does not demonstrate.
5. Always respond with valid JSON only. No markdown, no code fences, just raw JSON."""


def build_evaluation_prompt(control_code, control_description, evidence_items):
    """Build the evaluation prompt for a single control."""

    evidence_text = ""
    if evidence_items and evidence_items[0]["chunk_text"] is not None:
        for item in evidence_items:
            evidence_text += (
                f"\n--- Source: {item['original_filename']} "
                f"(relevance: {item['similarity_score']:.3f}) ---\n"
                f"{item['chunk_text']}\n"
            )
    else:
        evidence_text = "\n[NO EVIDENCE DOCUMENTS MATCHED THIS CONTROL]\n"

    return f"""CONTROL ID: {control_code}

CONTROL DESCRIPTION:
{control_description}

MATCHED EVIDENCE DOCUMENTS:
{evidence_text}

EVALUATION INSTRUCTIONS:
1. Read the control description carefully to understand ALL requirements.
2. Review each piece of evidence for relevance and completeness.
3. Determine if the evidence demonstrates the control was executed.
4. Consider: completeness, relevance, recency, and specificity of evidence.

VERDICT CRITERIA:
- PASS: Evidence clearly demonstrates the control was executed as described.
  Key requirements are directly addressed with specific, concrete evidence.
- FAIL: Evidence exists but shows the control was NOT followed, or evidence
  directly contradicts the control requirements.
- INSUFFICIENT_EVIDENCE: Not enough relevant evidence to make a determination,
  evidence is too vague, or key aspects of the control are not addressed.

Respond ONLY with this JSON structure:
{{"verdict": "PASS|FAIL|INSUFFICIENT_EVIDENCE", "confidence": <0.0-1.0>, "reasoning": "<2-4 sentences>", "evidence_summary": "<1-2 sentences summarizing evidence>", "key_evidence_points": ["<specific quotes or facts from evidence>"], "gaps": ["<missing evidence or concerns>"]}}"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run LLM Evaluation

# COMMAND ----------

import requests
import json
import uuid
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Get workspace URL and token for API calls
DATABRICKS_HOST = (
    dbutils.notebook.entry_point.getDbutils().notebook()
    .getContext().apiUrl().get()
)
TOKEN = (
    dbutils.notebook.entry_point.getDbutils().notebook()
    .getContext().apiToken().get()
)


def call_llm(prompt, system_prompt=SYSTEM_PROMPT):
    """Call the Foundation Model API."""
    response = requests.post(
        f"{DATABRICKS_HOST}/serving-endpoints/{MODEL_NAME}/invocations",
        headers={"Authorization": f"Bearer {TOKEN}"},
        json={
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 1024,
            "temperature": 0.1,
        },
        timeout=60,
    )
    response.raise_for_status()
    return response.json()["choices"][0]["message"]["content"]


def parse_llm_response(text):
    """Parse JSON from LLM response, handling common formatting issues."""
    import re
    # Strip markdown code fences if present
    text = re.sub(r'```json\s*', '', text)
    text = re.sub(r'```\s*', '', text)
    text = text.strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Try to extract JSON object
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            return json.loads(match.group())
        return {
            "verdict": "INSUFFICIENT_EVIDENCE",
            "confidence": 0.0,
            "reasoning": f"LLM response could not be parsed: {text[:200]}",
            "evidence_summary": "",
            "key_evidence_points": [],
            "gaps": ["Response parsing error"],
        }


def evaluate_control(row):
    """Evaluate a single control against its matched evidence."""
    control_id = row["control_id"]
    audit_id = row["audit_id"]
    control_code = row["control_code"]
    evidence_items = row["evidence_items"]
    match_count = row["match_count"]
    best_score = row["best_score"]

    # Short-circuit: no evidence at all
    if match_count == 0 or best_score is None or best_score < SIMILARITY_THRESHOLD:
        return {
            "evaluation_id": str(uuid.uuid4()),
            "control_id": control_id,
            "audit_id": audit_id,
            "ai_verdict": "INSUFFICIENT_EVIDENCE",
            "ai_confidence": 0.1,
            "ai_reasoning": "No evidence documents matched this control above the similarity threshold.",
            "evidence_summary": "No relevant evidence found.",
            "matched_document_ids": [],
            "matched_chunk_ids": [],
            "model_used": MODEL_NAME,
            "prompt_version": PROMPT_VERSION,
            "evaluated_at": datetime.utcnow(),
            "_created_at": datetime.utcnow(),
        }

    # Build prompt and call LLM
    prompt = build_evaluation_prompt(
        control_code, row["control_description"], evidence_items
    )

    try:
        llm_output = call_llm(prompt)
        parsed = parse_llm_response(llm_output)
    except Exception as e:
        parsed = {
            "verdict": "INSUFFICIENT_EVIDENCE",
            "confidence": 0.0,
            "reasoning": f"LLM evaluation failed: {str(e)[:200]}",
            "evidence_summary": "",
            "gaps": ["LLM call error"],
        }

    # Extract unique document and chunk IDs
    doc_ids = list(set(
        item["document_id"] for item in evidence_items
        if item.get("document_id")
    ))
    chunk_ids = [
        item["chunk_id"] for item in evidence_items
        if item.get("chunk_id")
    ]

    return {
        "evaluation_id": str(uuid.uuid4()),
        "control_id": control_id,
        "audit_id": audit_id,
        "ai_verdict": parsed.get("verdict", "INSUFFICIENT_EVIDENCE"),
        "ai_confidence": float(parsed.get("confidence", 0.0)),
        "ai_reasoning": parsed.get("reasoning", ""),
        "evidence_summary": parsed.get("evidence_summary", ""),
        "matched_document_ids": doc_ids,
        "matched_chunk_ids": chunk_ids,
        "model_used": MODEL_NAME,
        "prompt_version": PROMPT_VERSION,
        "evaluated_at": datetime.utcnow(),
        "_created_at": datetime.utcnow(),
    }


# Collect evaluation inputs
eval_inputs = df_eval_input.collect()
print(f"Evaluating {len(eval_inputs)} controls...")

# Run evaluations in parallel (8 concurrent LLM calls)
results = []
with ThreadPoolExecutor(max_workers=8) as executor:
    futures = {
        executor.submit(evaluate_control, row): row["control_code"]
        for row in eval_inputs
    }
    for future in as_completed(futures):
        control_code = futures[future]
        try:
            result = future.result()
            results.append(result)
            print(f"  {control_code}: {result['ai_verdict']} (confidence: {result['ai_confidence']:.2f})")
        except Exception as e:
            print(f"  {control_code}: ERROR - {e}")

print(f"\nCompleted {len(results)} evaluations.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Evaluation Results

# COMMAND ----------

if results:
    # Add NULL auditor fields
    for r in results:
        r["auditor_verdict"] = None
        r["auditor_notes"] = None
        r["auditor_id"] = None
        r["reviewed_at"] = None

    df_results = spark.createDataFrame(results)

    # Clear previous evaluations for this audit (re-running is idempotent)
    if audit_id:
        spark.sql(f"DELETE FROM {FQ}.evaluation_results WHERE audit_id = '{audit_id}'")

    df_results.write.mode("append").saveAsTable(f"{FQ}.evaluation_results")
    print(f"Saved {len(results)} evaluation results.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluation Summary

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        er.ai_verdict,
        COUNT(*) AS count,
        ROUND(AVG(er.ai_confidence), 3) AS avg_confidence,
        ROUND(MIN(er.ai_confidence), 3) AS min_confidence,
        ROUND(MAX(er.ai_confidence), 3) AS max_confidence
    FROM {FQ}.evaluation_results er
    WHERE er.audit_id = '{audit_id}' OR '{audit_id}' = ''
    GROUP BY er.ai_verdict
    ORDER BY count DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detailed Results

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        c.control_code,
        c.control_title,
        c.risk_level,
        er.ai_verdict,
        ROUND(er.ai_confidence, 3) AS confidence,
        er.ai_reasoning,
        er.evidence_summary,
        SIZE(er.matched_document_ids) AS evidence_docs_count
    FROM {FQ}.evaluation_results er
    JOIN {FQ}.controls c ON er.control_id = c.control_id
    WHERE er.audit_id = '{audit_id}' OR '{audit_id}' = ''
    ORDER BY
        CASE er.ai_verdict
            WHEN 'FAIL' THEN 1
            WHEN 'INSUFFICIENT_EVIDENCE' THEN 2
            ELSE 3
        END,
        c.control_code
"""))

print("LLM evaluation complete.")
