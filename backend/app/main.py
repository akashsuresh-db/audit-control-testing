"""
Audit Control Testing Application - FastAPI Backend
Deployed as a Databricks App.
"""
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional
import os
import uuid
import csv
import io
import base64
from datetime import datetime

from .db import execute_sql, fetch_sql

app = FastAPI(
    title="Audit Control Testing API",
    description="AI-powered internal control testing for auditors",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CATALOG = "main"
SCHEMA = "audit_schema"
FQ = f"{CATALOG}.{SCHEMA}"

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

PARSEABLE_FORMATS = {"pdf", "png", "jpg", "jpeg", "doc", "docx", "ppt", "pptx"}
TEXT_FORMATS = {"txt", "csv", "rtf"}


# ---- Pydantic Models ----

class AuditCreate(BaseModel):
    audit_name: str
    framework: str
    client_name: str
    description: Optional[str] = ""


class ReviewRequest(BaseModel):
    verdict: str
    notes: Optional[str] = ""
    auditor_id: str


# ---- Serve Frontend ----

@app.get("/")
async def serve_frontend():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


# ---- Health Check ----

@app.get("/api/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


# ---- Audit Engagements ----

@app.get("/api/audits")
async def list_audits():
    return fetch_sql(f"SELECT * FROM {FQ}.audit_engagements ORDER BY created_at DESC")


@app.post("/api/audits")
async def create_audit(audit: AuditCreate):
    audit_id = f"AUD-{datetime.now().strftime('%Y')}-{uuid.uuid4().hex[:6].upper()}"
    execute_sql(
        f"INSERT INTO {FQ}.audit_engagements VALUES "
        f"(:audit_id, :name, :framework, :client, :desc, 'CREATED', 'api_user', current_timestamp(), current_timestamp())",
        {"audit_id": audit_id, "name": audit.audit_name, "framework": audit.framework,
         "client": audit.client_name, "desc": audit.description or ""},
    )
    return {"audit_id": audit_id, "status": "CREATED"}


@app.get("/api/audits/{audit_id}")
async def get_audit(audit_id: str):
    rows = fetch_sql(
        f"SELECT * FROM {FQ}.audit_engagements WHERE audit_id = :aid",
        {"aid": audit_id},
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Audit not found")
    return rows[0]


# ---- Controls ----

@app.get("/api/audits/{audit_id}/controls")
async def list_controls(audit_id: str):
    return fetch_sql(
        f"SELECT control_id, control_code, framework, control_title, "
        f"control_description, control_category, risk_level, frequency, control_owner "
        f"FROM {FQ}.controls WHERE audit_id = :aid ORDER BY control_code",
        {"aid": audit_id},
    )


@app.post("/api/audits/{audit_id}/controls")
async def upload_controls(audit_id: str, file: UploadFile = File(...)):
    """Upload a controls CSV file and insert into the controls table."""
    content = await file.read()
    text = content.decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))

    inserted = 0
    for row in reader:
        control_id = str(uuid.uuid4())
        execute_sql(
            f"INSERT INTO {FQ}.controls "
            f"(control_id, audit_id, control_code, framework, control_title, "
            f"control_description, control_category, risk_level, frequency, "
            f"control_owner, uploaded_by, uploaded_at, source_file, _ingested_at) "
            f"VALUES (:cid, :aid, :code, :fw, :title, :desc, :cat, :risk, :freq, "
            f":owner, 'app_upload', current_timestamp(), :src, current_timestamp())",
            {
                "cid": control_id, "aid": audit_id,
                "code": row.get("control_code", ""),
                "fw": row.get("framework", ""),
                "title": row.get("control_title", ""),
                "desc": row.get("control_description", ""),
                "cat": row.get("control_category", ""),
                "risk": row.get("risk_level", "MEDIUM"),
                "freq": row.get("frequency", ""),
                "owner": row.get("control_owner", ""),
                "src": file.filename,
            },
        )
        inserted += 1

    return {
        "status": "uploaded",
        "audit_id": audit_id,
        "filename": file.filename,
        "controls_inserted": inserted,
    }


# ---- Evidence Documents ----

@app.get("/api/audits/{audit_id}/evidence")
async def list_evidence(audit_id: str):
    return fetch_sql(
        f"SELECT document_id, original_filename, file_type, file_size_bytes, "
        f"page_count, parse_status, ocr_applied, uploaded_at "
        f"FROM {FQ}.evidence_documents WHERE audit_id = :aid ORDER BY uploaded_at DESC",
        {"aid": audit_id},
    )


@app.post("/api/audits/{audit_id}/evidence")
async def upload_evidence(audit_id: str, files: list[UploadFile] = File(...)):
    """Upload evidence files and register them in the evidence_documents table."""
    uploaded = []
    for file in files:
        content = await file.read()
        doc_id = str(uuid.uuid4())
        ext = file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else "bin"
        size = len(content)

        # Store file to UC Volume
        volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/evidence_raw/{audit_id}"
        file_path = f"{volume_path}/{doc_id}.{ext}"

        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            w.files.upload(file_path, io.BytesIO(content), overwrite=True)
        except Exception:
            pass

        extracted = ""
        parse_status = "PENDING"

        if ext in TEXT_FORMATS:
            try:
                extracted = content.decode("utf-8")
                parse_status = "COMPLETED"
            except Exception:
                pass
        elif ext in PARSEABLE_FORMATS:
            # Store binary content as base64 for ai_parse_document in pipeline
            extracted = ""
            parse_status = "PENDING_AI_PARSE"

        execute_sql(
            f"INSERT INTO {FQ}.evidence_documents "
            f"(document_id, audit_id, original_filename, file_type, file_path, "
            f"file_size_bytes, extracted_text, parse_status, uploaded_by, uploaded_at, _ingested_at) "
            f"VALUES (:did, :aid, :fname, :ext, :fpath, :size, :text, :status, "
            f"'app_upload', current_timestamp(), current_timestamp())",
            {
                "did": doc_id, "aid": audit_id, "fname": file.filename,
                "ext": ext, "fpath": file_path, "size": size,
                "text": extracted, "status": parse_status,
            },
        )

        uploaded.append({
            "document_id": doc_id,
            "filename": file.filename,
            "size": size,
            "parse_status": parse_status,
        })

    return {"uploaded": uploaded, "count": len(uploaded)}


@app.get("/api/evidence/{document_id}")
async def get_evidence_detail(document_id: str):
    rows = fetch_sql(
        f"SELECT * FROM {FQ}.evidence_documents WHERE document_id = :did",
        {"did": document_id},
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Document not found")
    return rows[0]


# ---- Pipeline Trigger ----

@app.post("/api/audits/{audit_id}/evaluate")
async def trigger_evaluation(audit_id: str):
    """Trigger the evaluation workflow for an audit."""
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    try:
        run = w.jobs.run_now(
            job_id=675224616522707,
            notebook_params={"audit_id": audit_id},
        )
        return {
            "audit_id": audit_id,
            "status": "TRIGGERED",
            "run_id": run.run_id,
            "message": "Evaluation pipeline triggered.",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to trigger pipeline: {str(e)}")


@app.get("/api/audits/{audit_id}/status")
async def get_pipeline_status(audit_id: str):
    """Check evaluation pipeline status."""
    controls = fetch_sql(
        f"SELECT COUNT(*) as cnt FROM {FQ}.controls WHERE audit_id = :aid",
        {"aid": audit_id},
    )
    evaluated = fetch_sql(
        f"SELECT COUNT(*) as cnt FROM {FQ}.evaluation_results WHERE audit_id = :aid",
        {"aid": audit_id},
    )
    reviewed = fetch_sql(
        f"SELECT COUNT(*) as cnt FROM {FQ}.evaluation_results WHERE audit_id = :aid AND auditor_verdict IS NOT NULL",
        {"aid": audit_id},
    )

    total = controls[0]["cnt"] if controls else 0
    eval_count = evaluated[0]["cnt"] if evaluated else 0
    review_count = reviewed[0]["cnt"] if reviewed else 0

    return {
        "audit_id": audit_id,
        "total_controls": total,
        "evaluated": eval_count,
        "reviewed": review_count,
        "status": "COMPLETED" if eval_count >= total and total > 0 else "IN_PROGRESS" if eval_count > 0 else "PENDING",
    }


# ---- Evaluation Results ----

@app.get("/api/audits/{audit_id}/results")
async def get_results(audit_id: str):
    return fetch_sql(
        f"SELECT er.evaluation_id, er.control_id, er.ai_verdict, er.ai_confidence, "
        f"er.ai_reasoning, er.evidence_summary, "
        f"CAST(er.matched_document_ids AS STRING) AS matched_document_ids, "
        f"er.auditor_verdict, er.auditor_notes, er.auditor_id, er.reviewed_at, "
        f"er.model_used, er.prompt_version, er.evaluated_at, "
        f"c.control_code, c.control_title, c.control_description, "
        f"c.control_category, c.risk_level, c.framework "
        f"FROM {FQ}.evaluation_results er "
        f"JOIN {FQ}.controls c ON er.control_id = c.control_id "
        f"WHERE er.audit_id = :aid ORDER BY c.control_code",
        {"aid": audit_id},
    )


@app.get("/api/audits/{audit_id}/results/summary")
async def get_results_summary(audit_id: str):
    return fetch_sql(
        f"SELECT ai_verdict, COUNT(*) AS count, "
        f"ROUND(AVG(ai_confidence), 3) AS avg_confidence, "
        f"COUNT(CASE WHEN auditor_verdict IS NOT NULL THEN 1 END) AS reviewed_count, "
        f"COUNT(CASE WHEN ai_verdict != auditor_verdict AND auditor_verdict IS NOT NULL THEN 1 END) AS override_count "
        f"FROM {FQ}.evaluation_results WHERE audit_id = :aid GROUP BY ai_verdict",
        {"aid": audit_id},
    )


@app.get("/api/results/{evaluation_id}")
async def get_evaluation_detail(evaluation_id: str):
    rows = fetch_sql(
        f"SELECT er.evaluation_id, er.control_id, er.audit_id, er.ai_verdict, er.ai_confidence, "
        f"er.ai_reasoning, er.evidence_summary, "
        f"CAST(er.matched_document_ids AS STRING) AS matched_document_ids, "
        f"CAST(er.matched_chunk_ids AS STRING) AS matched_chunk_ids, "
        f"er.auditor_verdict, er.auditor_notes, er.auditor_id, er.reviewed_at, "
        f"er.model_used, er.prompt_version, er.evaluated_at, "
        f"c.control_code, c.control_title, c.control_description, "
        f"c.control_category, c.risk_level "
        f"FROM {FQ}.evaluation_results er "
        f"JOIN {FQ}.controls c ON er.control_id = c.control_id "
        f"WHERE er.evaluation_id = :eid",
        {"eid": evaluation_id},
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Evaluation not found")

    result = rows[0]
    chunks = fetch_sql(
        f"SELECT dc.chunk_id, dc.chunk_text, dc.chunk_index, "
        f"ed.original_filename, ed.document_id, "
        f"m.similarity_score, m.match_rank "
        f"FROM {FQ}.control_evidence_matches m "
        f"JOIN {FQ}.document_chunks dc ON m.chunk_id = dc.chunk_id "
        f"JOIN {FQ}.evidence_documents ed ON m.document_id = ed.document_id "
        f"WHERE m.control_id = :cid AND m.audit_id = :aid ORDER BY m.match_rank",
        {"cid": result["control_id"], "aid": result["audit_id"]},
    )
    result["matched_evidence"] = chunks
    return result


@app.get("/api/audits/{audit_id}/evidence-matches")
async def get_all_evidence_matches(audit_id: str):
    """Get all evidence matches for an audit, grouped by control."""
    return fetch_sql(
        f"SELECT m.control_id, c.control_code, "
        f"dc.chunk_id, dc.chunk_text, dc.chunk_index, "
        f"ed.original_filename, ed.document_id, "
        f"ROUND(m.similarity_score, 4) AS similarity_score, m.match_rank "
        f"FROM {FQ}.control_evidence_matches m "
        f"JOIN {FQ}.controls c ON m.control_id = c.control_id "
        f"JOIN {FQ}.document_chunks dc ON m.chunk_id = dc.chunk_id "
        f"JOIN {FQ}.evidence_documents ed ON m.document_id = ed.document_id "
        f"WHERE m.audit_id = :aid AND m.similarity_score >= 0.4 AND m.match_rank <= 8 "
        f"ORDER BY c.control_code, m.match_rank",
        {"aid": audit_id},
    )


@app.put("/api/results/{evaluation_id}/review")
async def submit_review(evaluation_id: str, review: ReviewRequest):
    """Auditor submits their final verdict."""
    execute_sql(
        f"UPDATE {FQ}.evaluation_results "
        f"SET auditor_verdict = :verdict, auditor_notes = :notes, "
        f"auditor_id = :auditor, reviewed_at = current_timestamp() "
        f"WHERE evaluation_id = :eid",
        {"verdict": review.verdict, "notes": review.notes or "",
         "auditor": review.auditor_id, "eid": evaluation_id},
    )

    rows = fetch_sql(
        f"SELECT audit_id FROM {FQ}.evaluation_results WHERE evaluation_id = :eid",
        {"eid": evaluation_id},
    )
    audit_id = rows[0]["audit_id"] if rows else "UNKNOWN"

    log_id = str(uuid.uuid4())
    execute_sql(
        f"INSERT INTO {FQ}.audit_log VALUES "
        f"(:lid, :aid, :auditor, 'REVIEW', 'EVALUATION', :eid, :detail, current_timestamp())",
        {"lid": log_id, "aid": audit_id, "auditor": review.auditor_id,
         "eid": evaluation_id, "detail": f'{{"verdict": "{review.verdict}"}}'},
    )

    return {"status": "reviewed", "evaluation_id": evaluation_id}


# ---- Audit Log ----

@app.get("/api/audits/{audit_id}/audit-log")
async def get_audit_log(audit_id: str, limit: int = Query(default=100, le=1000)):
    return fetch_sql(
        f"SELECT * FROM {FQ}.audit_log WHERE audit_id = :aid ORDER BY timestamp DESC LIMIT {limit}",
        {"aid": audit_id},
    )


# ---- Export ----

@app.get("/api/audits/{audit_id}/export")
async def export_results(audit_id: str):
    """Export evaluation results as structured data for report generation."""
    results = fetch_sql(
        f"SELECT c.control_code, c.control_title, c.control_description, "
        f"c.control_category, c.risk_level, c.framework, "
        f"er.ai_verdict, er.ai_confidence, er.ai_reasoning, er.evidence_summary, "
        f"er.auditor_verdict, er.auditor_notes, er.auditor_id, er.reviewed_at, "
        f"COALESCE(er.auditor_verdict, er.ai_verdict) AS final_verdict "
        f"FROM {FQ}.evaluation_results er "
        f"JOIN {FQ}.controls c ON er.control_id = c.control_id "
        f"WHERE er.audit_id = :aid ORDER BY c.control_code",
        {"aid": audit_id},
    )

    summary = fetch_sql(
        f"SELECT COALESCE(er.auditor_verdict, er.ai_verdict) AS final_verdict, "
        f"COUNT(*) AS count "
        f"FROM {FQ}.evaluation_results er "
        f"WHERE er.audit_id = :aid "
        f"GROUP BY COALESCE(er.auditor_verdict, er.ai_verdict)",
        {"aid": audit_id},
    )

    return {
        "audit_id": audit_id,
        "generated_at": datetime.utcnow().isoformat(),
        "summary": summary,
        "results": results,
    }


# Mount static files last so API routes take priority
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
