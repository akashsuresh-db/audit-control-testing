"""
Audit Control Testing Application - FastAPI Backend
Enterprise AI-powered audit control testing platform.
Deployed as a Databricks App with Lakebase (pgvector) backend.
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
from datetime import datetime

# Both backends are optional — import whichever is available
try:
    from .lakebase import pg_execute, pg_fetch, pg_fetch_one, similarity_search
except ImportError:
    def pg_execute(*a, **kw): raise RuntimeError("pg8000 not installed")
    def pg_fetch(*a, **kw): raise RuntimeError("pg8000 not installed")
    def pg_fetch_one(*a, **kw): raise RuntimeError("pg8000 not installed")
    def similarity_search(*a, **kw): raise RuntimeError("pg8000 not installed")

try:
    from .db import execute_sql, fetch_sql
except ImportError:
    def execute_sql(*a, **kw): raise RuntimeError("databricks-sql-connector not installed")
    def fetch_sql(*a, **kw): raise RuntimeError("databricks-sql-connector not installed")

app = FastAPI(
    title="AuditLens API",
    description="Enterprise AI-powered audit control testing platform with pgvector similarity search",
    version="3.0.0",
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
DIST_DIR = os.path.join(STATIC_DIR, "dist")

PARSEABLE_FORMATS = {"pdf", "png", "jpg", "jpeg", "doc", "docx", "ppt", "pptx"}
TEXT_FORMATS = {"txt", "csv", "rtf"}

# Determine backend mode: "lakebase" or "databricks"
BACKEND_MODE = os.environ.get("BACKEND_MODE", "lakebase")


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
    index = os.path.join(DIST_DIR, "index.html")
    if os.path.exists(index):
        return FileResponse(index)
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


# ---- Health Check ----

@app.get("/api/health")
async def health():
    return {"status": "healthy", "backend": BACKEND_MODE, "timestamp": datetime.utcnow().isoformat()}


# ---- Audit Engagements ----

@app.get("/api/audits")
async def list_audits():
    if BACKEND_MODE == "lakebase":
        return pg_fetch("SELECT * FROM audit_engagements ORDER BY created_at DESC")
    return fetch_sql(f"SELECT * FROM {FQ}.audit_engagements ORDER BY created_at DESC")


@app.post("/api/audits")
async def create_audit(audit: AuditCreate):
    audit_id = f"AUD-{datetime.now().strftime('%Y')}-{uuid.uuid4().hex[:6].upper()}"
    if BACKEND_MODE == "lakebase":
        pg_execute(
            """INSERT INTO audit_engagements (audit_id, audit_name, framework, client_name, description, status, created_by)
               VALUES (%s, %s, %s, %s, %s, 'CREATED', 'api_user')""",
            (audit_id, audit.audit_name, audit.framework, audit.client_name, audit.description or ""),
        )
    else:
        execute_sql(
            f"INSERT INTO {FQ}.audit_engagements VALUES "
            f"(:audit_id, :name, :framework, :client, :desc, 'CREATED', 'api_user', current_timestamp(), current_timestamp())",
            {"audit_id": audit_id, "name": audit.audit_name, "framework": audit.framework,
             "client": audit.client_name, "desc": audit.description or ""},
        )
    return {"audit_id": audit_id, "status": "CREATED"}


@app.get("/api/audits/{audit_id}")
async def get_audit(audit_id: str):
    if BACKEND_MODE == "lakebase":
        row = pg_fetch_one("SELECT * FROM audit_engagements WHERE audit_id = %s", (audit_id,))
    else:
        rows = fetch_sql(f"SELECT * FROM {FQ}.audit_engagements WHERE audit_id = :aid", {"aid": audit_id})
        row = rows[0] if rows else None
    if not row:
        raise HTTPException(status_code=404, detail="Audit not found")
    return row


# ---- Controls ----

@app.get("/api/audits/{audit_id}/controls")
async def list_controls(audit_id: str):
    if BACKEND_MODE == "lakebase":
        return pg_fetch(
            """SELECT control_id::text, audit_id, control_code, framework, control_title,
                      control_description, control_category, risk_level, frequency, control_owner
               FROM controls WHERE audit_id = %s ORDER BY control_code""",
            (audit_id,),
        )
    return fetch_sql(
        f"SELECT control_id, control_code, framework, control_title, "
        f"control_description, control_category, risk_level, frequency, control_owner "
        f"FROM {FQ}.controls WHERE audit_id = :aid ORDER BY control_code",
        {"aid": audit_id},
    )


@app.post("/api/audits/{audit_id}/controls")
async def upload_controls(audit_id: str, file: UploadFile = File(...)):
    content = await file.read()
    text = content.decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))
    inserted = 0

    for row in reader:
        control_id = str(uuid.uuid4())
        if BACKEND_MODE == "lakebase":
            pg_execute(
                """INSERT INTO controls (control_id, audit_id, control_code, framework, control_title,
                   control_description, control_category, risk_level, frequency, control_owner,
                   uploaded_by, source_file)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'app_upload', %s)""",
                (control_id, audit_id, row.get("control_code", ""), row.get("framework", ""),
                 row.get("control_title", ""), row.get("control_description", ""),
                 row.get("control_category", ""), row.get("risk_level", "MEDIUM"),
                 row.get("frequency", ""), row.get("control_owner", ""), file.filename),
            )
        else:
            execute_sql(
                f"INSERT INTO {FQ}.controls "
                f"(control_id, audit_id, control_code, framework, control_title, "
                f"control_description, control_category, risk_level, frequency, "
                f"control_owner, uploaded_by, uploaded_at, source_file, _ingested_at) "
                f"VALUES (:cid, :aid, :code, :fw, :title, :desc, :cat, :risk, :freq, "
                f":owner, 'app_upload', current_timestamp(), :src, current_timestamp())",
                {"cid": control_id, "aid": audit_id, "code": row.get("control_code", ""),
                 "fw": row.get("framework", ""), "title": row.get("control_title", ""),
                 "desc": row.get("control_description", ""), "cat": row.get("control_category", ""),
                 "risk": row.get("risk_level", "MEDIUM"), "freq": row.get("frequency", ""),
                 "owner": row.get("control_owner", ""), "src": file.filename},
            )
        inserted += 1

    return {"status": "uploaded", "audit_id": audit_id, "filename": file.filename, "controls_inserted": inserted}


# ---- Evidence Documents ----

@app.get("/api/audits/{audit_id}/evidence")
async def list_evidence(audit_id: str):
    if BACKEND_MODE == "lakebase":
        return pg_fetch(
            """SELECT document_id::text, audit_id, original_filename, file_type, file_size_bytes,
                      page_count, parse_status, ocr_applied, uploaded_at
               FROM evidence_documents WHERE audit_id = %s ORDER BY uploaded_at DESC""",
            (audit_id,),
        )
    return fetch_sql(
        f"SELECT document_id, original_filename, file_type, file_size_bytes, "
        f"page_count, parse_status, ocr_applied, uploaded_at "
        f"FROM {FQ}.evidence_documents WHERE audit_id = :aid ORDER BY uploaded_at DESC",
        {"aid": audit_id},
    )


@app.post("/api/audits/{audit_id}/evidence")
async def upload_evidence(audit_id: str, files: list[UploadFile] = File(...)):
    uploaded = []
    for file in files:
        content = await file.read()
        doc_id = str(uuid.uuid4())
        ext = file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else "bin"
        size = len(content)

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
            parse_status = "PENDING_AI_PARSE"

        if BACKEND_MODE == "lakebase":
            pg_execute(
                """INSERT INTO evidence_documents
                   (document_id, audit_id, original_filename, file_type, file_path,
                    file_size_bytes, extracted_text, parse_status, uploaded_by)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'app_upload')""",
                (doc_id, audit_id, file.filename, ext, file_path, size, extracted, parse_status),
            )
        else:
            execute_sql(
                f"INSERT INTO {FQ}.evidence_documents "
                f"(document_id, audit_id, original_filename, file_type, file_path, "
                f"file_size_bytes, extracted_text, parse_status, uploaded_by, uploaded_at, _ingested_at) "
                f"VALUES (:did, :aid, :fname, :ext, :fpath, :size, :text, :status, "
                f"'app_upload', current_timestamp(), current_timestamp())",
                {"did": doc_id, "aid": audit_id, "fname": file.filename,
                 "ext": ext, "fpath": file_path, "size": size,
                 "text": extracted, "status": parse_status},
            )

        uploaded.append({"document_id": doc_id, "filename": file.filename, "size": size, "parse_status": parse_status})

    return {"uploaded": uploaded, "count": len(uploaded)}


@app.get("/api/evidence/{document_id}")
async def get_evidence_detail(document_id: str):
    if BACKEND_MODE == "lakebase":
        row = pg_fetch_one("SELECT * FROM evidence_documents WHERE document_id::text = %s", (document_id,))
    else:
        rows = fetch_sql(f"SELECT * FROM {FQ}.evidence_documents WHERE document_id = :did", {"did": document_id})
        row = rows[0] if rows else None
    if not row:
        raise HTTPException(status_code=404, detail="Document not found")
    return row


@app.get("/api/evidence/{document_id}/content")
async def get_evidence_content(document_id: str):
    """Get the extracted text content of a document for the evidence viewer."""
    if BACKEND_MODE == "lakebase":
        row = pg_fetch_one(
            "SELECT document_id::text, extracted_text, file_type FROM evidence_documents WHERE document_id::text = %s",
            (document_id,),
        )
    else:
        rows = fetch_sql(
            f"SELECT document_id, extracted_text, file_type FROM {FQ}.evidence_documents WHERE document_id = :did",
            {"did": document_id},
        )
        row = rows[0] if rows else None
    if not row:
        raise HTTPException(status_code=404, detail="Document not found")
    return {"document_id": document_id, "content": row.get("extracted_text", ""), "file_type": row.get("file_type", "")}


# ---- Pipeline Trigger ----

@app.post("/api/audits/{audit_id}/evaluate")
async def trigger_evaluation(audit_id: str):
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    try:
        run = w.jobs.run_now(
            job_id=675224616522707,
            notebook_params={"audit_id": audit_id},
        )
        return {"audit_id": audit_id, "status": "TRIGGERED", "run_id": run.run_id, "message": "Evaluation pipeline triggered."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to trigger pipeline: {str(e)}")


@app.get("/api/audits/{audit_id}/status")
async def get_pipeline_status(audit_id: str):
    if BACKEND_MODE == "lakebase":
        total_row = pg_fetch_one("SELECT COUNT(*) as cnt FROM controls WHERE audit_id = %s", (audit_id,))
        eval_row = pg_fetch_one("SELECT COUNT(*) as cnt FROM evaluation_results WHERE audit_id = %s", (audit_id,))
        review_row = pg_fetch_one("SELECT COUNT(*) as cnt FROM evaluation_results WHERE audit_id = %s AND auditor_verdict IS NOT NULL", (audit_id,))
        total = total_row["cnt"] if total_row else 0
        eval_count = eval_row["cnt"] if eval_row else 0
        review_count = review_row["cnt"] if review_row else 0
    else:
        controls = fetch_sql(f"SELECT COUNT(*) as cnt FROM {FQ}.controls WHERE audit_id = :aid", {"aid": audit_id})
        evaluated = fetch_sql(f"SELECT COUNT(*) as cnt FROM {FQ}.evaluation_results WHERE audit_id = :aid", {"aid": audit_id})
        reviewed = fetch_sql(f"SELECT COUNT(*) as cnt FROM {FQ}.evaluation_results WHERE audit_id = :aid AND auditor_verdict IS NOT NULL", {"aid": audit_id})
        total = controls[0]["cnt"] if controls else 0
        eval_count = evaluated[0]["cnt"] if evaluated else 0
        review_count = reviewed[0]["cnt"] if reviewed else 0

    status = "COMPLETED" if eval_count >= total and total > 0 else "IN_PROGRESS" if eval_count > 0 else "PENDING"
    return {"audit_id": audit_id, "total_controls": total, "evaluated": eval_count, "reviewed": review_count, "status": status}


# ---- Evaluation Results ----

@app.get("/api/audits/{audit_id}/results")
async def get_results(audit_id: str):
    if BACKEND_MODE == "lakebase":
        return pg_fetch(
            """SELECT er.evaluation_id::text, er.control_id::text, er.ai_verdict, er.ai_confidence,
                      er.ai_reasoning, er.evidence_summary,
                      er.matched_document_ids, er.auditor_verdict, er.auditor_notes,
                      er.auditor_id, er.reviewed_at, er.model_used, er.prompt_version, er.evaluated_at,
                      c.control_code, c.control_title, c.control_description,
                      c.control_category, c.risk_level, c.framework
               FROM evaluation_results er
               JOIN controls c ON er.control_id = c.control_id
               WHERE er.audit_id = %s ORDER BY c.control_code""",
            (audit_id,),
        )
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
    if BACKEND_MODE == "lakebase":
        return pg_fetch(
            """SELECT ai_verdict, COUNT(*) AS count,
                      ROUND(AVG(ai_confidence)::numeric, 3) AS avg_confidence,
                      COUNT(CASE WHEN auditor_verdict IS NOT NULL THEN 1 END) AS reviewed_count,
                      COUNT(CASE WHEN ai_verdict != auditor_verdict AND auditor_verdict IS NOT NULL THEN 1 END) AS override_count
               FROM evaluation_results WHERE audit_id = %s GROUP BY ai_verdict""",
            (audit_id,),
        )
    return fetch_sql(
        f"SELECT ai_verdict, COUNT(*) AS count, "
        f"ROUND(AVG(ai_confidence), 3) AS avg_confidence, "
        f"COUNT(CASE WHEN auditor_verdict IS NOT NULL THEN 1 END) AS reviewed_count, "
        f"COUNT(CASE WHEN ai_verdict != auditor_verdict AND auditor_verdict IS NOT NULL THEN 1 END) AS override_count "
        f"FROM {FQ}.evaluation_results WHERE audit_id = :aid GROUP BY ai_verdict",
        {"aid": audit_id},
    )


@app.get("/api/audits/{audit_id}/evidence-matches")
async def get_all_evidence_matches(audit_id: str):
    if BACKEND_MODE == "lakebase":
        return pg_fetch(
            """SELECT m.control_id::text, c.control_code,
                      dc.chunk_id::text, dc.chunk_text, dc.chunk_index,
                      ed.original_filename, ed.document_id::text,
                      ROUND(m.similarity_score::numeric, 4) AS similarity_score, m.match_rank
               FROM control_evidence_matches m
               JOIN controls c ON m.control_id = c.control_id
               JOIN document_chunks dc ON m.chunk_id = dc.chunk_id
               JOIN evidence_documents ed ON m.document_id = ed.document_id
               WHERE m.audit_id = %s AND m.similarity_score >= 0.4 AND m.match_rank <= 8
               ORDER BY c.control_code, m.match_rank""",
            (audit_id,),
        )
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


@app.get("/api/results/{evaluation_id}")
async def get_evaluation_detail(evaluation_id: str):
    if BACKEND_MODE == "lakebase":
        row = pg_fetch_one(
            """SELECT er.evaluation_id::text, er.control_id::text, er.audit_id,
                      er.ai_verdict, er.ai_confidence, er.ai_reasoning, er.evidence_summary,
                      er.matched_document_ids, er.matched_chunk_ids,
                      er.auditor_verdict, er.auditor_notes, er.auditor_id, er.reviewed_at,
                      er.model_used, er.prompt_version, er.evaluated_at,
                      c.control_code, c.control_title, c.control_description,
                      c.control_category, c.risk_level
               FROM evaluation_results er
               JOIN controls c ON er.control_id = c.control_id
               WHERE er.evaluation_id::text = %s""",
            (evaluation_id,),
        )
    else:
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
        row = rows[0] if rows else None

    if not row:
        raise HTTPException(status_code=404, detail="Evaluation not found")

    # Get matched evidence chunks
    if BACKEND_MODE == "lakebase":
        chunks = pg_fetch(
            """SELECT dc.chunk_id::text, dc.chunk_text, dc.chunk_index,
                      ed.original_filename, ed.document_id::text,
                      m.similarity_score, m.match_rank
               FROM control_evidence_matches m
               JOIN document_chunks dc ON m.chunk_id = dc.chunk_id
               JOIN evidence_documents ed ON m.document_id = ed.document_id
               WHERE m.control_id::text = %s AND m.audit_id = %s ORDER BY m.match_rank""",
            (row["control_id"], row["audit_id"]),
        )
    else:
        chunks = fetch_sql(
            f"SELECT dc.chunk_id, dc.chunk_text, dc.chunk_index, "
            f"ed.original_filename, ed.document_id, "
            f"m.similarity_score, m.match_rank "
            f"FROM {FQ}.control_evidence_matches m "
            f"JOIN {FQ}.document_chunks dc ON m.chunk_id = dc.chunk_id "
            f"JOIN {FQ}.evidence_documents ed ON m.document_id = ed.document_id "
            f"WHERE m.control_id = :cid AND m.audit_id = :aid ORDER BY m.match_rank",
            {"cid": row["control_id"], "aid": row["audit_id"]},
        )
    row["matched_evidence"] = chunks
    return row


@app.put("/api/results/{evaluation_id}/review")
async def submit_review(evaluation_id: str, review: ReviewRequest):
    if BACKEND_MODE == "lakebase":
        pg_execute(
            """UPDATE evaluation_results
               SET auditor_verdict = %s, auditor_notes = %s, auditor_id = %s, reviewed_at = NOW()
               WHERE evaluation_id::text = %s""",
            (review.verdict, review.notes or "", review.auditor_id, evaluation_id),
        )
        row = pg_fetch_one("SELECT audit_id FROM evaluation_results WHERE evaluation_id::text = %s", (evaluation_id,))
        audit_id = row["audit_id"] if row else "UNKNOWN"
        pg_execute(
            "INSERT INTO audit_log (audit_id, user_id, action, entity_type, entity_id, details) VALUES (%s, %s, 'REVIEW', 'EVALUATION', %s, %s)",
            (audit_id, review.auditor_id, evaluation_id, f'{{"verdict": "{review.verdict}"}}'),
        )
    else:
        execute_sql(
            f"UPDATE {FQ}.evaluation_results "
            f"SET auditor_verdict = :verdict, auditor_notes = :notes, "
            f"auditor_id = :auditor, reviewed_at = current_timestamp() "
            f"WHERE evaluation_id = :eid",
            {"verdict": review.verdict, "notes": review.notes or "", "auditor": review.auditor_id, "eid": evaluation_id},
        )
        rows = fetch_sql(f"SELECT audit_id FROM {FQ}.evaluation_results WHERE evaluation_id = :eid", {"eid": evaluation_id})
        audit_id = rows[0]["audit_id"] if rows else "UNKNOWN"
        log_id = str(uuid.uuid4())
        execute_sql(
            f"INSERT INTO {FQ}.audit_log VALUES (:lid, :aid, :auditor, 'REVIEW', 'EVALUATION', :eid, :detail, current_timestamp())",
            {"lid": log_id, "aid": audit_id, "auditor": review.auditor_id, "eid": evaluation_id, "detail": f'{{"verdict": "{review.verdict}"}}'},
        )

    return {"status": "reviewed", "evaluation_id": evaluation_id}


# ---- Annotations ----

@app.get("/api/audits/{audit_id}/annotations")
async def get_annotations(audit_id: str, document_id: Optional[str] = None, control_id: Optional[str] = None):
    """Get annotations for evidence highlighting."""
    if BACKEND_MODE == "lakebase":
        sql = """SELECT annotation_id::text, control_id::text, document_id::text, chunk_id::text,
                        audit_id, start_char, end_char, similarity_score, explanation_text,
                        control_code, control_title, violation_type, created_at
                 FROM annotations WHERE audit_id = %s"""
        params = [audit_id]
        if document_id:
            sql += " AND document_id::text = %s"
            params.append(document_id)
        if control_id:
            sql += " AND control_id::text = %s"
            params.append(control_id)
        sql += " ORDER BY start_char"
        return pg_fetch(sql, tuple(params))
    else:
        sql = (
            f"SELECT annotation_id, control_id, document_id, chunk_id, "
            f"audit_id, start_char, end_char, similarity_score, explanation_text, "
            f"control_code, control_title, violation_type, created_at "
            f"FROM {FQ}.annotations WHERE audit_id = :aid"
        )
        p = {"aid": audit_id}
        if document_id:
            sql += " AND document_id = :did"
            p["did"] = document_id
        if control_id:
            sql += " AND control_id = :cid"
            p["cid"] = control_id
        sql += " ORDER BY start_char"
        return fetch_sql(sql, p)


# ---- Dashboard Stats ----

@app.get("/api/audits/{audit_id}/dashboard")
async def get_dashboard_stats(audit_id: str):
    """Aggregate dashboard statistics."""
    if BACKEND_MODE == "lakebase":
        stats = pg_fetch_one(
            """SELECT
                (SELECT COUNT(*) FROM controls WHERE audit_id = %s) AS total_controls,
                (SELECT COUNT(*) FROM evaluation_results WHERE audit_id = %s) AS controls_tested,
                (SELECT COUNT(*) FROM evaluation_results WHERE audit_id = %s AND ai_verdict = 'PASS') AS pass_count,
                (SELECT COUNT(*) FROM evaluation_results WHERE audit_id = %s AND ai_verdict = 'FAIL') AS fail_count,
                (SELECT COUNT(*) FROM evaluation_results WHERE audit_id = %s AND ai_verdict = 'INSUFFICIENT_EVIDENCE') AS insufficient_count,
                (SELECT COALESCE(AVG(ai_confidence), 0) FROM evaluation_results WHERE audit_id = %s) AS avg_confidence,
                (SELECT COUNT(*) FROM evidence_documents WHERE audit_id = %s) AS total_evidence,
                (SELECT COUNT(*) FROM evidence_documents WHERE audit_id = %s AND parse_status = 'COMPLETED') AS evidence_processed,
                (SELECT COUNT(*) FROM annotations WHERE audit_id = %s) AS total_findings""",
            (audit_id,) * 9,
        )
    else:
        stats = {
            "total_controls": 0, "controls_tested": 0, "pass_count": 0, "fail_count": 0,
            "insufficient_count": 0, "avg_confidence": 0, "total_evidence": 0,
            "evidence_processed": 0, "total_findings": 0,
        }
        try:
            rows = fetch_sql(
                f"SELECT COUNT(*) as c FROM {FQ}.controls WHERE audit_id = :aid", {"aid": audit_id}
            )
            stats["total_controls"] = rows[0]["c"] if rows else 0
        except Exception:
            pass

    if stats:
        tested = stats.get("controls_tested", 0) or 0
        passed = stats.get("pass_count", 0) or 0
        stats["compliance_rate"] = round((passed / tested * 100) if tested > 0 else 0, 1)
        stats["pending_count"] = (stats.get("total_controls", 0) or 0) - tested
    return stats


# ---- Findings (derived from FAIL results + annotations) ----

@app.get("/api/audits/{audit_id}/findings")
async def get_findings(audit_id: str):
    if BACKEND_MODE == "lakebase":
        return pg_fetch(
            """SELECT er.evaluation_id::text AS finding_id, er.audit_id,
                      er.control_id::text, c.control_code, c.control_title,
                      CASE WHEN c.risk_level = 'HIGH' THEN 'CRITICAL'
                           WHEN c.risk_level = 'MEDIUM' THEN 'HIGH'
                           ELSE 'MEDIUM' END AS severity,
                      CASE WHEN er.auditor_verdict IS NOT NULL THEN 'CONFIRMED'
                           ELSE 'OPEN' END AS status,
                      c.control_title AS title,
                      er.ai_reasoning AS description,
                      er.ai_confidence AS risk_score,
                      er.auditor_notes AS remediation_plan,
                      er.auditor_id AS assigned_to,
                      er.evaluated_at AS created_at, er.reviewed_at AS updated_at
               FROM evaluation_results er
               JOIN controls c ON er.control_id = c.control_id
               WHERE er.audit_id = %s AND er.ai_verdict = 'FAIL'
               ORDER BY c.risk_level DESC, er.ai_confidence DESC""",
            (audit_id,),
        )
    return []


# ---- Audit Log ----

@app.get("/api/audits/{audit_id}/audit-log")
async def get_audit_log(audit_id: str, limit: int = Query(default=100, le=1000)):
    if BACKEND_MODE == "lakebase":
        return pg_fetch(
            "SELECT log_id::text, audit_id, user_id, action, entity_type, entity_id, details, timestamp FROM audit_log WHERE audit_id = %s ORDER BY timestamp DESC LIMIT %s",
            (audit_id, limit),
        )
    return fetch_sql(
        f"SELECT * FROM {FQ}.audit_log WHERE audit_id = :aid ORDER BY timestamp DESC LIMIT {limit}",
        {"aid": audit_id},
    )


# ---- Export ----

@app.get("/api/audits/{audit_id}/export")
async def export_results(audit_id: str):
    if BACKEND_MODE == "lakebase":
        results = pg_fetch(
            """SELECT c.control_code, c.control_title, c.control_description,
                      c.control_category, c.risk_level, c.framework,
                      er.ai_verdict, er.ai_confidence, er.ai_reasoning, er.evidence_summary,
                      er.auditor_verdict, er.auditor_notes, er.auditor_id, er.reviewed_at,
                      COALESCE(er.auditor_verdict, er.ai_verdict) AS final_verdict
               FROM evaluation_results er
               JOIN controls c ON er.control_id = c.control_id
               WHERE er.audit_id = %s ORDER BY c.control_code""",
            (audit_id,),
        )
        summary = pg_fetch(
            """SELECT COALESCE(er.auditor_verdict, er.ai_verdict) AS final_verdict, COUNT(*) AS count
               FROM evaluation_results er WHERE er.audit_id = %s
               GROUP BY COALESCE(er.auditor_verdict, er.ai_verdict)""",
            (audit_id,),
        )
    else:
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
            f"SELECT COALESCE(er.auditor_verdict, er.ai_verdict) AS final_verdict, COUNT(*) AS count "
            f"FROM {FQ}.evaluation_results er WHERE er.audit_id = :aid "
            f"GROUP BY COALESCE(er.auditor_verdict, er.ai_verdict)",
            {"aid": audit_id},
        )

    return {"audit_id": audit_id, "generated_at": datetime.utcnow().isoformat(), "summary": summary, "results": results}


# ---- pgvector Similarity Search API ----

@app.post("/api/audits/{audit_id}/similarity-search")
async def run_similarity_search(audit_id: str, query_embedding: list[float], top_k: int = 15, threshold: float = 0.4):
    """Direct pgvector similarity search endpoint."""
    if BACKEND_MODE != "lakebase":
        raise HTTPException(status_code=400, detail="Similarity search requires Lakebase backend")
    results = similarity_search(query_embedding, audit_id, top_k, threshold)
    return {"results": results, "count": len(results)}


# ---- Serve React dist files ----

if os.path.exists(DIST_DIR):
    app.mount("/assets", StaticFiles(directory=os.path.join(DIST_DIR, "assets")), name="assets")

# Fallback: serve legacy static
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# Catch-all for React Router (must be last)
@app.get("/{full_path:path}")
async def serve_react_app(full_path: str):
    """Serve React app for all non-API routes (client-side routing support)."""
    if full_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="API endpoint not found")
    index = os.path.join(DIST_DIR, "index.html")
    if os.path.exists(index):
        return FileResponse(index)
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))
