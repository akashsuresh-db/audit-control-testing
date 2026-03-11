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
    """Get evidence matches with char offsets and contextual paragraphs."""
    if BACKEND_MODE == "lakebase":
        matches = pg_fetch(
            """SELECT m.match_id::text, m.control_id::text, c.control_code, c.control_title,
                      dc.chunk_id::text, dc.chunk_text, dc.chunk_index,
                      dc.start_char, dc.end_char,
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
    else:
        matches = fetch_sql(
            f"SELECT m.match_id, m.control_id, c.control_code, c.control_title, "
            f"dc.chunk_id, dc.chunk_text, dc.chunk_index, "
            f"dc.start_char, dc.end_char, "
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

    # Expand each match to include contextual paragraph from full document
    doc_cache = {}
    for match in matches:
        doc_id = match.get("document_id")
        start = match.get("start_char")
        end = match.get("end_char")

        # Get full doc text (cached)
        if doc_id and doc_id not in doc_cache:
            if BACKEND_MODE == "lakebase":
                doc_row = pg_fetch_one("SELECT extracted_text FROM evidence_documents WHERE document_id::text = %s", (doc_id,))
            else:
                doc_rows = fetch_sql(f"SELECT extracted_text FROM {FQ}.evidence_documents WHERE document_id = :did", {"did": doc_id})
                doc_row = doc_rows[0] if doc_rows else None
            doc_cache[doc_id] = (doc_row or {}).get("extracted_text", "")

        full_text = doc_cache.get(doc_id, "")

        # Build context paragraph: expand to surrounding paragraph boundaries
        if full_text and start is not None and end is not None:
            start = int(start)
            end = min(int(end), len(full_text))

            # Find paragraph start (look back for double newline)
            ctx_start = full_text.rfind("\n\n", 0, start)
            ctx_start = ctx_start + 2 if ctx_start >= 0 else max(0, start - 200)

            # Find paragraph end (look forward for double newline)
            ctx_end = full_text.find("\n\n", end)
            ctx_end = ctx_end if ctx_end >= 0 else min(len(full_text), end + 200)

            match["context_text"] = full_text[ctx_start:ctx_end].strip()
            match["context_start"] = ctx_start
            match["context_end"] = ctx_end
        else:
            match["context_text"] = match.get("chunk_text", "")
            match["context_start"] = start or 0
            match["context_end"] = end or 0

    return matches


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
    """Get annotations for evidence highlighting.
    Auto-generates from evidence matches when no explicit annotations exist."""

    # Try explicit annotations first
    annotations = []
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
        annotations = pg_fetch(sql, tuple(params))
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
        try:
            annotations = fetch_sql(sql, p)
        except Exception:
            annotations = []

    # Auto-generate annotations from evidence matches if none exist
    if not annotations and control_id:
        if BACKEND_MODE == "lakebase":
            matches = pg_fetch(
                """SELECT m.match_id::text AS annotation_id, m.control_id::text, m.document_id::text,
                          dc.chunk_id::text, %s AS audit_id,
                          dc.start_char, dc.end_char, m.similarity_score,
                          CONCAT(c.control_code, ': Evidence match from ', ed.original_filename) AS explanation_text,
                          c.control_code, c.control_title, 'EVIDENCE_MATCH' AS violation_type,
                          m._matched_at AS created_at
                   FROM control_evidence_matches m
                   JOIN document_chunks dc ON m.chunk_id = dc.chunk_id
                   JOIN controls c ON m.control_id = c.control_id
                   JOIN evidence_documents ed ON m.document_id = ed.document_id
                   WHERE m.audit_id = %s AND m.control_id::text = %s
                     AND m.similarity_score >= 0.4 AND dc.start_char IS NOT NULL
                   ORDER BY dc.start_char""",
                (audit_id, audit_id, control_id),
            )
        else:
            matches = fetch_sql(
                f"SELECT m.match_id AS annotation_id, m.control_id, m.document_id, "
                f"dc.chunk_id, m.audit_id, "
                f"dc.start_char, dc.end_char, m.similarity_score, "
                f"CONCAT(c.control_code, ': Evidence match from ', ed.original_filename) AS explanation_text, "
                f"c.control_code, c.control_title, 'EVIDENCE_MATCH' AS violation_type, "
                f"m._matched_at AS created_at "
                f"FROM {FQ}.control_evidence_matches m "
                f"JOIN {FQ}.document_chunks dc ON m.chunk_id = dc.chunk_id "
                f"JOIN {FQ}.controls c ON m.control_id = c.control_id "
                f"JOIN {FQ}.evidence_documents ed ON m.document_id = ed.document_id "
                f"WHERE m.audit_id = :aid AND m.control_id = :cid "
                f"AND m.similarity_score >= 0.4 "
                f"ORDER BY dc.start_char",
                {"aid": audit_id, "cid": control_id},
            )
        annotations = [m for m in matches if m.get("start_char") is not None]

    return annotations


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


# ---- Evidence Sufficiency Engine ----

@app.get("/api/audits/{audit_id}/sufficiency")
async def get_evidence_sufficiency(audit_id: str):
    """Compute evidence sufficiency scores for each control."""
    if BACKEND_MODE == "databricks":
        controls_data = fetch_sql(
            f"SELECT c.control_id, c.control_code, c.control_title, c.control_description, c.risk_level "
            f"FROM {FQ}.controls c WHERE c.audit_id = :aid ORDER BY c.control_code",
            {"aid": audit_id},
        )
        matches_data = fetch_sql(
            f"SELECT m.control_id, m.similarity_score, m.match_rank, ed.original_filename, ed.document_id "
            f"FROM {FQ}.control_evidence_matches m "
            f"JOIN {FQ}.evidence_documents ed ON m.document_id = ed.document_id "
            f"WHERE m.audit_id = :aid AND m.similarity_score >= 0.4",
            {"aid": audit_id},
        )
        results_data = fetch_sql(
            f"SELECT control_id, ai_verdict, ai_confidence FROM {FQ}.evaluation_results WHERE audit_id = :aid",
            {"aid": audit_id},
        )
    else:
        controls_data = pg_fetch("SELECT control_id::text, control_code, control_title, control_description, risk_level FROM controls WHERE audit_id = %s ORDER BY control_code", (audit_id,))
        matches_data = pg_fetch("SELECT m.control_id::text, m.similarity_score, m.match_rank, ed.original_filename, ed.document_id::text FROM control_evidence_matches m JOIN evidence_documents ed ON m.document_id = ed.document_id WHERE m.audit_id = %s AND m.similarity_score >= 0.4", (audit_id,))
        results_data = pg_fetch("SELECT control_id::text, ai_verdict, ai_confidence FROM evaluation_results WHERE audit_id = %s", (audit_id,))

    # Build lookup maps
    matches_by_ctrl = {}
    for m in matches_data:
        cid = m["control_id"]
        matches_by_ctrl.setdefault(cid, []).append(m)

    results_by_ctrl = {r["control_id"]: r for r in results_data}

    sufficiency = []
    for ctrl in controls_data:
        cid = ctrl["control_id"]
        ctrl_matches = matches_by_ctrl.get(cid, [])
        result = results_by_ctrl.get(cid, {})

        # Compute sufficiency metrics
        source_count = len(set(m.get("document_id", "") for m in ctrl_matches))
        avg_score = sum(m.get("similarity_score", 0) for m in ctrl_matches) / max(len(ctrl_matches), 1)
        max_score = max((m.get("similarity_score", 0) for m in ctrl_matches), default=0)
        high_quality = sum(1 for m in ctrl_matches if (m.get("similarity_score", 0) or 0) >= 0.7)

        # Score formula: sources * 15 + avg_quality * 30 + high_matches * 10 + confidence * 20
        confidence = float(result.get("ai_confidence", 0) or 0)
        raw_score = min(100, (
            min(source_count, 3) * 15 +
            avg_score * 30 +
            min(high_quality, 3) * 10 +
            confidence * 20 +
            (5 if result.get("ai_verdict") == "PASS" else 0)
        ))
        score = round(raw_score)

        if score >= 70:
            status = "SUFFICIENT"
        elif score >= 40:
            status = "PARTIAL"
        else:
            status = "INSUFFICIENT"

        sufficiency.append({
            "control_id": cid,
            "control_code": ctrl["control_code"],
            "control_title": ctrl["control_title"],
            "risk_level": ctrl["risk_level"],
            "evidence_sources": source_count,
            "total_matches": len(ctrl_matches),
            "avg_similarity": round(avg_score, 3),
            "max_similarity": round(max_score, 3),
            "high_quality_matches": high_quality,
            "ai_verdict": result.get("ai_verdict"),
            "ai_confidence": confidence,
            "sufficiency_score": score,
            "sufficiency_status": status,
        })

    return sufficiency


# ---- Workpapers ----

@app.get("/api/audits/{audit_id}/workpapers")
async def get_workpapers(audit_id: str):
    """Generate audit workpapers for each control."""
    if BACKEND_MODE == "databricks":
        controls_data = fetch_sql(
            f"SELECT c.control_id, c.control_code, c.control_title, c.control_description, "
            f"c.control_category, c.risk_level, c.frequency "
            f"FROM {FQ}.controls c WHERE c.audit_id = :aid ORDER BY c.control_code",
            {"aid": audit_id},
        )
        results_data = fetch_sql(
            f"SELECT er.control_id, er.ai_verdict, er.ai_confidence, er.ai_reasoning, "
            f"er.evidence_summary, er.auditor_verdict, er.auditor_notes, er.auditor_id, er.reviewed_at "
            f"FROM {FQ}.evaluation_results er WHERE er.audit_id = :aid",
            {"aid": audit_id},
        )
        matches_data = fetch_sql(
            f"SELECT m.control_id, ed.original_filename, m.similarity_score "
            f"FROM {FQ}.control_evidence_matches m "
            f"JOIN {FQ}.evidence_documents ed ON m.document_id = ed.document_id "
            f"WHERE m.audit_id = :aid AND m.similarity_score >= 0.4 ORDER BY m.match_rank",
            {"aid": audit_id},
        )
    else:
        controls_data = pg_fetch("SELECT control_id::text, control_code, control_title, control_description, control_category, risk_level, frequency FROM controls WHERE audit_id = %s ORDER BY control_code", (audit_id,))
        results_data = pg_fetch("SELECT control_id::text, ai_verdict, ai_confidence, ai_reasoning, evidence_summary, auditor_verdict, auditor_notes, auditor_id, reviewed_at FROM evaluation_results WHERE audit_id = %s", (audit_id,))
        matches_data = pg_fetch("SELECT m.control_id::text, ed.original_filename, m.similarity_score FROM control_evidence_matches m JOIN evidence_documents ed ON m.document_id = ed.document_id WHERE m.audit_id = %s AND m.similarity_score >= 0.4 ORDER BY m.match_rank", (audit_id,))

    results_map = {r["control_id"]: r for r in results_data}
    matches_map = {}
    for m in matches_data:
        matches_map.setdefault(m["control_id"], []).append(m)

    workpapers = []
    for ctrl in controls_data:
        cid = ctrl["control_id"]
        result = results_map.get(cid, {})
        ctrl_matches = matches_map.get(cid, [])
        final_verdict = result.get("auditor_verdict") or result.get("ai_verdict") or "NOT TESTED"

        evidence_reviewed = [
            {"filename": m["original_filename"], "relevance": round(float(m.get("similarity_score", 0)) * 100)}
            for m in ctrl_matches[:8]
        ]

        # Generate testing procedure based on control category
        cat = ctrl.get("control_category", "General")
        procedures = {
            "Access Control": "1. Obtain access control policy and configuration.\n2. Review user provisioning process.\n3. Inspect sample of access requests for proper authorization.\n4. Verify periodic access review completion.\n5. Test for segregation of duties compliance.",
            "Change Management": "1. Obtain change management policy.\n2. Review change advisory board meeting minutes.\n3. Select sample of change tickets and verify approval workflow.\n4. Test segregation between development and production.\n5. Verify post-implementation review documentation.",
            "Financial Reporting": "1. Obtain month-end close checklist.\n2. Review reconciliation documentation.\n3. Inspect journal entries for proper approval.\n4. Verify management sign-off procedures.\n5. Test completeness and accuracy of financial records.",
            "Vulnerability Management": "1. Obtain vulnerability scan reports.\n2. Verify scanning frequency meets policy requirements.\n3. Review remediation SLAs for critical and high findings.\n4. Inspect evidence of patching and remediation.\n5. Evaluate trend analysis and improvement metrics.",
            "Business Continuity": "1. Obtain business continuity plan.\n2. Review disaster recovery test results.\n3. Verify RTO and RPO alignment with business requirements.\n4. Inspect backup and restoration test evidence.\n5. Evaluate plan update frequency and currency.",
            "Network Security": "1. Obtain network architecture documentation.\n2. Review firewall rule sets and change logs.\n3. Verify network segmentation controls.\n4. Inspect intrusion detection system alerts.\n5. Test perimeter security controls.",
        }
        procedure = procedures.get(cat, f"1. Review {cat} policy.\n2. Inspect supporting evidence.\n3. Evaluate control effectiveness.\n4. Document findings.")

        conclusion_map = {
            "PASS": f"Based on the evidence reviewed, the control '{ctrl['control_title']}' is operating effectively. Evidence demonstrates compliance with the control objective.",
            "FAIL": f"Testing identified deficiencies in the control '{ctrl['control_title']}'. The evidence does not support that the control is operating effectively. Findings have been documented for management remediation.",
            "INSUFFICIENT_EVIDENCE": f"Insufficient evidence was available to evaluate the control '{ctrl['control_title']}'. Additional evidence should be requested to complete the assessment.",
            "NOT TESTED": f"The control '{ctrl['control_title']}' has not yet been tested.",
        }

        workpapers.append({
            "control_id": cid,
            "control_code": ctrl["control_code"],
            "control_title": ctrl["control_title"],
            "control_objective": ctrl["control_description"],
            "control_category": cat,
            "risk_level": ctrl["risk_level"],
            "frequency": ctrl.get("frequency", ""),
            "testing_procedure": procedure,
            "evidence_reviewed": evidence_reviewed,
            "test_result": final_verdict,
            "ai_confidence": float(result.get("ai_confidence", 0) or 0),
            "ai_reasoning": result.get("ai_reasoning", ""),
            "auditor_verdict": result.get("auditor_verdict"),
            "auditor_notes": result.get("auditor_notes", ""),
            "auditor_id": result.get("auditor_id", ""),
            "reviewed_at": result.get("reviewed_at"),
            "conclusion": conclusion_map.get(final_verdict, ""),
        })

    return workpapers


# ---- Sampling Engine ----

class SamplingRequest(BaseModel):
    population_size: int
    sample_size: int
    method: str = "random"  # random, risk_based, stratified
    risk_field: Optional[str] = None
    strata_field: Optional[str] = None

@app.post("/api/audits/{audit_id}/sampling")
async def generate_sample(audit_id: str, req: SamplingRequest):
    """Generate audit test sample from a population."""
    import random
    import math

    population = list(range(1, req.population_size + 1))
    sample_size = min(req.sample_size, req.population_size)

    if req.method == "random":
        sample = sorted(random.sample(population, sample_size))
        method_desc = f"Simple random sampling: {sample_size} items selected from population of {req.population_size}"
    elif req.method == "risk_based":
        # Weight toward higher-numbered items (simulating higher risk)
        weights = [i ** 1.5 for i in population]
        total = sum(weights)
        probs = [w / total for w in weights]
        sample = sorted(random.choices(population, weights=probs, k=sample_size))
        sample = sorted(list(set(sample)))[:sample_size]
        method_desc = f"Risk-based sampling: {len(sample)} items selected with higher weighting for high-risk items"
    elif req.method == "stratified":
        # Split into 3 strata
        strata_size = req.population_size // 3
        strata = [
            population[:strata_size],
            population[strata_size:strata_size * 2],
            population[strata_size * 2:],
        ]
        per_stratum = max(1, sample_size // 3)
        sample = []
        for s in strata:
            sample.extend(sorted(random.sample(s, min(per_stratum, len(s)))))
        sample = sorted(sample)[:sample_size]
        method_desc = f"Stratified sampling: {len(sample)} items selected proportionally across 3 strata"
    else:
        sample = sorted(random.sample(population, sample_size))
        method_desc = f"Random sampling: {sample_size} items"

    # Calculate confidence level (simplified)
    z = 1.96  # 95% confidence
    p = 0.5
    margin = z * math.sqrt(p * (1 - p) / sample_size) if sample_size > 0 else 1
    confidence = round((1 - margin) * 100, 1)

    return {
        "audit_id": audit_id,
        "population_size": req.population_size,
        "sample_size": len(sample),
        "method": req.method,
        "method_description": method_desc,
        "confidence_level": max(0, confidence),
        "margin_of_error": round(margin * 100, 1),
        "sample_items": sample,
        "strata_count": 3 if req.method == "stratified" else 1,
    }


# ---- Findings Management ----

class FindingCreate(BaseModel):
    control_id: str
    risk_rating: str = "HIGH"
    title: str
    root_cause: str = ""
    impact: str = ""
    recommendation: str = ""

class FindingUpdate(BaseModel):
    status: Optional[str] = None
    management_response: Optional[str] = None
    remediation_status: Optional[str] = None
    risk_rating: Optional[str] = None

@app.post("/api/audits/{audit_id}/findings")
async def create_finding(audit_id: str, finding: FindingCreate):
    """Create a new audit finding."""
    finding_id = f"FND-{uuid.uuid4().hex[:8].upper()}"
    if BACKEND_MODE == "databricks":
        execute_sql(
            f"INSERT INTO {FQ}.audit_findings "
            f"(finding_id, audit_id, control_id, risk_rating, title, root_cause, impact, "
            f"recommendation, status, created_at) "
            f"VALUES (:fid, :aid, :cid, :risk, :title, :cause, :impact, :rec, 'OPEN', current_timestamp())",
            {"fid": finding_id, "aid": audit_id, "cid": finding.control_id,
             "risk": finding.risk_rating, "title": finding.title,
             "cause": finding.root_cause, "impact": finding.impact,
             "rec": finding.recommendation},
        )
    else:
        pg_execute(
            "INSERT INTO audit_findings (finding_id, audit_id, control_id, risk_rating, title, root_cause, impact, recommendation, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'OPEN')",
            (finding_id, audit_id, finding.control_id, finding.risk_rating, finding.title, finding.root_cause, finding.impact, finding.recommendation),
        )
    return {"finding_id": finding_id, "status": "OPEN"}

@app.put("/api/findings/{finding_id}")
async def update_finding(finding_id: str, update: FindingUpdate):
    """Update finding status or management response."""
    sets = []
    params = {}
    if update.status:
        sets.append("status = :status")
        params["status"] = update.status
    if update.management_response:
        sets.append("management_response = :mgmt")
        params["mgmt"] = update.management_response
    if update.remediation_status:
        sets.append("remediation_status = :rstat")
        params["rstat"] = update.remediation_status
    if update.risk_rating:
        sets.append("risk_rating = :risk")
        params["risk"] = update.risk_rating
    if not sets:
        return {"status": "no changes"}

    params["fid"] = finding_id
    if BACKEND_MODE == "databricks":
        execute_sql(
            f"UPDATE {FQ}.audit_findings SET {', '.join(sets)}, updated_at = current_timestamp() WHERE finding_id = :fid",
            params,
        )
    else:
        # Convert to pg-style params
        pg_sets = []
        pg_params = []
        for s in sets:
            col = s.split(" = ")[0]
            pg_sets.append(f"{col} = %s")
            key = s.split(":")[1] if ":" in s else col
            pg_params.append(params.get(key.strip(), ""))
        pg_params.append(finding_id)
        pg_execute(f"UPDATE audit_findings SET {', '.join(pg_sets)}, updated_at = NOW() WHERE finding_id = %s", tuple(pg_params))

    return {"finding_id": finding_id, "status": "updated"}


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
