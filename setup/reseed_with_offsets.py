"""
Reseed document_chunks with validated character offsets.
Offsets are verified against the actual document text to ensure highlighting works.
"""
import subprocess
import json
import uuid
import re


PROFILE = "fevm-akash-finance-app"
WAREHOUSE = "1b1d59e180e4ac26"
FQ = "main.audit_schema"
AUDIT_ID = "AUD-2026-001"


def run_sql(sql, quiet=False):
    r = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements",
         "-p", PROFILE, "--json",
         json.dumps({"warehouse_id": WAREHOUSE, "statement": sql, "wait_timeout": "30s"})],
        capture_output=True, text=True, timeout=60,
    )
    d = json.loads(r.stdout)
    if not quiet:
        state = d["status"]["state"]
        if state == "FAILED":
            print(f"  FAIL: {d['status'].get('error',{}).get('message','')[:200]}")
    return d


def fetch_sql(sql):
    d = run_sql(sql, quiet=True)
    if d["status"]["state"] != "SUCCEEDED":
        return []
    cols = [c["name"] for c in d.get("manifest", {}).get("schema", {}).get("columns", [])]
    rows = d.get("result", {}).get("data_array", [])
    return [dict(zip(cols, row)) for row in rows]


def escape(s):
    if s is None:
        return "NULL"
    return "'" + str(s).replace("'", "''").replace("\\", "\\\\") + "'"


def chunk_with_offsets(text, max_size=1200):
    """Chunk text by paragraph boundaries with verified character offsets."""
    paragraphs = re.split(r'(\n\s*\n)', text)  # Keep the delimiters
    chunks = []
    current_text = ""
    current_start = 0
    pos = 0

    for part in paragraphs:
        # Track position in the original text
        if not part.strip():
            pos += len(part)
            if current_text:
                current_text += part
            continue

        if len(current_text) + len(part) <= max_size:
            if not current_text:
                current_start = pos
            current_text += part
        else:
            if current_text.strip():
                # Verify: the text at these offsets matches
                actual = text[current_start:current_start + len(current_text)]
                if actual == current_text:
                    chunks.append({
                        "text": current_text.strip(),
                        "start": current_start,
                        "end": current_start + len(current_text),
                    })
                else:
                    # Fallback: find it
                    idx = text.find(current_text.strip(), max(0, current_start - 50))
                    if idx >= 0:
                        chunks.append({
                            "text": current_text.strip(),
                            "start": idx,
                            "end": idx + len(current_text.strip()),
                        })
            current_text = part
            current_start = pos

        pos += len(part)

    if current_text.strip():
        # Verify final chunk
        actual = text[current_start:current_start + len(current_text)]
        if actual == current_text:
            chunks.append({
                "text": current_text.strip(),
                "start": current_start,
                "end": current_start + len(current_text),
            })
        else:
            idx = text.find(current_text.strip(), max(0, current_start - 50))
            if idx >= 0:
                chunks.append({
                    "text": current_text.strip(),
                    "start": idx,
                    "end": idx + len(current_text.strip()),
                })

    return chunks


def main():
    print("Fetching evidence documents...")
    docs = fetch_sql(f"SELECT document_id, original_filename, extracted_text FROM {FQ}.evidence_documents WHERE audit_id = '{AUDIT_ID}' AND extracted_text IS NOT NULL")
    print(f"Found {len(docs)} documents")

    # Clear existing chunks for this audit
    run_sql(f"DELETE FROM {FQ}.document_chunks WHERE audit_id = '{AUDIT_ID}'")
    print("Cleared existing chunks")

    # Clear existing matches
    run_sql(f"DELETE FROM {FQ}.control_evidence_matches WHERE audit_id = '{AUDIT_ID}'")
    print("Cleared existing matches")

    all_chunks = {}  # doc_id -> list of chunks

    for doc in docs:
        doc_id = doc["document_id"]
        text = doc["extracted_text"]
        fname = doc["original_filename"]

        if not text or len(text.strip()) < 20:
            continue

        chunks = chunk_with_offsets(text)
        all_chunks[doc_id] = []

        for i, chunk in enumerate(chunks):
            chunk_id = str(uuid.uuid4())
            # Validate offset
            actual_slice = text[chunk["start"]:chunk["end"]].strip()
            chunk_text_clean = chunk["text"].strip()
            if actual_slice[:50] != chunk_text_clean[:50]:
                print(f"  WARNING: offset mismatch for {fname} chunk {i}")
                print(f"    Expected: {chunk_text_clean[:60]}")
                print(f"    Got:      {actual_slice[:60]}")
                continue

            token_count = len(chunk["text"].split())
            sql = (
                f"INSERT INTO {FQ}.document_chunks "
                f"(chunk_id, document_id, audit_id, chunk_index, chunk_text, "
                f"start_char, end_char, token_count, _created_at) "
                f"VALUES ({escape(chunk_id)}, {escape(doc_id)}, {escape(AUDIT_ID)}, "
                f"{i}, {escape(chunk['text'])}, "
                f"{chunk['start']}, {chunk['end']}, {token_count}, current_timestamp())"
            )
            run_sql(sql, quiet=True)

            all_chunks[doc_id].append({
                "chunk_id": chunk_id,
                "doc_id": doc_id,
                "index": i,
                "text": chunk["text"],
                "start": chunk["start"],
                "end": chunk["end"],
                "filename": fname,
            })

        print(f"  {fname}: {len(all_chunks.get(doc_id, []))} chunks with verified offsets")

    total_chunks = sum(len(v) for v in all_chunks.values())
    print(f"\nTotal chunks created: {total_chunks}")

    # Now create evidence matches linking controls to chunks
    print("\nCreating evidence matches...")
    controls = fetch_sql(f"SELECT control_id, control_code, control_description FROM {FQ}.controls WHERE audit_id = '{AUDIT_ID}'")

    # Simple keyword matching for demo (in production, this uses pgvector embeddings)
    keyword_map = {
        "SOX-AC-001": ["access", "role-based", "provisioning", "rbac", "authorization"],
        "SOX-AC-002": ["multi-factor", "mfa", "authentication", "two-factor", "token"],
        "SOX-AC-003": ["access review", "quarterly", "terminated", "deprovisioning"],
        "SOX-AC-004": ["privileged", "admin", "emergency access", "least-privilege"],
        "SOX-CM-001": ["change management", "change advisory", "approval", "deployment"],
        "SOX-CM-002": ["segregation", "duties", "requestor", "approve"],
        "SOX-FR-001": ["financial close", "month-end", "reconciliation", "journal"],
        "SOX-FR-002": ["journal entry", "posting", "approved", "supporting documentation"],
        "SOX-VM-001": ["vulnerability", "scan", "remediation", "cvss", "qualys"],
        "SOX-BC-001": ["business continuity", "disaster recovery", "rto", "rpo", "dr test"],
        "SOX-BC-002": ["backup", "recovery", "restoration", "encrypted backup"],
        "SOX-NW-001": ["network", "segmentation", "firewall", "intrusion detection", "vlan"],
    }

    match_count = 0
    for ctrl in controls:
        ctrl_id = ctrl["control_id"]
        ctrl_code = ctrl["control_code"]
        keywords = keyword_map.get(ctrl_code, [])

        rank = 0
        for doc_id, chunks in all_chunks.items():
            for chunk in chunks:
                text_lower = chunk["text"].lower()
                # Score based on keyword matches
                hits = sum(1 for kw in keywords if kw.lower() in text_lower)
                if hits == 0:
                    continue

                score = min(0.95, 0.5 + hits * 0.1)
                rank += 1
                if rank > 8:
                    break

                match_id = str(uuid.uuid4())
                sql = (
                    f"INSERT INTO {FQ}.control_evidence_matches "
                    f"(match_id, control_id, chunk_id, document_id, audit_id, "
                    f"similarity_score, match_rank, _matched_at) "
                    f"VALUES ({escape(match_id)}, {escape(ctrl_id)}, {escape(chunk['chunk_id'])}, "
                    f"{escape(chunk['doc_id'])}, {escape(AUDIT_ID)}, "
                    f"{score}, {rank}, current_timestamp())"
                )
                run_sql(sql, quiet=True)
                match_count += 1

        if rank > 0:
            print(f"  {ctrl_code}: {rank} matches")

    print(f"\nTotal matches: {match_count}")
    print("Done! Chunks and matches reseeded with validated offsets.")


if __name__ == "__main__":
    main()
