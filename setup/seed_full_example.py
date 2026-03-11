"""
Seed Lakebase with complete example data: chunks, evaluation results,
evidence matches, and annotations for the AUD-2026-001 audit.
"""
import subprocess
import json
import uuid
import re
import psycopg2

LAKEBASE_INSTANCE = "akash-finance-app"
LAKEBASE_HOST = "instance-383773af-2ab5-4bfd-971d-9dba95011ab4.database.cloud.databricks.com"
LAKEBASE_DB = "audit_platform"
PROFILE = "fevm-akash-finance-app"
AUDIT_ID = "AUD-2026-001"


def get_token():
    result = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/database/credentials",
         "-p", PROFILE, "--json",
         json.dumps({"request_id": "seed", "instance_names": [LAKEBASE_INSTANCE]})],
        capture_output=True, text=True, timeout=15,
    )
    return json.loads(result.stdout)["token"]


def get_conn():
    return psycopg2.connect(
        host=LAKEBASE_HOST, port=5432, database=LAKEBASE_DB,
        user="akash.s@databricks.com", password=get_token(), sslmode="require",
    )


def chunk_text(text, max_size=1200, overlap=150):
    """Create chunks with character offsets."""
    paragraphs = re.split(r'\n\s*\n', text)
    chunks = []
    current = ""
    current_start = 0
    pos = 0

    for para in paragraphs:
        para_start = text.find(para, pos)
        if para_start == -1:
            para_start = pos

        if len(current) + len(para) + 2 <= max_size:
            if current:
                current += "\n\n" + para
            else:
                current = para
                current_start = para_start
        else:
            if current:
                chunks.append({
                    "text": current,
                    "start": current_start,
                    "end": current_start + len(current),
                })
            current = para
            current_start = para_start

        pos = para_start + len(para)

    if current:
        chunks.append({
            "text": current,
            "start": current_start,
            "end": current_start + len(current),
        })
    return chunks


def main():
    conn = get_conn()
    cur = conn.cursor()

    # Clear existing derived data
    cur.execute("DELETE FROM annotations WHERE audit_id = %s", (AUDIT_ID,))
    cur.execute("DELETE FROM control_evidence_matches WHERE audit_id = %s", (AUDIT_ID,))
    cur.execute("DELETE FROM evaluation_results WHERE audit_id = %s", (AUDIT_ID,))
    cur.execute("DELETE FROM document_chunks WHERE audit_id = %s", (AUDIT_ID,))
    cur.execute("DELETE FROM audit_log WHERE audit_id = %s", (AUDIT_ID,))
    conn.commit()
    print("Cleared existing data")

    # Get controls
    cur.execute("SELECT control_id, control_code, control_title, control_description, risk_level FROM controls WHERE audit_id = %s ORDER BY control_code", (AUDIT_ID,))
    controls = {row[1]: {"id": str(row[0]), "code": row[1], "title": row[2], "desc": row[3], "risk": row[4]} for row in cur.fetchall()}

    # Get evidence documents
    cur.execute("SELECT document_id, original_filename, extracted_text FROM evidence_documents WHERE audit_id = %s", (AUDIT_ID,))
    docs = {row[1]: {"id": str(row[0]), "name": row[1], "text": row[2]} for row in cur.fetchall()}

    print(f"Controls: {len(controls)}, Evidence docs: {len(docs)}")

    # ---- Step 1: Create chunks ----
    chunk_map = {}  # doc_name -> list of chunk records
    for doc_name, doc in docs.items():
        chunks = chunk_text(doc["text"])
        chunk_map[doc_name] = []
        for i, c in enumerate(chunks):
            chunk_id = str(uuid.uuid4())
            cur.execute(
                """INSERT INTO document_chunks
                   (chunk_id, document_id, audit_id, chunk_index, chunk_text, start_char, end_char, token_count)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                (chunk_id, doc["id"], AUDIT_ID, i, c["text"], c["start"], c["end"], len(c["text"].split())),
            )
            chunk_map[doc_name].append({
                "chunk_id": chunk_id,
                "doc_id": doc["id"],
                "text": c["text"],
                "start": c["start"],
                "end": c["end"],
                "index": i,
            })
    conn.commit()
    total_chunks = sum(len(v) for v in chunk_map.values())
    print(f"Created {total_chunks} chunks")

    # ---- Step 2: Define realistic evaluation results ----
    # Map: control_code -> (verdict, confidence, reasoning, evidence_docs, violation_text)
    evaluations = {
        "SOX-AC-001": {
            "verdict": "PASS",
            "confidence": 0.92,
            "reasoning": "The access review report provides strong evidence of role-based access control implementation. The report documents 847 user accounts reviewed across all financial systems with a defined RBAC model. Access provisioning follows a documented approval workflow with manager authorization as evidenced by the helpdesk ticket integration. The 12 exceptions found were all remediated within the review period.",
            "summary": "Quarterly access review confirms RBAC implementation with documented approval workflows.",
            "docs": ["evidence_access_review_report.txt"],
        },
        "SOX-AC-002": {
            "verdict": "PASS",
            "confidence": 0.88,
            "reasoning": "The MFA policy document establishes clear requirements for multi-factor authentication across all financial systems. The policy mandates MFA for all remote access and privileged accounts. Enrollment tracking shows 98.7% compliance rate. However, the 1.3% gap in enrollment (approximately 11 accounts) should be monitored. Overall, the control is operating effectively.",
            "summary": "MFA policy enforced with 98.7% enrollment compliance for remote and privileged access.",
            "docs": ["evidence_mfa_policy.txt"],
        },
        "SOX-AC-003": {
            "verdict": "FAIL",
            "confidence": 0.85,
            "reasoning": "While the access review report shows quarterly reviews are being conducted, there are significant gaps. The report identifies 12 exceptions where users retained access beyond their role requirements. More critically, 3 terminated employees still had active accounts during the review period — accounts for Mark Johnson (terminated Oct 5), Lisa Park (terminated Nov 12), and James Wilson (terminated Dec 1) were not disabled promptly. This represents a control failure in the timely deprovisioning of terminated employee access.",
            "summary": "Access review found 3 terminated employees with active accounts — deprovisioning control failure.",
            "docs": ["evidence_access_review_report.txt"],
            "violations": [
                "Terminated employees with active access: Mark Johnson (terminated Oct 5, access removed Dec 15), Lisa Park (terminated Nov 12, access removed Jan 3), James Wilson (terminated Dec 1, access pending removal)",
                "12 exceptions found where users had access beyond their current role requirements",
            ],
        },
        "SOX-AC-004": {
            "verdict": "FAIL",
            "confidence": 0.79,
            "reasoning": "The access review covers privileged accounts but reveals concerning gaps. The audit logging report shows 47 privileged operations were performed outside of approved change windows. Emergency access procedures exist but the evidence shows 3 instances where emergency access was not time-limited — sessions remained active for over 72 hours. Additionally, the access review found that 2 shared admin accounts are still in use, violating the individual accountability requirement.",
            "summary": "Privileged access violations: shared admin accounts in use and emergency access not time-limited.",
            "docs": ["evidence_access_review_report.txt", "evidence_audit_logging.txt"],
            "violations": [
                "47 privileged operations performed outside approved change windows",
                "Emergency access sessions remained active for over 72 hours in 3 instances",
            ],
        },
        "SOX-CM-001": {
            "verdict": "INSUFFICIENT_EVIDENCE",
            "confidence": 0.45,
            "reasoning": "The available evidence does not include a dedicated change management log or change advisory board records. The audit logging report shows system changes were tracked, but there is no documentation of the formal change management workflow including request, review, approval, and post-implementation verification steps. Additional evidence such as change tickets, CAB meeting minutes, or change management tool exports would be needed to evaluate this control.",
            "summary": "No change management workflow documentation found in available evidence.",
            "docs": ["evidence_audit_logging.txt"],
        },
        "SOX-CM-002": {
            "verdict": "FAIL",
            "confidence": 0.82,
            "reasoning": "The audit logging report reveals a critical segregation of duties violation. Analysis of the logs shows that on January 15, 2026, the same user (sysadmin_ops) both initiated and approved a production deployment to the financial reporting system. This is a direct violation of the change management segregation requirement. Additionally, there is no evidence of separate development and production environments — the logs show direct deployments to production without staging.",
            "summary": "Same user initiated and approved production deployment — segregation of duties violation.",
            "docs": ["evidence_audit_logging.txt"],
            "violations": [
                "Same user (sysadmin_ops) both initiated and approved a production deployment on January 15",
                "Direct deployments to production detected without evidence of staging environment",
            ],
        },
        "SOX-FR-001": {
            "verdict": "INSUFFICIENT_EVIDENCE",
            "confidence": 0.35,
            "reasoning": "No evidence was provided related to the month-end or quarter-end financial close process. The available documents focus on IT controls rather than financial reporting procedures. A financial close checklist, reconciliation documentation, or management sign-off records would be needed to evaluate this control.",
            "summary": "No financial close documentation in evidence set.",
            "docs": [],
        },
        "SOX-FR-002": {
            "verdict": "INSUFFICIENT_EVIDENCE",
            "confidence": 0.30,
            "reasoning": "The evidence set does not contain documentation related to journal entry controls. No journal entry approval records, supporting documentation requirements, or review procedures were found. This control requires accounting-specific evidence that is not present in the current IT-focused evidence collection.",
            "summary": "No journal entry documentation available for review.",
            "docs": [],
        },
        "SOX-VM-001": {
            "verdict": "PASS",
            "confidence": 0.94,
            "reasoning": "The vulnerability scan report provides comprehensive evidence of an effective vulnerability management program. Qualys Guard Enterprise scans were conducted covering both internal and external systems. The scan identified 342 total vulnerabilities with a clear breakdown by severity. Critical vulnerabilities were remediated within the 7-day SLA. The CVSS average dropped from 4.2 to 3.1 compared to the prior scan, showing continuous improvement. Remediation rates of 100% for critical and 95% for high-severity issues demonstrate strong control effectiveness.",
            "summary": "Vulnerability scans conducted quarterly with 100% critical remediation within SLA.",
            "docs": ["evidence_vulnerability_scan_results.txt"],
        },
        "SOX-BC-001": {
            "verdict": "PASS",
            "confidence": 0.75,
            "reasoning": "The network segmentation assessment provides indirect evidence of business continuity planning. The report references defined RTO and RPO objectives for financial systems and confirms DR failover testing was conducted. However, a dedicated BCP document was not provided. The network assessment confirms that backup replication is functioning across availability zones, which supports continuity objectives.",
            "summary": "DR testing confirmed via network assessment; dedicated BCP document not provided.",
            "docs": ["evidence_network_segmentation.txt"],
        },
        "SOX-BC-002": {
            "verdict": "PASS",
            "confidence": 0.86,
            "reasoning": "The encryption and key management procedures document confirms that automated backup procedures include encrypted backups of all critical financial data. Backup integrity verification is documented with monthly restoration tests. The encryption procedures ensure backup data is protected with AES-256 encryption. Cross-region replication is active for disaster recovery purposes.",
            "summary": "Automated encrypted backups with monthly restoration testing confirmed.",
            "docs": ["evidence_encryption_key_mgmt.txt"],
        },
        "SOX-NW-001": {
            "verdict": "PASS",
            "confidence": 0.91,
            "reasoning": "The network segmentation assessment report provides strong evidence of effective network security controls. The third-party assessment by CyberDefense Solutions confirms network segmentation isolates financial system environments into dedicated VLANs. Firewall rules are documented and reviewed. Intrusion detection (Palo Alto Networks) is active on all critical segments with 99.97% uptime. The assessment found only 2 minor findings, both addressed within the reporting period.",
            "summary": "Third-party assessment confirms network segmentation, firewall controls, and IDS coverage.",
            "docs": ["evidence_network_segmentation.txt"],
        },
    }

    # ---- Step 3: Insert evaluation results and matches ----
    for ctrl_code, eval_data in evaluations.items():
        ctrl = controls[ctrl_code]
        eval_id = str(uuid.uuid4())

        # Get matched doc IDs
        matched_doc_ids = [docs[d]["id"] for d in eval_data["docs"] if d in docs]

        cur.execute(
            """INSERT INTO evaluation_results
               (evaluation_id, control_id, audit_id, ai_verdict, ai_confidence,
                ai_reasoning, evidence_summary, matched_document_ids, model_used, prompt_version)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (eval_id, ctrl["id"], AUDIT_ID, eval_data["verdict"], eval_data["confidence"],
             eval_data["reasoning"], eval_data["summary"], matched_doc_ids,
             "databricks-meta-llama-3-3-70b-instruct", "v5.0-pgvector"),
        )

        # Create evidence matches for each relevant document
        for rank, doc_name in enumerate(eval_data["docs"], 1):
            if doc_name not in chunk_map:
                continue
            chunks = chunk_map[doc_name]
            # Match top 3 chunks per document
            for i, chunk in enumerate(chunks[:3]):
                score = max(0.45, 0.92 - (rank - 1) * 0.1 - i * 0.08)
                match_id = str(uuid.uuid4())
                cur.execute(
                    """INSERT INTO control_evidence_matches
                       (match_id, control_id, chunk_id, document_id, audit_id, similarity_score, match_rank)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (match_id, ctrl["id"], chunk["chunk_id"], chunk["doc_id"],
                     AUDIT_ID, score, (rank - 1) * 3 + i + 1),
                )

        # Create annotations for FAIL verdicts
        if eval_data["verdict"] == "FAIL" and "violations" in eval_data:
            for vi, violation_text in enumerate(eval_data.get("violations", [])):
                # Find the violation text in the evidence
                for doc_name in eval_data["docs"]:
                    if doc_name not in docs:
                        continue
                    full_text = docs[doc_name]["text"]
                    # Search for key phrases from the violation
                    search_terms = violation_text.split("(")[0].strip()[:60].lower()
                    for chunk in chunk_map.get(doc_name, []):
                        if any(term in chunk["text"].lower() for term in search_terms.split()[:3]):
                            ann_id = str(uuid.uuid4())
                            cur.execute(
                                """INSERT INTO annotations
                                   (annotation_id, control_id, document_id, chunk_id, audit_id,
                                    start_char, end_char, similarity_score, explanation_text,
                                    control_code, control_title, violation_type)
                                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'VIOLATION')""",
                                (ann_id, ctrl["id"], docs[doc_name]["id"], chunk["chunk_id"],
                                 AUDIT_ID, chunk["start"], chunk["end"],
                                 0.85 - vi * 0.05,
                                 violation_text,
                                 ctrl["code"], ctrl["title"]),
                            )
                            break  # One annotation per violation
                    break  # Use first matching document

    conn.commit()

    # ---- Step 4: Add audit log entries ----
    log_entries = [
        ("admin@firm.com", "CREATE", "AUDIT", AUDIT_ID, "Audit engagement created: Q1 2026 SOX Compliance Review"),
        ("admin@firm.com", "UPLOAD", "CONTROL", "batch", "Uploaded 12 SOX controls from sox_controls.csv"),
        ("admin@firm.com", "UPLOAD", "EVIDENCE", "batch", "Uploaded 6 evidence documents"),
        ("pipeline@system", "PARSE", "EVIDENCE", "batch", "All 6 evidence documents parsed successfully"),
        ("pipeline@system", "EVALUATE", "CONTROL", "batch", "AI evaluation completed for 12 controls: 6 PASS, 3 FAIL, 3 INSUFFICIENT"),
        ("sarah.chen@firm.com", "REVIEW", "EVALUATION", controls["SOX-AC-003"]["id"], '{"verdict": "FAIL", "notes": "Confirmed - terminated employee access is a critical finding"}'),
        ("sarah.chen@firm.com", "REVIEW", "EVALUATION", controls["SOX-CM-002"]["id"], '{"verdict": "FAIL", "notes": "Segregation of duties violation confirmed. Escalating to management."}'),
        ("john.miller@firm.com", "REVIEW", "EVALUATION", controls["SOX-AC-001"]["id"], '{"verdict": "PASS", "notes": "RBAC implementation verified. Minor exceptions noted but remediated."}'),
    ]

    for user, action, etype, eid, details in log_entries:
        cur.execute(
            """INSERT INTO audit_log (audit_id, user_id, action, entity_type, entity_id, details)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (AUDIT_ID, user, action, etype, eid, details),
        )

    # Add auditor reviews for a few results
    cur.execute("""
        UPDATE evaluation_results SET auditor_verdict = 'FAIL', auditor_notes = 'Confirmed - terminated employee access is a critical finding. 3 accounts with delayed deprovisioning.', auditor_id = 'sarah.chen@firm.com', reviewed_at = NOW()
        WHERE audit_id = %s AND control_id = %s
    """, (AUDIT_ID, controls["SOX-AC-003"]["id"]))

    cur.execute("""
        UPDATE evaluation_results SET auditor_verdict = 'FAIL', auditor_notes = 'Segregation of duties violation confirmed. Same person deployed and approved. Escalating to IT management.', auditor_id = 'sarah.chen@firm.com', reviewed_at = NOW()
        WHERE audit_id = %s AND control_id = %s
    """, (AUDIT_ID, controls["SOX-CM-002"]["id"]))

    cur.execute("""
        UPDATE evaluation_results SET auditor_verdict = 'PASS', auditor_notes = 'RBAC verified. 12 exceptions were all remediated within the review period.', auditor_id = 'john.miller@firm.com', reviewed_at = NOW()
        WHERE audit_id = %s AND control_id = %s
    """, (AUDIT_ID, controls["SOX-AC-001"]["id"]))

    # Update audit status to COMPLETED
    cur.execute("UPDATE audit_engagements SET status = 'COMPLETED' WHERE audit_id = %s", (AUDIT_ID,))

    conn.commit()

    # Print summary
    cur.execute("SELECT COUNT(*) FROM document_chunks WHERE audit_id = %s", (AUDIT_ID,))
    print(f"Chunks: {cur.fetchone()[0]}")
    cur.execute("SELECT COUNT(*) FROM evaluation_results WHERE audit_id = %s", (AUDIT_ID,))
    print(f"Evaluation results: {cur.fetchone()[0]}")
    cur.execute("SELECT COUNT(*) FROM control_evidence_matches WHERE audit_id = %s", (AUDIT_ID,))
    print(f"Evidence matches: {cur.fetchone()[0]}")
    cur.execute("SELECT COUNT(*) FROM annotations WHERE audit_id = %s", (AUDIT_ID,))
    print(f"Annotations: {cur.fetchone()[0]}")
    cur.execute("SELECT COUNT(*) FROM audit_log WHERE audit_id = %s", (AUDIT_ID,))
    print(f"Audit log entries: {cur.fetchone()[0]}")

    # Print verdict summary
    cur.execute("""
        SELECT ai_verdict, COUNT(*), ROUND(AVG(ai_confidence)::numeric, 2)
        FROM evaluation_results WHERE audit_id = %s
        GROUP BY ai_verdict ORDER BY ai_verdict
    """, (AUDIT_ID,))
    print("\nVerdict Summary:")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]} controls (avg confidence: {row[2]})")

    conn.close()
    print("\nDone! Full example data seeded.")


if __name__ == "__main__":
    main()
