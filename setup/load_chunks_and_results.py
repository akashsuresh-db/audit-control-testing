"""Load synthetic document chunks, control-evidence matches, evaluation results, and audit log."""
import subprocess, json, tempfile, os, uuid
from datetime import datetime

PROFILE = "fevm-akash-finance-app"
WAREHOUSE = "1b1d59e180e4ac26"
FQ = "main.audit_schema"

def run_sql(stmt, desc=""):
    payload = json.dumps({"warehouse_id": WAREHOUSE, "statement": " ".join(stmt.split()), "wait_timeout": "50s"})
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write(payload)
        tmpfile = f.name
    try:
        result = subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements", "--profile", PROFILE, "--json", f"@{tmpfile}"], capture_output=True, text=True)
        data = json.loads(result.stdout) if result.stdout.strip() else {"status": {"state": "ERROR"}}
        status = data.get("status", {}).get("state", "UNKNOWN")
        error = data.get("status", {}).get("error", {}).get("message", "")
        if status == "SUCCEEDED":
            print(f"  OK [{desc}]")
        else:
            print(f"  FAIL [{desc}]: {error[:200]}")
        return data
    finally:
        os.unlink(tmpfile)

def esc(s):
    return s.replace("'", "''") if s else ""

# ============================================================
# Document Chunks (simplified - 2-3 chunks per document)
# ============================================================
print("=" * 60)
print("Loading document chunks...")
print("=" * 60)

chunks = [
    # DOC-001 chunks
    ("CHK-001-1", "DOC-001", "AUD-2026-001", 0, "Acme Corporation - User Access Provisioning Policy v3.2 - Effective Date: January 1, 2026. PURPOSE: This policy establishes the procedures for provisioning, modifying, and deprovisioning user access to Acme Corporation information systems. SCOPE: All employees, contractors, and third-party users requiring access to Acme IT systems. PROVISIONING PROCESS: All access requests must be submitted through the ServiceNow ITSM portal using the Access Request Form ARF-100.", 95),
    ("CHK-001-2", "DOC-001", "AUD-2026-001", 1, "Each request must include: employee name, department, manager name, requested systems, business justification, and requested access level. The direct manager must approve the request electronically within ServiceNow. IT Security reviews the request against the Role-Based Access Control RBAC matrix to ensure least privilege. Upon approval, IT Operations provisions access within 2 business days. LEAST PRIVILEGE: Access is granted based on the minimum permissions required to perform job duties.", 89),
    ("CHK-001-3", "DOC-001", "AUD-2026-001", 2, "The RBAC matrix is maintained in SharePoint and updated quarterly by IT Security. PRIVILEGED ACCESS: Privileged accounts require additional approval from the CISO and are reviewed monthly. AUDIT TRAIL: All provisioning actions are logged in ServiceNow with timestamps and approver details.", 52),
    # DOC-002 chunks
    ("CHK-002-1", "DOC-002", "AUD-2026-001", 0, "Access Request Evidence Package - Q1 2026. Sample 1: Request ARF-2026-0142 - Employee: Maria Garcia, Department: Accounting, Manager: Robert Chen approved 01/15/2026. Systems requested: SAP ERP GL module read-only, Oracle EPM reporting. Business justification: New hire in accounts payable team. RBAC matrix check: Verified by IT Security Jane Wu on 01/16/2026. Provisioned: 01/17/2026 by ops-admin.", 88),
    ("CHK-002-2", "DOC-002", "AUD-2026-001", 1, "Sample 2: Request ARF-2026-0198 - Employee: James Lee, Department: IT Development, Manager: Susan Park approved 02/03/2026. Systems requested: GitHub Enterprise developer role, JIRA project contributor, AWS Dev account restricted. Business justification: Transfer from QA to Development team. RBAC check: Verified 02/04/2026. Note: Production access NOT granted per SoD policy. Provisioned: 02/05/2026.", 85),
    ("CHK-002-3", "DOC-002", "AUD-2026-001", 2, "Total requests in Q1 2026: 342. All requests had manager approval documented. 100 percent RBAC matrix verification rate. Sample 3 shows Priya Patel promoted to Senior Accountant with access upgraded from read-only to write after proper RBAC verification on 02/21/2026.", 50),
    # DOC-003 chunks
    ("CHK-003-1", "DOC-003", "AUD-2026-001", 0, "Employee Termination Access Revocation Evidence - Q1 2026. Process: HR sends termination notification via automated workflow to IT Security within 1 hour of termination decision. IT Security disables Active Directory account immediately. All application-specific access is revoked within 24 hours.", 48),
    ("CHK-003-2", "DOC-003", "AUD-2026-001", 1, "Sample 1: Employee: Tom Richards, Termination Date: 01/22/2026. HR notification: 01/22/2026 09:15 AM. AD disabled: 09:32 AM. VPN revoked: 09:35 AM. Email disabled: 09:40 AM. Badge deactivated: 10:00 AM. SAP access removed: 11:15 AM. Checklist completed: 11:30 AM. Summary: 47 terminations in Q1 2026. Average time to full revocation: 3.2 hours. 100 percent completed within 24-hour SLA. Zero exceptions.", 82),
    # DOC-004 chunks
    ("CHK-004-1", "DOC-004", "AUD-2026-001", 0, "Quarterly User Access Review Report - Q4 2025. Performed by: Jane Doe, IT Security Manager. Applications Reviewed: SAP ERP, Oracle EPM, Workday HCM, Concur, SharePoint Financial Sites. Methodology: User listings extracted from each application on 12/15/2025. Lists compared against active employee roster from HR. Each manager reviewed their team members access and confirmed appropriateness.", 67),
    ("CHK-004-2", "DOC-004", "AUD-2026-001", 1, "Findings: Total users reviewed: 1847. Appropriate access confirmed: 1823 98.7 percent. Access revoked no longer needed: 18 users. Access modified excessive permissions: 6 users. Remediation: All 24 findings remediated within 10 business days. Review sign-off: Jane Doe IT Security 12/28/2025, John Smith VP IT 12/29/2025, CFO Robert Miller 12/30/2025.", 60),
    # DOC-005 chunks
    ("CHK-005-1", "DOC-005", "AUD-2026-001", 0, "Acme Corporation - Change Management Policy CM-POL-001 v4.0. All changes to production systems must follow the formal change management process. Developer submits change request in ServiceNow with description, business justification, risk assessment, rollback plan, and test results. Technical review by lead developer or architect. Change Advisory Board CAB meets weekly to review and approve standard changes.", 70),
    ("CHK-005-2", "DOC-005", "AUD-2026-001", 1, "Deployment: Separate operations team deploys approved changes using automated CI/CD pipeline. Developers do NOT have production deployment access. Emergency Changes: Emergency changes may bypass CAB but require CTO approval and must be formally documented within 48 hours. Post-Implementation Review: All changes are reviewed within 5 business days to confirm successful implementation.", 64),
    # DOC-006 chunks
    ("CHK-006-1", "DOC-006", "AUD-2026-001", 0, "Change Advisory Board Meeting Minutes - January 2026. 01/08 Meeting: 12 changes reviewed. 10 approved, 1 deferred insufficient testing, 1 rejected no business justification. CHG-2026-0012: SAP patch SP15 - Approved. Risk: Medium. Testing: UAT completed. CHG-2026-0015: New GL reporting module - Approved. Risk: High. Additional approval from CFO obtained.", 68),
    ("CHK-006-2", "DOC-006", "AUD-2026-001", 1, "01/22 Meeting: 15 changes reviewed. 14 approved, 1 emergency change ratified CHG-2026-0089 security patch for Log4j variant. Emergency change had CTO approval email evidence attached. Post-implementation review completed 01/25. Summary: 44 total changes in January. 42 standard approvals, 1 emergency, 1 rejection. 0 unauthorized changes detected.", 60),
    # DOC-007 chunks
    ("CHK-007-1", "DOC-007", "AUD-2026-001", 0, "Production Deployment Access Matrix - As of February 2026. Production deployment access is restricted to the DevOps Operations team only. Users with production deployment access: ops-deploy-1 Sarah Connor, ops-deploy-2 Mark Johnson, ops-deploy-3 Rita Patel. CI/CD Service Account: svc-cicd-prod automated deployments only. Developer team members verified to NOT have production access: 45 developers checked against AD groups. No developer accounts found in prod-deploy, prod-admin, or prod-operations AD groups.", 85),
    # DOC-008 chunks
    ("CHK-008-1", "DOC-008", "AUD-2026-001", 0, "Backup Monitoring Report - January 2026. Backup Schedule: Daily incremental at 2:00 AM EST. Weekly full backup every Sunday. Systems covered: SAP ERP database, Oracle EPM, File servers, SharePoint, Exchange Online. January Results: 31 daily incremental backups: 31 successful 100 percent. 4 weekly full backups: 4 successful. Backup verification: Random file restores performed on 01/12 and 01/26. Both successful. Backup storage: AWS S3 with cross-region replication. Encryption: AES-256.", 88),
    # DOC-009 chunks
    ("CHK-009-1", "DOC-009", "AUD-2026-001", 0, "SIEM Dashboard Screenshot - Splunk Enterprise Security. Dashboard shows: Active alerts: 3 two low one medium. Alerts triaged in last 24h: 47. Average triage time: 8 minutes. SLA compliance 15-min triage: 99.2 percent. Incidents opened this month: 12. Incidents resolved: 11. Log sources: 156 active. Failed log sources: 0. Events per second: 12847.", 65),
    # DOC-010 chunks
    ("CHK-010-1", "DOC-010", "AUD-2026-001", 0, "Monthly Financial Reconciliation Report - February 2026. Bank Reconciliation: 5 bank accounts reconciled. Total balance: 47283912.45. All reconciling items identified and resolved. Intercompany Reconciliation: 8 intercompany accounts balanced. Net intercompany position: zero. Subledger to GL Reconciliation: AP subledger Balanced. AR subledger Balanced.", 65),
    ("CHK-010-2", "DOC-010", "AUD-2026-001", 1, "FA subledger: 2100 variance identified traced to timing difference in asset disposal entry. Correcting entry JE-2026-0892 posted. All reconciliations completed within 5 business days of month-end. Reviewed and approved by Alice Green Controller 03/05/2026. Second review by Robert Miller CFO 03/05/2026.", 52),
    # DOC-011 chunks
    ("CHK-011-1", "DOC-011", "AUD-2026-001", 0, "Manual Journal Entry Log - February 2026. Total manual journal entries: 89. Entries above 10000 threshold: 23. JE-2026-0845: Amount 125000. Accrual for consulting services. Prepared by: Mark Thompson. Approved by: Alice Green Controller. Second approval: Robert Miller CFO. Supporting docs: Invoice attached.", 58),
    ("CHK-011-2", "DOC-011", "AUD-2026-001", 1, "All 23 entries above threshold had dual authorization documented. No exceptions noted. Monthly review by CFO completed 03/05/2026 sign-off on file. JE-2026-0892 Amount 2100 FA disposal correction per reconciliation finding also had dual approval.", 42),
    # DOC-012 chunks
    ("CHK-012-1", "DOC-012", "AUD-2026-002", 0, "GlobalBank Financial - Network Security Architecture Document v2.1. Perimeter Security: Next-gen firewalls Palo Alto PA-5260 deployed at all ingress/egress points. IDS/IPS Suricata monitors all traffic. DDoS protection via Cloudflare. Network Segmentation: Production, development, and corporate networks fully segmented via VLANs and firewall rules. DMZ isolates public-facing services.", 68),
    ("CHK-012-2", "DOC-012", "AUD-2026-002", 1, "Encryption: TLS 1.3 enforced for all external connections. Internal service mesh uses mTLS. Data at rest encrypted with AES-256 AWS KMS managed keys. Key rotation every 90 days. Authentication: Multi-factor authentication required for all users Okta with FIDO2. SSO integrated with all applications. Monitoring: All network flows logged to Splunk SIEM. 24/7 SOC with 15-minute SLA.", 72),
    # DOC-013 chunks
    ("CHK-013-1", "DOC-013", "AUD-2026-002", 0, "Multi-Factor Authentication Enrollment Report - February 2026. Total active users: 2341. MFA enrolled: 2341 100 percent. MFA methods: FIDO2 security keys: 1890 primary. Okta Verify push: 2200 secondary. SMS: 0 disabled per policy. Remote access users: 567. MFA enforcement: 100 percent. VPN connections without MFA: 0. Failed MFA attempts: 234. Locked accounts: 12. All investigated.", 72),
    # DOC-014 chunks
    ("CHK-014-1", "DOC-014", "AUD-2026-002", 0, "GlobalBank Financial - Incident Response Plan IRP-2026 v3.0. Detection: 24/7 SOC monitors SIEM alerts. Classification: P1 Critical data breach ransomware. P2 High unauthorized access malware. P3 Medium phishing policy violations. Escalation: P1 within 15 minutes to CISO. P2 within 1 hour. P3 within 4 hours.", 58),
    ("CHK-014-2", "DOC-014", "AUD-2026-002", 1, "Testing: Annual tabletop exercise. Last test: November 15, 2025. Participants: 18 staff across 6 departments. Test scenario: Ransomware attack on payment processing system. Results: RTO met 4 hours vs 6-hour target. 2 gaps identified and remediated by 12/15/2025. Post-Incident: Root cause analysis within 5 business days. Lessons learned documented.", 60),
    # DOC-015 chunks
    ("CHK-015-1", "DOC-015", "AUD-2026-002", 0, "Security Operations Center Monthly Metrics - January 2026. Alert Volume: Total alerts: 15234. True positives: 1847. Triage Performance: Average time to triage: 7.3 minutes. SLA compliance 15 min: 99.6 percent. Incidents: New: 23. Resolved: 21. Mean time to resolve: P1: 2.1 hours. P2: 6.4 hours. P3: 18.2 hours. All incidents documented in ServiceNow with root cause analysis.", 68),
    # DOC-016 chunks
    ("CHK-016-1", "DOC-016", "AUD-2026-002", 0, "Infrastructure Capacity Planning Report - Q1 2026. Current utilization: CPU 45 percent average 78 percent peak. Memory 62 percent average. Storage 71 percent used of 500TB. Auto-scaling configuration: Application tier scales from 10 to 50 instances based on CPU above 70 percent. Database tier: Aurora Serverless with auto-scaling. Forecast: current capacity sufficient through Q3 2026.", 65),
]

for chunk in chunks:
    chunk_id, doc_id, audit_id, idx, text, tokens = chunk
    text_esc = esc(text)
    sql = (
        f"INSERT INTO {FQ}.document_chunks VALUES ("
        f"'{chunk_id}', '{doc_id}', '{audit_id}', {idx}, '{text_esc}', {tokens}, "
        f"ARRAY({idx + 1}, {idx + 2}), NULL, '2026-03-01T10:00:00')"
    )
    run_sql(sql, chunk_id)

# ============================================================
# Control-Evidence Matches
# ============================================================
print("\n" + "=" * 60)
print("Loading control-evidence matches...")
print("=" * 60)

matches = [
    # CTL-001 User Access Provisioning -> DOC-001, DOC-002
    ("MTH-001", "CTL-001", "CHK-001-1", "DOC-001", "AUD-2026-001", 0.94, 1),
    ("MTH-002", "CTL-001", "CHK-001-2", "DOC-001", "AUD-2026-001", 0.91, 2),
    ("MTH-003", "CTL-001", "CHK-002-1", "DOC-002", "AUD-2026-001", 0.89, 3),
    ("MTH-004", "CTL-001", "CHK-002-2", "DOC-002", "AUD-2026-001", 0.85, 4),
    # CTL-002 Access Termination -> DOC-003
    ("MTH-005", "CTL-002", "CHK-003-1", "DOC-003", "AUD-2026-001", 0.93, 1),
    ("MTH-006", "CTL-002", "CHK-003-2", "DOC-003", "AUD-2026-001", 0.90, 2),
    # CTL-003 Periodic Access Reviews -> DOC-004
    ("MTH-007", "CTL-003", "CHK-004-1", "DOC-004", "AUD-2026-001", 0.95, 1),
    ("MTH-008", "CTL-003", "CHK-004-2", "DOC-004", "AUD-2026-001", 0.92, 2),
    # CTL-004 Change Management -> DOC-005, DOC-006
    ("MTH-009", "CTL-004", "CHK-005-1", "DOC-005", "AUD-2026-001", 0.93, 1),
    ("MTH-010", "CTL-004", "CHK-006-1", "DOC-006", "AUD-2026-001", 0.88, 2),
    ("MTH-011", "CTL-004", "CHK-006-2", "DOC-006", "AUD-2026-001", 0.85, 3),
    # CTL-005 SoD in Development -> DOC-007, DOC-005
    ("MTH-012", "CTL-005", "CHK-007-1", "DOC-007", "AUD-2026-001", 0.96, 1),
    ("MTH-013", "CTL-005", "CHK-005-2", "DOC-005", "AUD-2026-001", 0.87, 2),
    # CTL-006 Backup and Recovery -> DOC-008
    ("MTH-014", "CTL-006", "CHK-008-1", "DOC-008", "AUD-2026-001", 0.94, 1),
    # CTL-007 DR Testing -> DOC-008 (weak match - DR test not done yet)
    ("MTH-015", "CTL-007", "CHK-008-1", "DOC-008", "AUD-2026-001", 0.62, 1),
    # CTL-008 Security Monitoring -> DOC-009
    ("MTH-016", "CTL-008", "CHK-009-1", "DOC-009", "AUD-2026-001", 0.91, 1),
    # CTL-009 Financial Reconciliation -> DOC-010
    ("MTH-017", "CTL-009", "CHK-010-1", "DOC-010", "AUD-2026-001", 0.95, 1),
    ("MTH-018", "CTL-009", "CHK-010-2", "DOC-010", "AUD-2026-001", 0.88, 2),
    # CTL-010 Journal Entry Auth -> DOC-011
    ("MTH-019", "CTL-010", "CHK-011-1", "DOC-011", "AUD-2026-001", 0.93, 1),
    ("MTH-020", "CTL-010", "CHK-011-2", "DOC-011", "AUD-2026-001", 0.90, 2),
    # SOC2 matches
    ("MTH-021", "CTL-011", "CHK-012-1", "DOC-012", "AUD-2026-002", 0.94, 1),
    ("MTH-022", "CTL-011", "CHK-012-2", "DOC-012", "AUD-2026-002", 0.91, 2),
    ("MTH-023", "CTL-012", "CHK-013-1", "DOC-013", "AUD-2026-002", 0.95, 1),
    ("MTH-024", "CTL-012", "CHK-012-2", "DOC-012", "AUD-2026-002", 0.82, 2),
    ("MTH-025", "CTL-013", "CHK-015-1", "DOC-015", "AUD-2026-002", 0.93, 1),
    ("MTH-026", "CTL-014", "CHK-014-1", "DOC-014", "AUD-2026-002", 0.94, 1),
    ("MTH-027", "CTL-014", "CHK-014-2", "DOC-014", "AUD-2026-002", 0.91, 2),
    ("MTH-028", "CTL-015", "CHK-005-1", "DOC-005", "AUD-2026-002", 0.72, 1),
    ("MTH-029", "CTL-016", "CHK-016-1", "DOC-016", "AUD-2026-002", 0.92, 1),
    ("MTH-030", "CTL-017", "CHK-008-1", "DOC-008", "AUD-2026-002", 0.78, 1),
    ("MTH-031", "CTL-018", "CHK-014-1", "DOC-014", "AUD-2026-002", 0.68, 1),
]

# Insert matches in a single batch
values = []
for m in matches:
    values.append(f"('{m[0]}', '{m[1]}', '{m[2]}', '{m[3]}', '{m[4]}', {m[5]}, {m[6]}, '2026-03-01T12:00:00')")
sql = f"INSERT INTO {FQ}.control_evidence_matches VALUES " + ", ".join(values)
run_sql(sql, "all matches")

# ============================================================
# Evaluation Results
# ============================================================
print("\n" + "=" * 60)
print("Loading evaluation results...")
print("=" * 60)

evals = [
    ("EVAL-001", "CTL-001", "AUD-2026-001", "PASS", 0.92,
     "The evidence strongly supports that user access provisioning follows a documented, multi-step approval process. The provisioning policy (DOC-001) clearly defines the process requiring manager approval and RBAC matrix verification. The sample access requests (DOC-002) demonstrate consistent adherence with 342 requests in Q1 2026 all having documented manager approval and 100% RBAC verification rate.",
     "Policy defines formal provisioning via ServiceNow with manager approval and RBAC checks. Q1 2026 samples show all 342 requests followed the process.",
     "ARRAY('DOC-001', 'DOC-002')", "ARRAY('CHK-001-1', 'CHK-001-2', 'CHK-002-1', 'CHK-002-2')",
     "PASS", "Concur with AI assessment. Evidence is comprehensive.", "auditor1@firm.com"),
    ("EVAL-002", "CTL-002", "AUD-2026-001", "PASS", 0.91,
     "Evidence demonstrates a robust termination access revocation process. Automated HR workflow triggers IT Security within 1 hour. Sample terminations show AD accounts disabled within minutes and full access revocation completed well within the 24-hour SLA. All 47 Q1 terminations achieved 100% SLA compliance with zero exceptions.",
     "Automated termination workflow with rapid revocation. 47 terminations in Q1 all within 24-hour SLA.",
     "ARRAY('DOC-003')", "ARRAY('CHK-003-1', 'CHK-003-2')",
     "PASS", "Strong evidence with detailed timestamps.", "auditor1@firm.com"),
    ("EVAL-003", "CTL-003", "AUD-2026-001", "PASS", 0.89,
     "The quarterly access review for Q4 2025 was conducted by IT Security Manager Jane Doe covering all critical financial applications. 1,847 users were reviewed with 98.7% confirmed appropriate. 24 findings were remediated within 10 business days. Review was signed off by IT Security, VP IT, and CFO. However, Q1 2026 review evidence is not yet available.",
     "Q4 2025 review covered 1,847 users across 5 critical apps. 24 findings remediated. Triple sign-off obtained.",
     "ARRAY('DOC-004')", "ARRAY('CHK-004-1', 'CHK-004-2')",
     "PASS", "Q4 review is solid. Will need Q1 2026 review when available.", "auditor1@firm.com"),
    ("EVAL-004", "CTL-004", "AUD-2026-001", "PASS", 0.90,
     "Change management process is well-documented in policy CM-POL-001 requiring ServiceNow change requests with business justification, risk assessment, and testing evidence. CAB meeting minutes from January 2026 show 44 changes reviewed with proper approval workflow. One emergency change was handled per policy with CTO approval and post-implementation review.",
     "Formal CM policy in place. CAB reviewed 44 changes in Jan 2026. Emergency change followed expedited process with CTO approval.",
     "ARRAY('DOC-005', 'DOC-006')", "ARRAY('CHK-005-1', 'CHK-006-1', 'CHK-006-2')",
     "PASS", "Good evidence of operating effectiveness.", "auditor1@firm.com"),
    ("EVAL-005", "CTL-005", "AUD-2026-001", "PASS", 0.95,
     "Strong evidence of segregation of duties. The deployment access matrix explicitly shows only 3 operations staff and 1 CI/CD service account have production deployment access. All 45 developers were verified against AD groups and confirmed to have no production access. The change management policy also states developers do NOT have production deployment access.",
     "Only 3 ops staff and 1 service account have prod access. 45 developers confirmed excluded from all production AD groups.",
     "ARRAY('DOC-007', 'DOC-005')", "ARRAY('CHK-007-1', 'CHK-005-2')",
     None, None, None),
    ("EVAL-006", "CTL-006", "AUD-2026-001", "PASS", 0.88,
     "Backup monitoring report shows 100% success rate for both daily incremental and weekly full backups in January 2026. Random restoration tests were successful. Backups are encrypted (AES-256) and replicated cross-region. Failed backup alerts configured via PagerDuty. However, the quarterly restoration test is scheduled for March 2026 and has not yet been completed.",
     "100% backup success rate in Jan 2026. Cross-region replication with AES-256 encryption. Quarterly restore test pending.",
     "ARRAY('DOC-008')", "ARRAY('CHK-008-1')",
     "PASS", "Backup operations are solid. Will verify quarterly restore test completion.", "auditor1@firm.com"),
    ("EVAL-007", "CTL-007", "AUD-2026-001", "INSUFFICIENT_EVIDENCE", 0.45,
     "The backup report mentions that the last full DR test was September 2025 and the next is scheduled for March 2026. While this confirms DR testing is planned, there is no evidence of a DR test being completed within the audit period. The September 2025 test predates the audit period. The March 2026 test has not yet occurred.",
     "Last DR test was Sep 2025 (pre-audit period). Next scheduled for Mar 2026 but not yet completed.",
     "ARRAY('DOC-008')", "ARRAY('CHK-008-1')",
     "INSUFFICIENT_EVIDENCE", "Agree - need to follow up after March DR test is completed.", "auditor1@firm.com"),
    ("EVAL-008", "CTL-008", "AUD-2026-001", "PASS", 0.86,
     "SIEM dashboard screenshot shows active monitoring with Splunk Enterprise Security. Triage SLA compliance is 99.2% against a 15-minute target. 156 log sources are active with zero failures. However, this is a point-in-time screenshot rather than a trend report. Additional evidence such as monthly SOC metrics reports would strengthen this assessment.",
     "Splunk SIEM active with 99.2% triage SLA compliance. 156 log sources, zero failures. Point-in-time snapshot only.",
     "ARRAY('DOC-009')", "ARRAY('CHK-009-1')",
     "PASS", "Screenshot is supportive but will request monthly trend report.", "auditor1@firm.com"),
    ("EVAL-009", "CTL-009", "AUD-2026-001", "PASS", 0.93,
     "Monthly reconciliation for February 2026 demonstrates comprehensive coverage: 5 bank accounts, 8 intercompany accounts, and all subledgers reconciled. A variance of $2,100 was identified in the FA subledger, properly investigated, and corrected. All reconciliations completed within the 5 business day target. Dual review by Controller and CFO documented.",
     "Full reconciliation of bank, intercompany, and subledger accounts. Variance found and corrected. Dual review by Controller and CFO.",
     "ARRAY('DOC-010')", "ARRAY('CHK-010-1', 'CHK-010-2')",
     "PASS", "Excellent reconciliation evidence with proper variance resolution.", "auditor1@firm.com"),
    ("EVAL-010", "CTL-010", "AUD-2026-001", "PASS", 0.91,
     "Journal entry log shows all 23 manual entries above $10,000 had dual authorization from both the Controller and CFO. Specific examples include JE-2026-0845 ($125,000 consulting accrual) and JE-2026-0867 ($45,200 insurance reclassification) both with supporting documentation attached. Monthly CFO review completed with sign-off on file.",
     "All 23 entries above threshold had dual Controller/CFO approval. Supporting docs attached. Monthly CFO review signed.",
     "ARRAY('DOC-011')", "ARRAY('CHK-011-1', 'CHK-011-2')",
     "PASS", "Strong journal entry controls with proper authorization.", "auditor1@firm.com"),
    # SOC2 evaluations
    ("EVAL-011", "CTL-011", "AUD-2026-002", "PASS", 0.93,
     "Network security architecture document comprehensively covers logical access security: next-gen firewalls, IDS/IPS, network segmentation, TLS 1.3 encryption, mTLS for internal services, AES-256 at rest encryption, MFA via Okta/FIDO2, and 24/7 SOC monitoring. Infrastructure appears well-designed with defense-in-depth approach.",
     "Comprehensive security architecture with firewalls, IDS, encryption (TLS 1.3, AES-256), MFA, and 24/7 SOC.",
     "ARRAY('DOC-012')", "ARRAY('CHK-012-1', 'CHK-012-2')",
     None, None, None),
    ("EVAL-012", "CTL-012", "AUD-2026-002", "PASS", 0.94,
     "MFA enrollment report shows 100% enrollment across all 2,341 active users. FIDO2 security keys are the primary method with Okta Verify as secondary. SMS authentication is disabled per policy. All 567 remote access users have MFA enforced with zero VPN connections without MFA. Failed attempts are investigated with suspicious cases escalated to SOC.",
     "100% MFA enrollment. FIDO2 primary, Okta Verify secondary. SMS disabled. Zero unenforced remote connections.",
     "ARRAY('DOC-013', 'DOC-012')", "ARRAY('CHK-013-1', 'CHK-012-2')",
     "PASS", "Excellent MFA coverage and enforcement.", "auditor2@firm.com"),
    ("EVAL-013", "CTL-013", "AUD-2026-002", "PASS", 0.90,
     "SOC operations metrics show effective incident detection and monitoring. Average triage time of 7.3 minutes exceeds the 15-minute SLA. 99.6% SLA compliance. 23 incidents handled in January with clear categorization. All incidents documented with root cause analysis. Monthly CISO review conducted.",
     "SOC triage at 7.3 min avg (15 min SLA). 99.6% compliance. 23 incidents handled with RCA documentation.",
     "ARRAY('DOC-015')", "ARRAY('CHK-015-1')",
     None, None, None),
    ("EVAL-014", "CTL-014", "AUD-2026-002", "PASS", 0.88,
     "Incident response plan is comprehensive with clear classification (P1-P3), escalation timelines, and communication protocols. Annual tabletop exercise conducted November 2025 with 18 participants. RTO targets met during test. Two gaps identified and remediated within 30 days. Post-incident review process documented.",
     "Comprehensive IRP with annual testing. Nov 2025 tabletop met RTO targets. 2 gaps found and fixed.",
     "ARRAY('DOC-014')", "ARRAY('CHK-014-1', 'CHK-014-2')",
     "PASS", "IR plan and testing evidence are satisfactory.", "auditor2@firm.com"),
    ("EVAL-015", "CTL-015", "AUD-2026-002", "FAIL", 0.55,
     "The only evidence provided for SOC2 change management is a cross-reference to Acme Corps change management policy (DOC-005), which belongs to a different audit engagement (AUD-2026-001). No GlobalBank-specific change management evidence was provided. The similarity score is low (0.72) suggesting weak relevance. GlobalBank needs to provide their own CM policy, change logs, and approval evidence.",
     "No GlobalBank-specific CM evidence provided. Only cross-matched to different organizations policy.",
     "ARRAY('DOC-005')", "ARRAY('CHK-005-1')",
     "INSUFFICIENT_EVIDENCE", "Changed to INSUFFICIENT - the cross-org match is not applicable. Need GlobalBank CM docs.", "auditor2@firm.com"),
    ("EVAL-016", "CTL-016", "AUD-2026-002", "PASS", 0.87,
     "Capacity planning report shows current infrastructure utilization is well within limits with auto-scaling configured. CPU peaks at 78%, memory at 62%, storage at 71%. Auto-scaling from 10-50 instances for app tier. Quarterly review process with CTO presentation. Forecast shows capacity sufficient through Q3 2026.",
     "Utilization within limits. Auto-scaling configured. Quarterly planning with CTO review. Capacity sufficient through Q3.",
     "ARRAY('DOC-016')", "ARRAY('CHK-016-1')",
     None, None, None),
    ("EVAL-017", "CTL-017", "AUD-2026-002", "INSUFFICIENT_EVIDENCE", 0.42,
     "The matched evidence (DOC-008) is from a different organization (Acme Corp, AUD-2026-001) and describes their backup procedures. No GlobalBank-specific backup and recovery evidence was provided. The similarity score is weak (0.78) and the evidence is not applicable to GlobalBanks SOC2 engagement.",
     "No GlobalBank backup evidence provided. Cross-org match not applicable.",
     "ARRAY('DOC-008')", "ARRAY('CHK-008-1')",
     "INSUFFICIENT_EVIDENCE", "Correct - need GlobalBank backup evidence.", "auditor2@firm.com"),
    ("EVAL-018", "CTL-018", "AUD-2026-002", "INSUFFICIENT_EVIDENCE", 0.38,
     "The incident response plan mentions risk assessment in passing but does not constitute dedicated risk assessment evidence. No formal risk register, risk assessment methodology, or annual risk assessment report was provided. The matched evidence has a low similarity score (0.68) and only tangentially relates to risk management.",
     "No formal risk assessment or risk register provided. IR plan only tangentially mentions risk.",
     "ARRAY('DOC-014')", "ARRAY('CHK-014-1')",
     "INSUFFICIENT_EVIDENCE", "Need dedicated risk assessment document and risk register.", "auditor2@firm.com"),
]

for ev in evals:
    eid, cid, aid, verdict, conf, reasoning, summary, doc_ids, chunk_ids, aud_verdict, aud_notes, aud_id = ev
    reasoning_esc = esc(reasoning)
    summary_esc = esc(summary)
    aud_v = f"'{aud_verdict}'" if aud_verdict else "NULL"
    aud_n = f"'{esc(aud_notes)}'" if aud_notes else "NULL"
    aud_i = f"'{aud_id}'" if aud_id else "NULL"
    rev_at = "'2026-03-02T14:00:00'" if aud_verdict else "NULL"

    sql = (
        f"INSERT INTO {FQ}.evaluation_results VALUES ("
        f"'{eid}', '{cid}', '{aid}', '{verdict}', {conf}, "
        f"'{reasoning_esc}', '{summary_esc}', "
        f"{doc_ids}, {chunk_ids}, "
        f"{aud_v}, {aud_n}, {aud_i}, {rev_at}, "
        f"'databricks-meta-llama-3-3-70b-instruct', 'v2.1', '2026-03-01T15:00:00', '2026-03-01T15:00:00')"
    )
    run_sql(sql, eid)

# ============================================================
# Audit Log entries
# ============================================================
print("\n" + "=" * 60)
print("Loading audit log...")
print("=" * 60)

log_sql = f"""
INSERT INTO {FQ}.audit_log VALUES
('LOG-001', 'AUD-2026-001', 'auditor1@firm.com', 'UPLOAD', 'CONTROL', 'CTL-001', '{{"file": "sox_controls_2026.csv", "count": 10}}', '2026-01-20T10:00:00'),
('LOG-002', 'AUD-2026-001', 'auditor1@firm.com', 'UPLOAD', 'EVIDENCE', 'DOC-001', '{{"file": "user_provisioning_policy.pdf", "size": 245000}}', '2026-02-01T10:00:00'),
('LOG-003', 'AUD-2026-001', 'system', 'PARSE', 'EVIDENCE', 'DOC-001', '{{"status": "PARSED", "pages": 12}}', '2026-02-01T10:05:00'),
('LOG-004', 'AUD-2026-001', 'system', 'EVALUATE', 'CONTROL', 'CTL-001', '{{"verdict": "PASS", "confidence": 0.92, "model": "llama-3.3-70b"}}', '2026-03-01T15:00:00'),
('LOG-005', 'AUD-2026-001', 'auditor1@firm.com', 'REVIEW', 'EVALUATION', 'EVAL-001', '{{"ai_verdict": "PASS", "auditor_verdict": "PASS"}}', '2026-03-02T14:00:00'),
('LOG-006', 'AUD-2026-002', 'auditor2@firm.com', 'UPLOAD', 'CONTROL', 'CTL-011', '{{"file": "soc2_controls.csv", "count": 8}}', '2026-02-05T10:00:00'),
('LOG-007', 'AUD-2026-002', 'auditor2@firm.com', 'REVIEW', 'EVALUATION', 'EVAL-015', '{{"ai_verdict": "FAIL", "auditor_verdict": "INSUFFICIENT_EVIDENCE", "override": true}}', '2026-03-02T14:30:00'),
('LOG-008', 'AUD-2026-001', 'auditor1@firm.com', 'UPLOAD', 'EVIDENCE', 'DOC-005', '{{"file": "change_management_policy.pdf"}}', '2026-02-01T10:15:00'),
('LOG-009', 'AUD-2026-001', 'system', 'EVALUATE', 'CONTROL', 'CTL-007', '{{"verdict": "INSUFFICIENT_EVIDENCE", "confidence": 0.45}}', '2026-03-01T15:10:00'),
('LOG-010', 'AUD-2026-001', 'auditor1@firm.com', 'REVIEW', 'EVALUATION', 'EVAL-007', '{{"ai_verdict": "INSUFFICIENT_EVIDENCE", "auditor_verdict": "INSUFFICIENT_EVIDENCE"}}', '2026-03-02T14:15:00')
"""
run_sql(log_sql, "audit_log entries")

# ============================================================
# Verify all data
# ============================================================
print("\n" + "=" * 60)
print("Verifying data counts...")
print("=" * 60)

for table in ["audit_engagements", "controls", "evidence_documents", "document_chunks",
              "control_evidence_matches", "evaluation_results", "audit_log"]:
    r = run_sql(f"SELECT COUNT(*) FROM {FQ}.{table}", f"count:{table}")
    count = r.get("result", {}).get("data_array", [[0]])[0][0]
    print(f"    {table}: {count} rows")

print("\nAll synthetic data loaded!")
