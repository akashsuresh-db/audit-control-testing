"""Load synthetic evidence documents into main.audit_schema.evidence_documents"""
import subprocess, json, tempfile, os

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

# Evidence documents - insert one at a time to avoid SQL length limits
docs = [
    {
        "id": "DOC-001", "audit": "AUD-2026-001", "name": "user_provisioning_policy.pdf", "type": "pdf", "size": 245000, "pages": 12, "ocr": False,
        "text": "Acme Corporation - User Access Provisioning Policy v3.2 - Effective Date: January 1, 2026. 1. PURPOSE: This policy establishes the procedures for provisioning, modifying, and deprovisioning user access to Acme Corporation information systems. 2. SCOPE: All employees, contractors, and third-party users requiring access to Acme IT systems. 3. PROVISIONING PROCESS: 3.1 All access requests must be submitted through the ServiceNow ITSM portal using the Access Request Form (ARF-100). 3.2 Each request must include: employee name, department, manager name, requested systems, business justification, and requested access level. 3.3 The direct manager must approve the request electronically within ServiceNow. 3.4 IT Security reviews the request against the Role-Based Access Control (RBAC) matrix to ensure least privilege. 3.5 Upon approval, IT Operations provisions access within 2 business days. 4. LEAST PRIVILEGE: Access is granted based on the minimum permissions required to perform job duties. The RBAC matrix is maintained in SharePoint and updated quarterly by IT Security. 5. PRIVILEGED ACCESS: Privileged accounts require additional approval from the CISO and are reviewed monthly. 6. AUDIT TRAIL: All provisioning actions are logged in ServiceNow with timestamps and approver details."
    },
    {
        "id": "DOC-002", "audit": "AUD-2026-001", "name": "access_request_samples_q1_2026.pdf", "type": "pdf", "size": 180000, "pages": 25, "ocr": False,
        "text": "Access Request Evidence Package - Q1 2026. Sample 1: Request ARF-2026-0142 - Employee: Maria Garcia, Department: Accounting, Manager: Robert Chen (approved 01/15/2026). Systems requested: SAP ERP (GL module read-only), Oracle EPM (reporting). Business justification: New hire in accounts payable team. RBAC matrix check: Verified by IT Security (Jane Wu) on 01/16/2026. Provisioned: 01/17/2026 by ops-admin. Sample 2: Request ARF-2026-0198 - Employee: James Lee, Department: IT Development, Manager: Susan Park (approved 02/03/2026). Systems requested: GitHub Enterprise (developer role), JIRA (project contributor), AWS Dev account (restricted). Business justification: Transfer from QA to Development team. RBAC check: Verified 02/04/2026. Note: Production access NOT granted per SoD policy. Provisioned: 02/05/2026. Sample 3: Request ARF-2026-0267 - Employee: Priya Patel, Department: Finance, Manager: Alice Green (approved 02/20/2026). Systems requested: SAP ERP (AP module), Concur (expense approver level 2). Business justification: Promotion to Senior Accountant. RBAC check: Verified 02/21/2026. Previous read-only access upgraded to write. Provisioned: 02/22/2026. Total requests in Q1 2026: 342. All requests had manager approval documented. 100% RBAC matrix verification rate."
    },
    {
        "id": "DOC-003", "audit": "AUD-2026-001", "name": "termination_checklist_samples.pdf", "type": "pdf", "size": 156000, "pages": 18, "ocr": False,
        "text": "Employee Termination Access Revocation Evidence - Q1 2026. Process: HR sends termination notification via automated workflow to IT Security within 1 hour of termination decision. IT Security disables Active Directory account immediately. All application-specific access is revoked within 24 hours. Sample 1: Employee: Tom Richards, Termination Date: 01/22/2026. HR notification: 01/22/2026 09:15 AM. AD disabled: 01/22/2026 09:32 AM. VPN revoked: 01/22/2026 09:35 AM. Email disabled: 01/22/2026 09:40 AM. Badge deactivated: 01/22/2026 10:00 AM. SAP access removed: 01/22/2026 11:15 AM. Checklist completed: 01/22/2026 11:30 AM. Sample 2: Employee: Karen White, Termination Date: 02/14/2026. HR notification: 02/14/2026 02:00 PM. AD disabled: 02/14/2026 02:18 PM. VPN revoked: 02/14/2026 02:20 PM. All app access revoked: 02/14/2026 03:45 PM. Checklist signed by IT Security Lead. Summary: 47 terminations in Q1 2026. Average time to full revocation: 3.2 hours. 100% completed within 24-hour SLA. Zero exceptions noted."
    },
    {
        "id": "DOC-004", "audit": "AUD-2026-001", "name": "quarterly_access_review_q4_2025.pdf", "type": "pdf", "size": 320000, "pages": 35, "ocr": False,
        "text": "Quarterly User Access Review Report - Q4 2025. Performed by: Jane Doe, IT Security Manager. Review Period: October - December 2025. Applications Reviewed: SAP ERP, Oracle EPM, Workday HCM, Concur, SharePoint Financial Sites. Methodology: User listings extracted from each application on 12/15/2025. Lists compared against active employee roster from HR. Each manager reviewed their team members access and confirmed appropriateness. Findings: Total users reviewed: 1,847. Appropriate access confirmed: 1,823 (98.7%). Access revoked (no longer needed): 18 users. Access modified (excessive permissions): 6 users. Remediation: All 24 findings remediated within 10 business days. Evidence of remediation attached in Appendix B. Review sign-off: Jane Doe (IT Security) 12/28/2025, John Smith (VP IT) 12/29/2025, CFO Robert Miller 12/30/2025."
    },
    {
        "id": "DOC-005", "audit": "AUD-2026-001", "name": "change_management_policy.pdf", "type": "pdf", "size": 198000, "pages": 15, "ocr": False,
        "text": "Acme Corporation - Change Management Policy CM-POL-001 v4.0. Effective: July 1, 2025. 1. All changes to production systems must follow the formal change management process. 2. Change Request Process: 2.1 Developer submits change request in ServiceNow with description, business justification, risk assessment, rollback plan, and test results. 2.2 Technical review by lead developer or architect. 2.3 Change Advisory Board (CAB) meets weekly to review and approve standard changes. 2.4 High-risk changes require additional approval from VP of Engineering and business sponsor. 3. Testing Requirements: All changes must pass unit tests, integration tests, and UAT before production deployment. Test evidence must be attached to the change request. 4. Deployment: Separate operations team deploys approved changes using automated CI/CD pipeline. Developers do NOT have production deployment access. 5. Emergency Changes: Emergency changes may bypass CAB but require CTO approval and must be formally documented within 48 hours. 6. Post-Implementation Review: All changes are reviewed within 5 business days to confirm successful implementation."
    },
    {
        "id": "DOC-006", "audit": "AUD-2026-001", "name": "cab_meeting_minutes_jan_2026.pdf", "type": "pdf", "size": 87000, "pages": 8, "ocr": False,
        "text": "Change Advisory Board Meeting Minutes - January 2026. Meeting Date: 01/08/2026, 01/15/2026, 01/22/2026, 01/29/2026. Attendees: Bob Wilson (Chair), Technical Leads, Business Representatives. 01/08 Meeting: 12 changes reviewed. 10 approved, 1 deferred (insufficient testing), 1 rejected (no business justification). CHG-2026-0012: SAP patch SP15 - Approved. Risk: Medium. Testing: UAT completed 01/05. Deployment: 01/10 maintenance window. CHG-2026-0015: New GL reporting module - Approved. Risk: High. Additional approval from CFO obtained. UAT sign-off attached. 01/15 Meeting: 8 changes reviewed. All approved. 01/22 Meeting: 15 changes reviewed. 14 approved, 1 emergency change ratified (CHG-2026-0089 - security patch for Log4j variant). Emergency change had CTO approval (email evidence attached). Post-implementation review completed 01/25. 01/29 Meeting: 9 changes reviewed. All approved. Summary: 44 total changes in January. 42 standard approvals, 1 emergency, 1 rejection. 0 unauthorized changes detected."
    },
    {
        "id": "DOC-007", "audit": "AUD-2026-001", "name": "deployment_access_matrix.pdf", "type": "pdf", "size": 45000, "pages": 3, "ocr": False,
        "text": "Production Deployment Access Matrix - As of February 2026. Production deployment access is restricted to the DevOps Operations team only. Users with production deployment access: ops-deploy-1 (Sarah Connor), ops-deploy-2 (Mark Johnson), ops-deploy-3 (Rita Patel). CI/CD Service Account: svc-cicd-prod (automated deployments only, triggered by approved pipeline). Developer team members verified to NOT have production access: 45 developers checked against AD groups. No developer accounts found in prod-deploy, prod-admin, or prod-operations AD groups. Verification performed by: IT Security on 02/15/2026. Method: AD group membership export and cross-reference."
    },
    {
        "id": "DOC-008", "audit": "AUD-2026-001", "name": "backup_monitoring_report_jan_2026.pdf", "type": "pdf", "size": 67000, "pages": 5, "ocr": False,
        "text": "Backup Monitoring Report - January 2026. Backup Schedule: Daily incremental at 2:00 AM EST. Weekly full backup every Sunday at 1:00 AM EST. Systems covered: SAP ERP database, Oracle EPM, File servers, SharePoint, Exchange Online. January Results: 31 daily incremental backups: 31 successful (100%). 4 weekly full backups: 4 successful (100%). Backup verification: Random file restores performed on 01/12/2026 and 01/26/2026. Both successful. Backup storage: AWS S3 with cross-region replication to us-west-2. Encryption: AES-256. Retention: 90 days. Quarterly restoration test scheduled for March 2026. Last full DR test: September 2025 (next: March 2026). Alert configuration: Failed backups trigger PagerDuty alert to on-call engineer within 5 minutes."
    },
    {
        "id": "DOC-009", "audit": "AUD-2026-001", "name": "siem_dashboard_screenshot.png", "type": "png", "size": 2500000, "pages": 1, "ocr": True,
        "text": "SIEM Dashboard Screenshot - Splunk Enterprise Security. Captured: 02/28/2026. Dashboard shows: Active alerts: 3 (2 low, 1 medium). Alerts triaged in last 24h: 47. Average triage time: 8 minutes. SLA compliance (15-min triage): 99.2%. Incidents opened this month: 12. Incidents resolved: 11. Open incidents: 1 (in progress). Log sources: 156 active. Failed log sources: 0. Events per second: 12,847. Notable events panel shows correlation rules firing correctly across firewall, IDS, and endpoint detection platforms."
    },
    {
        "id": "DOC-010", "audit": "AUD-2026-001", "name": "monthly_reconciliation_feb_2026.pdf", "type": "pdf", "size": 410000, "pages": 42, "ocr": False,
        "text": "Monthly Financial Reconciliation Report - February 2026. Prepared by: Finance Team. Reviewed by: Alice Green, Controller. Bank Reconciliation: 5 bank accounts reconciled. Total balance: $47,283,912.45. All reconciling items identified and resolved. Outstanding items: 3 checks totaling $12,450 (cleared by 03/03). Intercompany Reconciliation: 8 intercompany accounts balanced. Net intercompany position: $0.00. Subledger to GL Reconciliation: AP subledger: Balanced. AR subledger: Balanced. FA subledger: $2,100 variance identified - traced to timing difference in asset disposal entry posted 02/28. Correcting entry JE-2026-0892 posted 03/01. All reconciliations completed within 5 business days of month-end. Reviewed and approved by Alice Green (Controller) 03/05/2026. Second review by Robert Miller (CFO) 03/05/2026."
    },
    {
        "id": "DOC-011", "audit": "AUD-2026-001", "name": "journal_entry_log_feb_2026.pdf", "type": "pdf", "size": 156000, "pages": 20, "ocr": False,
        "text": "Manual Journal Entry Log - February 2026. Total manual journal entries: 89. Entries above $10,000 threshold: 23. JE-2026-0845: Amount $125,000. Description: Accrual for consulting services. Prepared by: Mark Thompson. Approved by: Alice Green (Controller) 02/05/2026. Second approval: Robert Miller (CFO) 02/05/2026. Supporting docs: Invoice INV-8834 attached. JE-2026-0867: Amount $45,200. Description: Reclassification of prepaid insurance. Prepared by: Lisa Wang. Approved by: Alice Green 02/12/2026. Second approval: Robert Miller 02/12/2026. Supporting docs: Insurance schedule attached. JE-2026-0892: Amount $2,100. Description: FA disposal correction (per reconciliation finding). Prepared by: Sarah Kim. Approved by: Alice Green 03/01/2026. Second approval: Robert Miller 03/01/2026. All 23 entries above threshold had dual authorization documented. No exceptions noted. Monthly review by CFO completed 03/05/2026 - sign-off on file."
    },
    {
        "id": "DOC-012", "audit": "AUD-2026-002", "name": "network_security_architecture.pdf", "type": "pdf", "size": 890000, "pages": 28, "ocr": False,
        "text": "GlobalBank Financial - Network Security Architecture Document v2.1 - Updated January 2026. 1. Perimeter Security: Next-gen firewalls (Palo Alto PA-5260) deployed at all ingress/egress points. IDS/IPS (Suricata) monitors all traffic. DDoS protection via Cloudflare. 2. Network Segmentation: Production, development, and corporate networks fully segmented via VLANs and firewall rules. DMZ isolates public-facing services. Database tier accessible only from application tier. 3. Encryption: TLS 1.3 enforced for all external connections. Internal service mesh uses mTLS. Data at rest encrypted with AES-256 (AWS KMS managed keys). Key rotation every 90 days. 4. Authentication: Multi-factor authentication required for all users (Okta with FIDO2). SSO integrated with all applications. Service accounts use certificate-based authentication. 5. Monitoring: All network flows logged to Splunk SIEM. Anomaly detection via ML models. 24/7 SOC operated by CrowdStrike with 15-minute SLA for critical alerts."
    },
    {
        "id": "DOC-013", "audit": "AUD-2026-002", "name": "mfa_enrollment_report.pdf", "type": "pdf", "size": 34000, "pages": 4, "ocr": False,
        "text": "Multi-Factor Authentication Enrollment Report - February 2026. Source: Okta Admin Console. Total active users: 2,341. MFA enrolled: 2,341 (100%). MFA methods: FIDO2 security keys: 1,890 (primary). Okta Verify push: 2,200 (secondary). SMS: 0 (disabled per policy). Remote access users: 567. MFA enforcement: 100%. VPN connections without MFA: 0. Failed MFA attempts (February): 234. Locked accounts due to failed MFA: 12. All investigated - 10 user error, 2 suspicious (escalated to SOC). Policy: MFA required for all logins. Session timeout: 8 hours. Re-authentication required for privileged operations."
    },
    {
        "id": "DOC-014", "audit": "AUD-2026-002", "name": "incident_response_plan.pdf", "type": "pdf", "size": 230000, "pages": 22, "ocr": False,
        "text": "GlobalBank Financial - Incident Response Plan IRP-2026 v3.0. Effective: January 2026. 1. Detection: 24/7 SOC monitors SIEM alerts. Automated playbooks for common attack patterns. 2. Classification: P1 (Critical) - data breach, ransomware. P2 (High) - unauthorized access, malware. P3 (Medium) - phishing, policy violations. 3. Response Teams: Core IR team: CISO, SOC Lead, Legal, Communications. Extended: affected business unit, forensics. 4. Escalation: P1 within 15 minutes to CISO. P2 within 1 hour. P3 within 4 hours. 5. Communication: Internal stakeholders notified per communication matrix. Regulatory notifications within 72 hours for data breaches (GDPR/state laws). 6. Post-Incident: Root cause analysis within 5 business days. Lessons learned documented. Controls updated as needed. 7. Testing: Annual tabletop exercise. Last test: November 15, 2025. Participants: 18 staff across 6 departments. Test scenario: Ransomware attack on payment processing system. Results: RTO met (4 hours vs 6-hour target). 2 gaps identified: (1) backup restoration procedure needed update, (2) communication template for customers needed revision. Both remediated by 12/15/2025."
    },
    {
        "id": "DOC-015", "audit": "AUD-2026-002", "name": "soc_operations_metrics_jan_2026.pdf", "type": "pdf", "size": 78000, "pages": 6, "ocr": False,
        "text": "Security Operations Center Monthly Metrics - January 2026. Alert Volume: Total alerts: 15,234. True positives: 1,847 (12.1%). False positives: 13,387 (87.9%). Triage Performance: Average time to triage: 7.3 minutes. SLA compliance (15 min): 99.6%. Incidents: New incidents: 23. Resolved: 21. Carried forward: 2. Incident categories: Phishing attempts: 8. Malware detections: 5. Unauthorized access attempts: 4. Policy violations: 3. DDoS attempts: 2. Data exfiltration attempt: 1 (P1 - fully contained). Mean time to resolve: P1: 2.1 hours. P2: 6.4 hours. P3: 18.2 hours. All incidents documented in ServiceNow with root cause analysis. Monthly review with CISO completed 02/03/2026."
    },
    {
        "id": "DOC-016", "audit": "AUD-2026-002", "name": "capacity_planning_q1_2026.pdf", "type": "pdf", "size": 125000, "pages": 10, "ocr": False,
        "text": "Infrastructure Capacity Planning Report - Q1 2026. Current utilization: CPU: 45% average, 78% peak. Memory: 62% average. Storage: 71% used of 500TB allocated. Network bandwidth: 34% average utilization. Auto-scaling configuration: Application tier scales from 10 to 50 instances based on CPU > 70%. Database tier: Aurora Serverless with auto-scaling enabled (2-64 ACUs). Forecast: Based on 15% YoY growth, current capacity sufficient through Q3 2026. Recommendations: Increase storage allocation by 20% before Q2 to accommodate new data lake initiative. Review completed by Tom Harris (VP Infrastructure) and presented to CTO on 02/15/2026."
    },
]

print("Loading evidence documents...")
for doc in docs:
    text = esc(doc["text"])
    path = f"/Volumes/main/audit_schema/evidence_raw/{doc['audit']}/{doc['name']}"
    sql = (
        f"INSERT INTO {FQ}.evidence_documents VALUES ("
        f"'{doc['id']}', '{doc['audit']}', '{doc['name']}', '{doc['type']}', "
        f"'{path}', {doc['size']}, {doc['pages']}, '{text}', 'PARSED', NULL, "
        f"{'true' if doc['ocr'] else 'false'}, 'auditor@firm.com', "
        f"'2026-02-01T10:00:00', '2026-02-01T10:00:00')"
    )
    run_sql(sql, doc["id"])

# Verify
r = run_sql(f"SELECT COUNT(*) FROM {FQ}.evidence_documents", "count")
count = r.get("result", {}).get("data_array", [[0]])[0][0]
print(f"\nTotal evidence documents: {count}")
