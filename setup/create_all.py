"""
Create all infrastructure and load synthetic data for the Audit Control Testing App.
Uses main.audit_schema in the Databricks workspace.
"""
import subprocess, json, tempfile, os, sys

PROFILE = "fevm-akash-finance-app"
WAREHOUSE = "1b1d59e180e4ac26"
CATALOG = "main"
SCHEMA = "audit_schema"
FQ = f"{CATALOG}.{SCHEMA}"

def run_sql(stmt, desc=""):
    clean = " ".join(stmt.split())
    payload = json.dumps({"warehouse_id": WAREHOUSE, "statement": clean, "wait_timeout": "50s"})
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write(payload)
        tmpfile = f.name
    try:
        result = subprocess.run(
            ["databricks", "api", "post", "/api/2.0/sql/statements",
             "--profile", PROFILE, "--json", f"@{tmpfile}"],
            capture_output=True, text=True
        )
        if not result.stdout.strip():
            print(f"  FAIL [{desc}]: {result.stderr[:150]}")
            return {"status": {"state": "FAILED"}}
        data = json.loads(result.stdout)
        status = data.get("status", {}).get("state", "UNKNOWN")
        error = data.get("status", {}).get("error", {}).get("message", "")
        if status == "SUCCEEDED":
            print(f"  OK [{desc}]")
        else:
            print(f"  FAIL [{desc}]: {error[:200]}")
        return data
    finally:
        os.unlink(tmpfile)

# ============================================================
# PHASE 1: Create Tables
# ============================================================
print("=" * 60)
print("PHASE 1: Creating tables")
print("=" * 60)

tables = [
    (f"""CREATE TABLE IF NOT EXISTS {FQ}.audit_engagements (
        audit_id STRING NOT NULL,
        audit_name STRING,
        framework STRING,
        client_name STRING,
        description STRING,
        status STRING,
        created_by STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    ) USING DELTA COMMENT 'Audit engagement metadata'""", "audit_engagements"),

    (f"""CREATE TABLE IF NOT EXISTS {FQ}.controls (
        control_id STRING NOT NULL,
        audit_id STRING NOT NULL,
        control_code STRING,
        framework STRING,
        control_title STRING,
        control_description STRING,
        control_category STRING,
        risk_level STRING,
        frequency STRING,
        control_owner STRING,
        embedding ARRAY<FLOAT>,
        uploaded_by STRING,
        uploaded_at TIMESTAMP,
        source_file STRING,
        _ingested_at TIMESTAMP
    ) USING DELTA PARTITIONED BY (audit_id) COMMENT 'Audit controls'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""", "controls"),

    (f"""CREATE TABLE IF NOT EXISTS {FQ}.evidence_documents (
        document_id STRING NOT NULL,
        audit_id STRING NOT NULL,
        original_filename STRING,
        file_type STRING,
        file_path STRING,
        file_size_bytes BIGINT,
        page_count INT,
        extracted_text STRING,
        parse_status STRING,
        parse_error STRING,
        ocr_applied BOOLEAN DEFAULT FALSE,
        uploaded_by STRING,
        uploaded_at TIMESTAMP,
        _ingested_at TIMESTAMP
    ) USING DELTA PARTITIONED BY (audit_id) COMMENT 'Evidence documents'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""", "evidence_documents"),

    (f"""CREATE TABLE IF NOT EXISTS {FQ}.document_chunks (
        chunk_id STRING NOT NULL,
        document_id STRING NOT NULL,
        audit_id STRING NOT NULL,
        chunk_index INT,
        chunk_text STRING,
        token_count INT,
        page_numbers ARRAY<INT>,
        embedding ARRAY<FLOAT>,
        _created_at TIMESTAMP
    ) USING DELTA PARTITIONED BY (audit_id) COMMENT 'Document chunks'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""", "document_chunks"),

    (f"""CREATE TABLE IF NOT EXISTS {FQ}.control_evidence_matches (
        match_id STRING NOT NULL,
        control_id STRING NOT NULL,
        chunk_id STRING NOT NULL,
        document_id STRING NOT NULL,
        audit_id STRING NOT NULL,
        similarity_score DOUBLE,
        match_rank INT,
        _matched_at TIMESTAMP
    ) USING DELTA PARTITIONED BY (audit_id) COMMENT 'Vector similarity matches'""", "control_evidence_matches"),

    (f"""CREATE TABLE IF NOT EXISTS {FQ}.evaluation_results (
        evaluation_id STRING NOT NULL,
        control_id STRING NOT NULL,
        audit_id STRING NOT NULL,
        ai_verdict STRING,
        ai_confidence DOUBLE,
        ai_reasoning STRING,
        evidence_summary STRING,
        matched_document_ids ARRAY<STRING>,
        matched_chunk_ids ARRAY<STRING>,
        auditor_verdict STRING,
        auditor_notes STRING,
        auditor_id STRING,
        reviewed_at TIMESTAMP,
        model_used STRING,
        prompt_version STRING,
        evaluated_at TIMESTAMP,
        _created_at TIMESTAMP
    ) USING DELTA PARTITIONED BY (audit_id) COMMENT 'AI evaluation results'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true',
                   'delta.logRetentionDuration' = 'interval 365 days')""", "evaluation_results"),

    (f"""CREATE TABLE IF NOT EXISTS {FQ}.audit_log (
        log_id STRING NOT NULL,
        audit_id STRING,
        user_id STRING,
        action STRING,
        entity_type STRING,
        entity_id STRING,
        details STRING,
        timestamp TIMESTAMP
    ) USING DELTA PARTITIONED BY (audit_id) COMMENT 'Audit trail'
    TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 365 days')""", "audit_log"),

    (f"""CREATE TABLE IF NOT EXISTS {FQ}.framework_mappings (
        mapping_id STRING NOT NULL,
        source_framework STRING,
        source_control STRING,
        target_framework STRING,
        target_control STRING,
        mapping_type STRING,
        similarity_score DOUBLE,
        _created_at TIMESTAMP
    ) USING DELTA COMMENT 'Cross-framework control mappings'""", "framework_mappings"),
]

for stmt, desc in tables:
    run_sql(stmt, desc)

# ============================================================
# PHASE 2: Create Volumes
# ============================================================
print("\n" + "=" * 60)
print("PHASE 2: Creating volumes")
print("=" * 60)

volumes = [
    (f"CREATE VOLUME IF NOT EXISTS {FQ}.controls_raw COMMENT 'Raw control files'", "controls_raw"),
    (f"CREATE VOLUME IF NOT EXISTS {FQ}.evidence_raw COMMENT 'Raw evidence documents'", "evidence_raw"),
    (f"CREATE VOLUME IF NOT EXISTS {FQ}.checkpoints COMMENT 'Auto Loader checkpoints'", "checkpoints"),
]

for stmt, desc in volumes:
    run_sql(stmt, desc)

# ============================================================
# PHASE 3: Load Synthetic Data
# ============================================================
print("\n" + "=" * 60)
print("PHASE 3: Loading synthetic data")
print("=" * 60)

# 3a. Audit Engagements
run_sql(f"""
INSERT INTO {FQ}.audit_engagements VALUES
('AUD-2026-001', 'Acme Corp SOX 2026 Annual Audit', 'SOX', 'Acme Corporation', 'Annual SOX compliance audit for fiscal year 2026', 'IN_PROGRESS', 'auditor1@firm.com', '2026-01-15T09:00:00', '2026-03-01T14:30:00'),
('AUD-2026-002', 'GlobalBank SOC2 Type II', 'SOC2', 'GlobalBank Financial', 'SOC2 Type II examination covering security and availability', 'IN_PROGRESS', 'auditor2@firm.com', '2026-02-01T10:00:00', '2026-02-28T11:00:00'),
('AUD-2026-003', 'TechStart ISO 27001 Certification', 'ISO27001', 'TechStart Inc', 'Initial ISO 27001 certification audit', 'CREATED', 'auditor1@firm.com', '2026-03-01T08:00:00', '2026-03-01T08:00:00')
""", "audit_engagements data")

# 3b. Controls - SOX Controls
run_sql(f"""
INSERT INTO {FQ}.controls VALUES
('CTL-001', 'AUD-2026-001', 'SOX-AC-001', 'SOX', 'User Access Provisioning', 'New user accounts are provisioned only upon receipt of a properly authorized access request form approved by the employee manager and IT security. Access is granted based on the principle of least privilege and role-based access control matrices are maintained.', 'Access Control', 'HIGH', 'Continuous', 'John Smith', NULL, 'auditor1@firm.com', '2026-01-20T10:00:00', 'sox_controls_2026.csv', '2026-01-20T10:00:00'),
('CTL-002', 'AUD-2026-001', 'SOX-AC-002', 'SOX', 'User Access Termination', 'Upon employee termination or transfer, access to all systems is revoked within 24 hours. HR sends termination notification to IT and a checklist is completed confirming all access has been removed including VPN, email, badge access, and application-specific accounts.', 'Access Control', 'HIGH', 'Continuous', 'John Smith', NULL, 'auditor1@firm.com', '2026-01-20T10:00:00', 'sox_controls_2026.csv', '2026-01-20T10:00:00'),
('CTL-003', 'AUD-2026-001', 'SOX-AC-003', 'SOX', 'Periodic Access Reviews', 'Management performs quarterly access reviews of all critical financial applications. Reviews include verification that users have appropriate access based on current job responsibilities. Evidence of review completion and any remediation actions are documented.', 'Access Control', 'HIGH', 'Quarterly', 'Jane Doe', NULL, 'auditor1@firm.com', '2026-01-20T10:00:00', 'sox_controls_2026.csv', '2026-01-20T10:00:00'),
('CTL-004', 'AUD-2026-001', 'SOX-CM-001', 'SOX', 'Change Management Approval', 'All changes to production financial systems require documented change requests with business justification, risk assessment, testing evidence, and approval from both the change advisory board and application owner before deployment.', 'Change Management', 'HIGH', 'Continuous', 'Bob Wilson', NULL, 'auditor1@firm.com', '2026-01-20T10:00:00', 'sox_controls_2026.csv', '2026-01-20T10:00:00'),
('CTL-005', 'AUD-2026-001', 'SOX-CM-002', 'SOX', 'Segregation of Duties in Development', 'Developers do not have access to deploy changes directly to the production environment. A separate operations team or automated deployment pipeline with appropriate approval gates handles production deployments.', 'Change Management', 'HIGH', 'Continuous', 'Bob Wilson', NULL, 'auditor1@firm.com', '2026-01-20T10:00:00', 'sox_controls_2026.csv', '2026-01-20T10:00:00'),
('CTL-006', 'AUD-2026-001', 'SOX-BC-001', 'SOX', 'Backup and Recovery', 'Critical financial data is backed up daily with full backups weekly. Backup success is monitored and failures are escalated. Backup restoration tests are performed quarterly and documented with results reviewed by management.', 'Business Continuity', 'MEDIUM', 'Daily', 'Sarah Lee', NULL, 'auditor1@firm.com', '2026-01-20T10:00:00', 'sox_controls_2026.csv', '2026-01-20T10:00:00'),
('CTL-007', 'AUD-2026-001', 'SOX-BC-002', 'SOX', 'Disaster Recovery Testing', 'The disaster recovery plan is tested annually through a full failover exercise. Test results including recovery time objectives and recovery point objectives are documented and gaps are remediated within 30 days.', 'Business Continuity', 'MEDIUM', 'Annual', 'Sarah Lee', NULL, 'auditor1@firm.com', '2026-01-20T10:00:00', 'sox_controls_2026.csv', '2026-01-20T10:00:00'),
('CTL-008', 'AUD-2026-001', 'SOX-MO-001', 'SOX', 'Security Monitoring and Logging', 'Security events from firewalls, IDS/IPS, and critical servers are aggregated in a SIEM platform. Alerts are triaged within 15 minutes and security incidents are investigated and documented with root cause analysis.', 'Monitoring', 'HIGH', 'Continuous', 'Mike Chen', NULL, 'auditor1@firm.com', '2026-01-20T10:00:00', 'sox_controls_2026.csv', '2026-01-20T10:00:00'),
('CTL-009', 'AUD-2026-001', 'SOX-FR-001', 'SOX', 'Financial Reporting Reconciliation', 'Monthly reconciliation of all intercompany accounts, bank accounts, and general ledger subledger balances is performed. Reconciling items are investigated and resolved within 5 business days. Reconciliations are reviewed and approved by a controller.', 'Financial Reporting', 'HIGH', 'Monthly', 'Alice Green', NULL, 'auditor1@firm.com', '2026-01-20T10:00:00', 'sox_controls_2026.csv', '2026-01-20T10:00:00'),
('CTL-010', 'AUD-2026-001', 'SOX-FR-002', 'SOX', 'Journal Entry Authorization', 'All manual journal entries above $10,000 require dual authorization. Journal entries are supported by adequate documentation and business justification. A monthly review of all manual journal entries is performed by the CFO.', 'Financial Reporting', 'HIGH', 'Continuous', 'Alice Green', NULL, 'auditor1@firm.com', '2026-01-20T10:00:00', 'sox_controls_2026.csv', '2026-01-20T10:00:00')
""", "SOX controls")

# 3c. Controls - SOC2 Controls
run_sql(f"""
INSERT INTO {FQ}.controls VALUES
('CTL-011', 'AUD-2026-002', 'SOC2-CC6.1', 'SOC2', 'Logical Access Security', 'The entity implements logical access security software, infrastructure, and architectures over protected information assets to protect them from security events. This includes firewalls, intrusion detection, multi-factor authentication, and encryption of data in transit and at rest.', 'Security', 'HIGH', 'Continuous', 'David Kim', NULL, 'auditor2@firm.com', '2026-02-05T10:00:00', 'soc2_controls.csv', '2026-02-05T10:00:00'),
('CTL-012', 'AUD-2026-002', 'SOC2-CC6.2', 'SOC2', 'User Authentication', 'Prior to issuing system credentials and granting system access, the entity registers and authorizes new internal and external users. For those users whose access is no longer authorized, credentials are removed or disabled. MFA is required for all remote access.', 'Security', 'HIGH', 'Continuous', 'David Kim', NULL, 'auditor2@firm.com', '2026-02-05T10:00:00', 'soc2_controls.csv', '2026-02-05T10:00:00'),
('CTL-013', 'AUD-2026-002', 'SOC2-CC7.1', 'SOC2', 'Incident Detection', 'The entity monitors system components and operation of those components for anomalies that are indicative of malicious acts, natural disasters, and errors affecting the entity ability to meet its objectives. A 24/7 SOC monitors alerts and escalates incidents per documented procedures.', 'Monitoring', 'HIGH', 'Continuous', 'Lisa Park', NULL, 'auditor2@firm.com', '2026-02-05T10:00:00', 'soc2_controls.csv', '2026-02-05T10:00:00'),
('CTL-014', 'AUD-2026-002', 'SOC2-CC7.2', 'SOC2', 'Incident Response', 'The entity designs, implements, and operates processes and communication protocols to detect and respond to security incidents. Incident response plans are tested annually and post-incident reviews are conducted to improve detection and response capabilities.', 'Incident Management', 'HIGH', 'Annual', 'Lisa Park', NULL, 'auditor2@firm.com', '2026-02-05T10:00:00', 'soc2_controls.csv', '2026-02-05T10:00:00'),
('CTL-015', 'AUD-2026-002', 'SOC2-CC8.1', 'SOC2', 'Change Management Process', 'Changes to infrastructure, data, software, and procedures are authorized, designed, developed, configured, documented, tested, approved, and implemented to meet the entity commitments and requirements. Emergency changes follow a streamlined process with post-implementation review.', 'Change Management', 'HIGH', 'Continuous', 'Tom Harris', NULL, 'auditor2@firm.com', '2026-02-05T10:00:00', 'soc2_controls.csv', '2026-02-05T10:00:00'),
('CTL-016', 'AUD-2026-002', 'SOC2-A1.1', 'SOC2', 'System Availability', 'The entity maintains, monitors, and evaluates current processing capacity and use of system components to manage capacity demand. Infrastructure is auto-scaled based on load and capacity planning is performed quarterly.', 'Availability', 'MEDIUM', 'Quarterly', 'Tom Harris', NULL, 'auditor2@firm.com', '2026-02-05T10:00:00', 'soc2_controls.csv', '2026-02-05T10:00:00'),
('CTL-017', 'AUD-2026-002', 'SOC2-A1.2', 'SOC2', 'Data Backup and Recovery', 'The entity authorizes, designs, develops, implements, operates, approves, maintains, and monitors environmental protections, software, data backup processes, and recovery infrastructure. Backups are encrypted and stored offsite with daily verification.', 'Availability', 'MEDIUM', 'Daily', 'David Kim', NULL, 'auditor2@firm.com', '2026-02-05T10:00:00', 'soc2_controls.csv', '2026-02-05T10:00:00'),
('CTL-018', 'AUD-2026-002', 'SOC2-CC9.1', 'SOC2', 'Risk Assessment', 'The entity identifies, assesses, and manages risk related to the achievement of objectives. Risk assessments are performed annually and updated for significant changes. Risk register is reviewed quarterly by the executive team.', 'Risk Management', 'HIGH', 'Annual', 'Lisa Park', NULL, 'auditor2@firm.com', '2026-02-05T10:00:00', 'soc2_controls.csv', '2026-02-05T10:00:00')
""", "SOC2 controls")

# 3d. Evidence Documents with synthetic extracted text
evidence_docs = [
    # Evidence for SOX Access Controls
    ("DOC-001", "AUD-2026-001", "user_provisioning_policy.pdf", "pdf", "/Volumes/main/audit_schema/evidence_raw/AUD-2026-001/user_provisioning_policy.pdf", 245000, 12,
     "Acme Corporation - User Access Provisioning Policy v3.2 - Effective Date: January 1, 2026. "
     "1. PURPOSE: This policy establishes the procedures for provisioning, modifying, and deprovisioning user access to Acme Corporation information systems. "
     "2. SCOPE: All employees, contractors, and third-party users requiring access to Acme IT systems. "
     "3. PROVISIONING PROCESS: 3.1 All access requests must be submitted through the ServiceNow ITSM portal using the Access Request Form (ARF-100). "
     "3.2 Each request must include: employee name, department, manager name, requested systems, business justification, and requested access level. "
     "3.3 The direct manager must approve the request electronically within ServiceNow. "
     "3.4 IT Security reviews the request against the Role-Based Access Control (RBAC) matrix to ensure least privilege. "
     "3.5 Upon approval, IT Operations provisions access within 2 business days. "
     "4. LEAST PRIVILEGE: Access is granted based on the minimum permissions required to perform job duties. "
     "The RBAC matrix is maintained in SharePoint and updated quarterly by IT Security. "
     "5. PRIVILEGED ACCESS: Privileged accounts require additional approval from the CISO and are reviewed monthly. "
     "6. AUDIT TRAIL: All provisioning actions are logged in ServiceNow with timestamps and approver details.",
     "PARSED", None, False),

    ("DOC-002", "AUD-2026-001", "access_request_samples_q1_2026.pdf", "pdf", "/Volumes/main/audit_schema/evidence_raw/AUD-2026-001/access_request_samples_q1_2026.pdf", 180000, 25,
     "Access Request Evidence Package - Q1 2026. "
     "Sample 1: Request ARF-2026-0142 - Employee: Maria Garcia, Department: Accounting, Manager: Robert Chen (approved 01/15/2026). "
     "Systems requested: SAP ERP (GL module read-only), Oracle EPM (reporting). Business justification: New hire in accounts payable team. "
     "RBAC matrix check: Verified by IT Security (Jane Wu) on 01/16/2026. Provisioned: 01/17/2026 by ops-admin. "
     "Sample 2: Request ARF-2026-0198 - Employee: James Lee, Department: IT Development, Manager: Susan Park (approved 02/03/2026). "
     "Systems requested: GitHub Enterprise (developer role), JIRA (project contributor), AWS Dev account (restricted). "
     "Business justification: Transfer from QA to Development team. RBAC check: Verified 02/04/2026. Note: Production access NOT granted per SoD policy. Provisioned: 02/05/2026. "
     "Sample 3: Request ARF-2026-0267 - Employee: Priya Patel, Department: Finance, Manager: Alice Green (approved 02/20/2026). "
     "Systems requested: SAP ERP (AP module), Concur (expense approver level 2). Business justification: Promotion to Senior Accountant. "
     "RBAC check: Verified 02/21/2026. Previous read-only access upgraded to write. Provisioned: 02/22/2026. "
     "Total requests in Q1 2026: 342. All requests had manager approval documented. 100% RBAC matrix verification rate.",
     "PARSED", None, False),

    ("DOC-003", "AUD-2026-001", "termination_checklist_samples.pdf", "pdf", "/Volumes/main/audit_schema/evidence_raw/AUD-2026-001/termination_checklist_samples.pdf", 156000, 18,
     "Employee Termination Access Revocation Evidence - Q1 2026. "
     "Process: HR sends termination notification via automated workflow to IT Security within 1 hour of termination decision. "
     "IT Security disables Active Directory account immediately. All application-specific access is revoked within 24 hours. "
     "Sample 1: Employee: Tom Richards, Termination Date: 01/22/2026. HR notification: 01/22/2026 09:15 AM. "
     "AD disabled: 01/22/2026 09:32 AM. VPN revoked: 01/22/2026 09:35 AM. Email disabled: 01/22/2026 09:40 AM. "
     "Badge deactivated: 01/22/2026 10:00 AM. SAP access removed: 01/22/2026 11:15 AM. Checklist completed: 01/22/2026 11:30 AM. "
     "Sample 2: Employee: Karen White, Termination Date: 02/14/2026. HR notification: 02/14/2026 02:00 PM. "
     "AD disabled: 02/14/2026 02:18 PM. VPN revoked: 02/14/2026 02:20 PM. All app access revoked: 02/14/2026 03:45 PM. "
     "Checklist signed by IT Security Lead. "
     "Summary: 47 terminations in Q1 2026. Average time to full revocation: 3.2 hours. 100% completed within 24-hour SLA. "
     "Zero exceptions noted.",
     "PARSED", None, False),

    ("DOC-004", "AUD-2026-001", "quarterly_access_review_q4_2025.pdf", "pdf", "/Volumes/main/audit_schema/evidence_raw/AUD-2026-001/quarterly_access_review_q4_2025.pdf", 320000, 35,
     "Quarterly User Access Review Report - Q4 2025. Performed by: Jane Doe, IT Security Manager. Review Period: October - December 2025. "
     "Applications Reviewed: SAP ERP, Oracle EPM, Workday HCM, Concur, SharePoint Financial Sites. "
     "Methodology: User listings extracted from each application on 12/15/2025. Lists compared against active employee roster from HR. "
     "Each manager reviewed their team members access and confirmed appropriateness. "
     "Findings: Total users reviewed: 1,847. Appropriate access confirmed: 1,823 (98.7%). "
     "Access revoked (no longer needed): 18 users. Access modified (excessive permissions): 6 users. "
     "Remediation: All 24 findings remediated within 10 business days. Evidence of remediation attached in Appendix B. "
     "Review sign-off: Jane Doe (IT Security) 12/28/2025, John Smith (VP IT) 12/29/2025, CFO Robert Miller 12/30/2025.",
     "PARSED", None, False),

    ("DOC-005", "AUD-2026-001", "change_management_policy.pdf", "pdf", "/Volumes/main/audit_schema/evidence_raw/AUD-2026-001/change_management_policy.pdf", 198000, 15,
     "Acme Corporation - Change Management Policy CM-POL-001 v4.0. Effective: July 1, 2025. "
     "1. All changes to production systems must follow the formal change management process. "
     "2. Change Request Process: 2.1 Developer submits change request in ServiceNow with description, business justification, risk assessment, rollback plan, and test results. "
     "2.2 Technical review by lead developer or architect. "
     "2.3 Change Advisory Board (CAB) meets weekly to review and approve standard changes. "
     "2.4 High-risk changes require additional approval from VP of Engineering and business sponsor. "
     "3. Testing Requirements: All changes must pass unit tests, integration tests, and UAT before production deployment. "
     "Test evidence must be attached to the change request. "
     "4. Deployment: Separate operations team deploys approved changes using automated CI/CD pipeline. "
     "Developers do NOT have production deployment access. "
     "5. Emergency Changes: Emergency changes may bypass CAB but require CTO approval and must be formally documented within 48 hours. "
     "6. Post-Implementation Review: All changes are reviewed within 5 business days to confirm successful implementation.",
     "PARSED", None, False),

    ("DOC-006", "AUD-2026-001", "cab_meeting_minutes_jan_2026.pdf", "pdf", "/Volumes/main/audit_schema/evidence_raw/AUD-2026-001/cab_meeting_minutes_jan_2026.pdf", 87000, 8,
     "Change Advisory Board Meeting Minutes - January 2026. "
     "Meeting Date: 01/08/2026, 01/15/2026, 01/22/2026, 01/29/2026. "
     "Attendees: Bob Wilson (Chair), Technical Leads, Business Representatives. "
     "01/08 Meeting: 12 changes reviewed. 10 approved, 1 deferred (insufficient testing), 1 rejected (no business justification). "
     "CHG-2026-0012: SAP patch SP15 - Approved. Risk: Medium. Testing: UAT completed 01/05. Deployment: 01/10 maintenance window. "
     "CHG-2026-0015: New GL reporting module - Approved. Risk: High. Additional approval from CFO obtained. UAT sign-off attached. "
     "01/15 Meeting: 8 changes reviewed. All approved. "
     "01/22 Meeting: 15 changes reviewed. 14 approved, 1 emergency change ratified (CHG-2026-0089 - security patch for Log4j variant). "
     "Emergency change had CTO approval (email evidence attached). Post-implementation review completed 01/25. "
     "01/29 Meeting: 9 changes reviewed. All approved. "
     "Summary: 44 total changes in January. 42 standard approvals, 1 emergency, 1 rejection. 0 unauthorized changes detected.",
     "PARSED", None, False),

    ("DOC-007", "AUD-2026-001", "deployment_access_matrix.pdf", "pdf", "/Volumes/main/audit_schema/evidence_raw/AUD-2026-001/deployment_access_matrix.pdf", 45000, 3,
     "Production Deployment Access Matrix - As of February 2026. "
     "Production deployment access is restricted to the DevOps Operations team only. "
     "Users with production deployment access: ops-deploy-1 (Sarah Connor), ops-deploy-2 (Mark Johnson), ops-deploy-3 (Rita Patel). "
     "CI/CD Service Account: svc-cicd-prod (automated deployments only, triggered by approved pipeline). "
     "Developer team members verified to NOT have production access: 45 developers checked against AD groups. "
     "No developer accounts found in prod-deploy, prod-admin, or prod-operations AD groups. "
     "Verification performed by: IT Security on 02/15/2026. Method: AD group membership export and cross-reference.",
     "PARSED", None, False),

    ("DOC-008", "AUD-2026-001", "backup_monitoring_report_jan_2026.pdf", "pdf", "/Volumes/main/audit_schema/evidence_raw/AUD-2026-001/backup_monitoring_report_jan_2026.pdf", 67000, 5,
     "Backup Monitoring Report - January 2026. "
     "Backup Schedule: Daily incremental at 2:00 AM EST. Weekly full backup every Sunday at 1:00 AM EST. "
     "Systems covered: SAP ERP database, Oracle EPM, File servers, SharePoint, Exchange Online. "
     "January Results: 31 daily incremental backups: 31 successful (100%). 4 weekly full backups: 4 successful (100%). "
     "Backup verification: Random file restores performed on 01/12/2026 and 01/26/2026. Both successful. "
     "Backup storage: AWS S3 with cross-region replication to us-west-2. Encryption: AES-256. Retention: 90 days. "
     "Quarterly restoration test scheduled for March 2026. Last full DR test: September 2025 (next: March 2026). "
     "Alert configuration: Failed backups trigger PagerDuty alert to on-call engineer within 5 minutes.",
     "PARSED", None, False),

    ("DOC-009", "AUD-2026-001", "siem_dashboard_screenshot.png", "png", "/Volumes/main/audit_schema/evidence_raw/AUD-2026-001/siem_dashboard_screenshot.png", 2500000, 1,
     "SIEM Dashboard Screenshot - Splunk Enterprise Security. Captured: 02/28/2026. "
     "Dashboard shows: Active alerts: 3 (2 low, 1 medium). Alerts triaged in last 24h: 47. "
     "Average triage time: 8 minutes. SLA compliance (15-min triage): 99.2%. "
     "Incidents opened this month: 12. Incidents resolved: 11. Open incidents: 1 (in progress). "
     "Log sources: 156 active. Failed log sources: 0. Events per second: 12,847. "
     "Notable events panel shows correlation rules firing correctly across firewall, IDS, and endpoint detection platforms.",
     "PARSED", None, True),

    ("DOC-010", "AUD-2026-001", "monthly_reconciliation_feb_2026.pdf", "pdf", "/Volumes/main/audit_schema/evidence_raw/AUD-2026-001/monthly_reconciliation_feb_2026.pdf", 410000, 42,
     "Monthly Financial Reconciliation Report - February 2026. Prepared by: Finance Team. Reviewed by: Alice Green, Controller. "
     "Bank Reconciliation: 5 bank accounts reconciled. Total balance: $47,283,912.45. "
     "All reconciling items identified and resolved. Outstanding items: 3 checks totaling $12,450 (cleared by 03/03). "
     "Intercompany Reconciliation: 8 intercompany accounts balanced. Net intercompany position: $0.00. "
     "Subledger to GL Reconciliation: AP subledger: Balanced. AR subledger: Balanced. FA subledger: $2,100 variance identified - "
     "traced to timing difference in asset disposal entry posted 02/28. Correcting entry JE-2026-0892 posted 03/01. "
     "All reconciliations completed within 5 business days of month-end. "
     "Reviewed and approved by Alice Green (Controller) 03/05/2026. Second review by Robert Miller (CFO) 03/05/2026.",
     "PARSED", None, False),

    ("DOC-011", "AUD-2026-001", "journal_entry_log_feb_2026.pdf", "pdf", "/Volumes/main/audit_schema/evidence_raw/AUD-2026-001/journal_entry_log_feb_2026.pdf", 156000, 20,
     "Manual Journal Entry Log - February 2026. "
     "Total manual journal entries: 89. Entries above $10,000 threshold: 23. "
     "JE-2026-0845: Amount $125,000. Description: Accrual for consulting services. Prepared by: Mark Thompson. "
     "Approved by: Alice Green (Controller) 02/05/2026. Second approval: Robert Miller (CFO) 02/05/2026. Supporting docs: Invoice INV-8834 attached. "
     "JE-2026-0867: Amount $45,200. Description: Reclassification of prepaid insurance. Prepared by: Lisa Wang. "
     "Approved by: Alice Green 02/12/2026. Second approval: Robert Miller 02/12/2026. Supporting docs: Insurance schedule attached. "
     "JE-2026-0892: Amount $2,100. Description: FA disposal correction (per reconciliation finding). Prepared by: Sarah Kim. "
     "Approved by: Alice Green 03/01/2026. Second approval: Robert Miller 03/01/2026. "
     "All 23 entries above threshold had dual authorization documented. No exceptions noted. "
     "Monthly review by CFO completed 03/05/2026 - sign-off on file.",
     "PARSED", None, False),

    # Evidence for SOC2
    ("DOC-012", "AUD-2026-002", "network_security_architecture.pdf", "pdf", "/Volumes/main/audit_schema/evidence_raw/AUD-2026-002/network_security_architecture.pdf", 890000, 28,
     "GlobalBank Financial - Network Security Architecture Document v2.1 - Updated January 2026. "
     "1. Perimeter Security: Next-gen firewalls (Palo Alto PA-5260) deployed at all ingress/egress points. "
     "IDS/IPS (Suricata) monitors all traffic. DDoS protection via Cloudflare. "
     "2. Network Segmentation: Production, development, and corporate networks fully segmented via VLANs and firewall rules. "
     "DMZ isolates public-facing services. Database tier accessible only from application tier. "
     "3. Encryption: TLS 1.3 enforced for all external connections. Internal service mesh uses mTLS. "
     "Data at rest encrypted with AES-256 (AWS KMS managed keys). Key rotation every 90 days. "
     "4. Authentication: Multi-factor authentication required for all users (Okta with FIDO2). "
     "SSO integrated with all applications. Service accounts use certificate-based authentication. "
     "5. Monitoring: All network flows logged to Splunk SIEM. Anomaly detection via ML models. "
     "24/7 SOC operated by CrowdStrike with 15-minute SLA for critical alerts.",
     "PARSED", None, False),

    ("DOC-013", "AUD-2026-002", "mfa_enrollment_report.pdf", "pdf", "/Volumes/main/audit_schema/evidence_raw/AUD-2026-002/mfa_enrollment_report.pdf", 34000, 4,
     "Multi-Factor Authentication Enrollment Report - February 2026. Source: Okta Admin Console. "
     "Total active users: 2,341. MFA enrolled: 2,341 (100%). "
     "MFA methods: FIDO2 security keys: 1,890 (primary). Okta Verify push: 2,200 (secondary). SMS: 0 (disabled per policy). "
     "Remote access users: 567. MFA enforcement: 100%. VPN connections without MFA: 0. "
     "Failed MFA attempts (February): 234. Locked accounts due to failed MFA: 12. All investigated - 10 user error, 2 suspicious (escalated to SOC). "
     "Policy: MFA required for all logins. Session timeout: 8 hours. Re-authentication required for privileged operations.",
     "PARSED", None, False),

    ("DOC-014", "AUD-2026-002", "incident_response_plan.pdf", "pdf", "/Volumes/main/audit_schema/evidence_raw/AUD-2026-002/incident_response_plan.pdf", 230000, 22,
     "GlobalBank Financial - Incident Response Plan IRP-2026 v3.0. Effective: January 2026. "
     "1. Detection: 24/7 SOC monitors SIEM alerts. Automated playbooks for common attack patterns. "
     "2. Classification: P1 (Critical) - data breach, ransomware. P2 (High) - unauthorized access, malware. P3 (Medium) - phishing, policy violations. "
     "3. Response Teams: Core IR team: CISO, SOC Lead, Legal, Communications. Extended: affected business unit, forensics. "
     "4. Escalation: P1 within 15 minutes to CISO. P2 within 1 hour. P3 within 4 hours. "
     "5. Communication: Internal stakeholders notified per communication matrix. Regulatory notifications within 72 hours for data breaches (GDPR/state laws). "
     "6. Post-Incident: Root cause analysis within 5 business days. Lessons learned documented. Controls updated as needed. "
     "7. Testing: Annual tabletop exercise. Last test: November 15, 2025. Participants: 18 staff across 6 departments. "
     "Test scenario: Ransomware attack on payment processing system. Results: RTO met (4 hours vs 6-hour target). "
     "2 gaps identified: (1) backup restoration procedure needed update, (2) communication template for customers needed revision. Both remediated by 12/15/2025.",
     "PARSED", None, False),

    ("DOC-015", "AUD-2026-002", "soc_operations_metrics_jan_2026.pdf", "pdf", "/Volumes/main/audit_schema/evidence_raw/AUD-2026-002/soc_operations_metrics_jan_2026.pdf", 78000, 6,
     "Security Operations Center Monthly Metrics - January 2026. "
     "Alert Volume: Total alerts: 15,234. True positives: 1,847 (12.1%). False positives: 13,387 (87.9%). "
     "Triage Performance: Average time to triage: 7.3 minutes. SLA compliance (15 min): 99.6%. "
     "Incidents: New incidents: 23. Resolved: 21. Carried forward: 2. "
     "Incident categories: Phishing attempts: 8. Malware detections: 5. Unauthorized access attempts: 4. "
     "Policy violations: 3. DDoS attempts: 2. Data exfiltration attempt: 1 (P1 - fully contained). "
     "Mean time to resolve: P1: 2.1 hours. P2: 6.4 hours. P3: 18.2 hours. "
     "All incidents documented in ServiceNow with root cause analysis. Monthly review with CISO completed 02/03/2026.",
     "PARSED", None, False),

    ("DOC-016", "AUD-2026-002", "capacity_planning_q1_2026.pdf", "pdf", "/Volumes/main/audit_schema/evidence_raw/AUD-2026-002/capacity_planning_q1_2026.pdf", 125000, 10,
     "Infrastructure Capacity Planning Report - Q1 2026. "
     "Current utilization: CPU: 45% average, 78% peak. Memory: 62% average. Storage: 71% used of 500TB allocated. "
     "Network bandwidth: 34% average utilization. "
     "Auto-scaling configuration: Application tier scales from 10 to 50 instances based on CPU > 70%. "
     "Database tier: Aurora Serverless with auto-scaling enabled (2-64 ACUs). "
     "Forecast: Based on 15% YoY growth, current capacity sufficient through Q3 2026. "
     "Recommendations: Increase storage allocation by 20% before Q2 to accommodate new data lake initiative. "
     "Review completed by Tom Harris (VP Infrastructure) and presented to CTO on 02/15/2026.",
     "PARSED", None, False)
]

# Insert evidence documents in batches (SQL has length limits)
for i in range(0, len(evidence_docs), 3):
    batch = evidence_docs[i:i+3]
    values = []
    for doc in batch:
        doc_id, audit_id, fname, ftype, fpath, fsize, pages, text, status, error, ocr = doc
        # Escape single quotes in text
        text_escaped = text.replace("'", "''")
        error_val = f"'{error}'" if error else "NULL"
        values.append(
            f"('{doc_id}', '{audit_id}', '{fname}', '{ftype}', '{fpath}', "
            f"{fsize}, {pages}, '{text_escaped}', '{status}', {error_val}, "
            f"{'true' if ocr else 'false'}, 'auditor@firm.com', "
            f"'2026-02-01T10:00:00', '2026-02-01T10:00:00')"
        )
    sql = f"INSERT INTO {FQ}.evidence_documents VALUES " + ",\n".join(values)
    run_sql(sql, f"evidence batch {i//3 + 1}")

print("\n" + "=" * 60)
print("PHASE 4: Verify data")
print("=" * 60)

for table in ["audit_engagements", "controls", "evidence_documents"]:
    r = run_sql(f"SELECT COUNT(*) FROM {FQ}.{table}", f"count:{table}")
    count = r.get("result", {}).get("data_array", [[0]])[0][0]
    print(f"    {table}: {count} rows")

print("\nDone!")
