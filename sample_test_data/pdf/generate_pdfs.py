#!/usr/bin/env python3
"""Generate realistic SOX compliance audit evidence PDF documents."""

from fpdf import FPDF
import os

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))


class AuditPDF(FPDF):
    """Base PDF class with professional audit document styling."""

    def __init__(self, doc_number, doc_title, classification="CONFIDENTIAL"):
        super().__init__()
        self.doc_number = doc_number
        self.doc_title = doc_title
        self.classification = classification
        self.set_auto_page_break(auto=True, margin=25)

    def header(self):
        self.set_font("Helvetica", "B", 8)
        self.set_text_color(100, 100, 100)
        self.cell(0, 5, "Meridian Financial Services, Inc.", align="L")
        self.ln(4)
        self.set_font("Helvetica", "", 7)
        self.cell(0, 5, f"Document No: {self.doc_number}", align="L")
        self.cell(0, 5, self.classification, align="R")
        self.ln(5)
        self.set_draw_color(0, 51, 102)
        self.set_line_width(0.5)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(3)

    def footer(self):
        self.set_y(-20)
        self.set_draw_color(0, 51, 102)
        self.set_line_width(0.3)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(2)
        self.set_font("Helvetica", "I", 7)
        self.set_text_color(100, 100, 100)
        self.cell(0, 5, f"{self.classification} - {self.doc_title}", align="L")
        self.cell(0, 5, f"Page {self.page_no()}/{{nb}}", align="R")

    def add_title_page(self, title, subtitle, version, effective_date, owner, approver):
        self.add_page()
        self.ln(30)
        self.set_font("Helvetica", "B", 24)
        self.set_text_color(0, 51, 102)
        self.multi_cell(0, 12, title, align="C")
        self.ln(5)
        self.set_font("Helvetica", "", 14)
        self.set_text_color(80, 80, 80)
        self.multi_cell(0, 8, subtitle, align="C")
        self.ln(20)

        self.set_font("Helvetica", "", 10)
        self.set_text_color(0, 0, 0)
        meta = [
            ("Document Number", self.doc_number),
            ("Version", version),
            ("Effective Date", effective_date),
            ("Document Owner", owner),
            ("Approved By", approver),
            ("Classification", self.classification),
        ]
        for label, value in meta:
            self.set_font("Helvetica", "B", 10)
            self.cell(55, 8, f"{label}:", align="R")
            self.set_font("Helvetica", "", 10)
            self.cell(0, 8, f"  {value}")
            self.ln(8)

    def section_header(self, number, title):
        self.ln(4)
        self.set_font("Helvetica", "B", 13)
        self.set_text_color(0, 51, 102)
        self.cell(0, 8, f"{number}  {title}")
        self.ln(9)
        self.set_text_color(0, 0, 0)

    def sub_header(self, number, title):
        self.ln(2)
        self.set_font("Helvetica", "B", 11)
        self.set_text_color(40, 40, 40)
        self.cell(0, 7, f"{number}  {title}")
        self.ln(8)
        self.set_text_color(0, 0, 0)

    def body_text(self, text):
        self.set_font("Helvetica", "", 10)
        self.multi_cell(0, 5.5, text)
        self.ln(2)

    def bullet(self, text):
        self.set_font("Helvetica", "", 10)
        x = self.get_x()
        self.cell(8, 5.5, "-")
        self.multi_cell(0, 5.5, text)
        self.ln(1)

    def add_table(self, headers, rows, col_widths=None):
        if col_widths is None:
            col_widths = [190 / len(headers)] * len(headers)
        # Header
        self.set_font("Helvetica", "B", 9)
        self.set_fill_color(0, 51, 102)
        self.set_text_color(255, 255, 255)
        for i, h in enumerate(headers):
            self.cell(col_widths[i], 7, h, border=1, fill=True, align="C")
        self.ln()
        # Rows
        self.set_font("Helvetica", "", 9)
        self.set_text_color(0, 0, 0)
        fill = False
        for row in rows:
            if fill:
                self.set_fill_color(230, 237, 245)
            else:
                self.set_fill_color(255, 255, 255)
            max_h = 7
            for i, cell in enumerate(row):
                self.cell(col_widths[i], max_h, str(cell), border=1, fill=True, align="C")
            self.ln()
            fill = not fill
        self.ln(3)

    def signature_block(self, name, title, date):
        self.ln(5)
        self.set_font("Helvetica", "", 10)
        self.cell(60, 6, "_" * 35)
        self.cell(50, 6, "")
        self.cell(60, 6, "_" * 25)
        self.ln(5)
        self.set_font("Helvetica", "B", 10)
        self.cell(60, 6, name)
        self.cell(50, 6, "")
        self.cell(60, 6, f"Date: {date}")
        self.ln(5)
        self.set_font("Helvetica", "I", 9)
        self.cell(60, 6, title)
        self.ln(8)


def create_access_control_policy():
    pdf = AuditPDF("MFS-ITP-2025-001", "Access Control Policy")
    pdf.alias_nb_pages()
    pdf.add_title_page(
        "Corporate Access Control Policy",
        "Information Security Policy Series",
        "4.2",
        "January 15, 2025",
        "Sarah Chen, CISO",
        "James Morton, CTO",
    )

    pdf.add_page()
    pdf.section_header("1", "PURPOSE AND SCOPE")
    pdf.body_text(
        "This policy establishes the requirements and procedures for managing access to Meridian Financial Services' "
        "information systems and data assets. It applies to all employees, contractors, consultants, and third-party "
        "personnel who access company systems. This policy supports compliance with SOX Section 404, PCI DSS Requirement 7, "
        "and ISO 27001:2022 Annex A.9."
    )

    pdf.section_header("2", "ROLE-BASED ACCESS CONTROL (RBAC)")
    pdf.sub_header("2.1", "Access Control Framework")
    pdf.body_text(
        "All system access shall be granted based on the principle of least privilege and aligned with the "
        "employee's documented job function. Access roles are defined in the Enterprise Role Matrix (ERM) "
        "maintained by the Identity and Access Management (IAM) team."
    )
    pdf.body_text("The following role tiers are defined:")
    pdf.bullet("Tier 1 - Standard User: Read-only access to departmental applications and shared resources.")
    pdf.bullet("Tier 2 - Power User: Read/write access to departmental systems; cannot modify configurations.")
    pdf.bullet("Tier 3 - Application Administrator: Full application management within a defined scope.")
    pdf.bullet("Tier 4 - System Administrator: Infrastructure-level access; requires dual approval.")
    pdf.bullet("Tier 5 - Privileged/Emergency Access: Break-glass access with mandatory post-use review.")

    pdf.sub_header("2.2", "Segregation of Duties (SoD)")
    pdf.body_text(
        "Conflicting duties must be separated to prevent fraud and errors. The SoD matrix is enforced through "
        "automated controls in SAP GRC Access Control. Key segregation requirements include: no single individual "
        "may initiate and approve financial transactions, manage user accounts for systems they administer, or "
        "both develop and deploy code to production environments."
    )

    pdf.section_header("3", "MULTI-FACTOR AUTHENTICATION (MFA)")
    pdf.body_text(
        "Multi-factor authentication is mandatory for all access to systems classified as Critical or High "
        "in the asset inventory. MFA must combine at least two of the following factors:"
    )
    pdf.bullet("Something you know: Password or PIN meeting complexity requirements.")
    pdf.bullet("Something you have: Hardware token (YubiKey), soft token (Okta Verify), or smart card.")
    pdf.bullet("Something you are: Biometric authentication (fingerprint, facial recognition).")
    pdf.body_text(
        "MFA is required for: all remote/VPN access, privileged account sessions, access to financial applications "
        "(SAP ECC, Oracle HFM, BlackLine), email access from non-corporate devices, and administrative console access."
    )

    pdf.section_header("4", "PRIVILEGED ACCESS MANAGEMENT (PAM)")
    pdf.body_text(
        "Privileged accounts are managed through CyberArk Privileged Access Security. All privileged sessions "
        "are recorded and stored for a minimum of 12 months. Privileged credentials are automatically rotated "
        "every 24 hours and checked out through the PAM vault."
    )
    pdf.body_text(
        "Standing privileged access is prohibited for production financial systems. All privileged access "
        "must be requested through ServiceNow with documented business justification, approved by the system "
        "owner and the requestor's manager, and limited to a maximum session duration of 4 hours."
    )

    pdf.add_page()
    pdf.section_header("5", "ACCOUNT PROVISIONING AND DEPROVISIONING")
    pdf.sub_header("5.1", "Provisioning Workflow")
    pdf.body_text(
        "New account provisioning follows a standardized workflow integrated with the HR onboarding process:"
    )
    pdf.bullet("HR creates employee record in Workday, triggering automatic identity creation in Okta.")
    pdf.bullet("Manager submits access request via ServiceNow specifying required role(s) from the ERM.")
    pdf.bullet("IAM team validates request against role requirements and SoD rules within 4 business hours.")
    pdf.bullet("System owner approves access grant; accounts are provisioned automatically via SCIM.")
    pdf.bullet("Employee completes security awareness training within 5 business days of account activation.")

    pdf.sub_header("5.2", "Deprovisioning Workflow")
    pdf.body_text(
        "Account deprovisioning is triggered automatically upon HR termination processing. For voluntary "
        "terminations, access is revoked on the last day of employment by 6:00 PM local time. For involuntary "
        "terminations, access is revoked immediately upon HR notification. Contractor accounts are automatically "
        "disabled on the contract end date stored in the Vendor Management System."
    )

    pdf.section_header("6", "PASSWORD POLICY")
    pdf.body_text("The following password requirements apply to all user accounts:")
    pdf.add_table(
        ["Parameter", "Requirement"],
        [
            ["Minimum Length", "14 characters"],
            ["Complexity", "Upper, lower, number, special character"],
            ["Maximum Age", "90 days (standard), 60 days (privileged)"],
            ["History", "Last 24 passwords cannot be reused"],
            ["Lockout Threshold", "5 failed attempts"],
            ["Lockout Duration", "30 minutes (auto-unlock)"],
            ["Idle Session Timeout", "15 minutes"],
        ],
        col_widths=[80, 110],
    )

    pdf.section_header("7", "DOCUMENT CONTROL")
    pdf.body_text("This policy is reviewed annually or upon significant changes to the control environment.")
    pdf.signature_block("Sarah Chen, CISSP, CISM", "Chief Information Security Officer", "January 15, 2025")
    pdf.signature_block("James Morton", "Chief Technology Officer", "January 15, 2025")

    pdf.output(os.path.join(OUTPUT_DIR, "access_control_policy.pdf"))


def create_quarterly_access_review():
    pdf = AuditPDF("MFS-IAM-2025-Q4-001", "Quarterly Access Review Report")
    pdf.alias_nb_pages()
    pdf.add_title_page(
        "Quarterly User Access Review",
        "Q4 2025 (October - December)",
        "1.0",
        "January 10, 2026",
        "Michael Torres, IAM Manager",
        "Sarah Chen, CISO",
    )

    pdf.add_page()
    pdf.section_header("1", "EXECUTIVE SUMMARY")
    pdf.body_text(
        "This report documents the Q4 2025 quarterly user access review conducted in accordance with "
        "SOX ITGC Control AC-04 (Periodic Access Recertification). The review covered all in-scope financial "
        "applications and supporting infrastructure. A total of 1,247 user accounts across 8 systems were reviewed. "
        "The overall recertification completion rate was 98.7%, exceeding the 95% target."
    )

    pdf.section_header("2", "REVIEW SCOPE AND METHODOLOGY")
    pdf.body_text(
        "Access reviews were conducted using SailPoint IdentityNow. System owners and department managers "
        "certified the appropriateness of access for all users within their purview. Reviews were initiated "
        "on October 15, 2025, with a completion deadline of November 30, 2025."
    )

    pdf.section_header("3", "SYSTEMS REVIEWED")
    pdf.add_table(
        ["System", "Type", "SOX Relevant", "Total Users", "Reviewed", "Completion %"],
        [
            ["SAP ECC 6.0", "ERP", "Yes", "412", "412", "100.0%"],
            ["Oracle HFM", "Financial", "Yes", "87", "87", "100.0%"],
            ["BlackLine", "Reconciliation", "Yes", "63", "63", "100.0%"],
            ["Workday HCM", "HR/Payroll", "Yes", "298", "295", "99.0%"],
            ["ServiceNow", "ITSM", "Indirect", "189", "185", "97.9%"],
            ["Salesforce CRM", "CRM", "Indirect", "156", "152", "97.4%"],
            ["Active Directory", "Infrastructure", "Yes", "1,247", "1,231", "98.7%"],
            ["CyberArk PAM", "Security", "Yes", "42", "42", "100.0%"],
        ],
        col_widths=[32, 28, 22, 26, 26, 26],
    )

    pdf.section_header("4", "REVIEW RESULTS BY DEPARTMENT")
    pdf.add_table(
        ["Department", "Users", "Certified", "Revoked", "Modified", "Pending"],
        [
            ["Finance & Accounting", "187", "182", "3", "2", "0"],
            ["Information Technology", "156", "149", "4", "3", "0"],
            ["Human Resources", "98", "96", "1", "1", "0"],
            ["Operations", "234", "228", "2", "3", "1"],
            ["Sales & Marketing", "189", "183", "4", "2", "0"],
            ["Legal & Compliance", "67", "67", "0", "0", "0"],
            ["Executive Leadership", "34", "34", "0", "0", "0"],
            ["External/Contractors", "82", "75", "5", "1", "1"],
        ],
        col_widths=[38, 25, 25, 25, 25, 25],
    )

    pdf.add_page()
    pdf.section_header("5", "EXCEPTIONS AND REMEDIATION")
    pdf.body_text(
        "A total of 19 access revocations and 12 access modifications were identified during the review period. "
        "Key exceptions are summarized below:"
    )
    pdf.add_table(
        ["Exception ID", "System", "Description", "Severity", "Status"],
        [
            ["EX-2025-041", "SAP ECC", "Terminated contractor retained access", "High", "Remediated"],
            ["EX-2025-042", "Workday", "Role conflict: HR admin + payroll approver", "High", "Remediated"],
            ["EX-2025-043", "SAP ECC", "Excessive auth in FI module (2 users)", "Medium", "Remediated"],
            ["EX-2025-044", "ServiceNow", "4 stale accounts (>90 days inactive)", "Low", "Remediated"],
            ["EX-2025-045", "Salesforce", "API user with admin rights not reviewed", "Medium", "In Progress"],
        ],
        col_widths=[26, 24, 65, 22, 24],
    )

    pdf.body_text(
        "Exception EX-2025-041: A contractor whose engagement ended on September 12, 2025 retained SAP access "
        "until discovered during this review on October 22, 2025. Root cause analysis determined the Vendor "
        "Management System offboarding notification failed due to a workflow error. Corrective action: VMS-SAP "
        "integration was repaired and validated on November 5, 2025. Compensating control: audit log review "
        "confirmed no unauthorized transactions were executed during the gap period."
    )

    pdf.section_header("6", "SIGN-OFF")
    pdf.body_text(
        "We have reviewed the Q4 2025 access recertification results and confirm the review was conducted "
        "in accordance with Meridian Financial Services' Access Control Policy (MFS-ITP-2025-001) and SOX "
        "ITGC requirements."
    )
    pdf.signature_block("Michael Torres", "IAM Manager", "January 8, 2026")
    pdf.signature_block("Sarah Chen, CISSP, CISM", "Chief Information Security Officer", "January 9, 2026")
    pdf.signature_block("David Nakamura", "IT Director", "January 10, 2026")
    pdf.signature_block("Patricia Okonkwo, CPA", "Compliance Officer", "January 10, 2026")

    pdf.output(os.path.join(OUTPUT_DIR, "quarterly_access_review.pdf"))


def create_change_management():
    pdf = AuditPDF("MFS-ITP-2025-003", "Change Management Procedures")
    pdf.alias_nb_pages()
    pdf.add_title_page(
        "Change Management Policy\nand Procedures",
        "IT General Controls - Change Management",
        "3.1",
        "March 1, 2025",
        "Robert Kim, VP of IT Operations",
        "James Morton, CTO",
    )

    pdf.add_page()
    pdf.section_header("1", "PURPOSE")
    pdf.body_text(
        "This document defines the change management policy and procedures for all modifications to "
        "information systems, applications, databases, and infrastructure within the SOX compliance boundary. "
        "It ensures that changes are authorized, tested, and implemented in a controlled manner to minimize "
        "risk to financial reporting systems."
    )

    pdf.section_header("2", "CHANGE REQUEST WORKFLOW")
    pdf.sub_header("2.1", "Standard Change Process")
    pdf.body_text("All standard changes follow the five-phase lifecycle:")
    pdf.bullet(
        "Phase 1 - Request: Change requestor submits RFC via ServiceNow ITSM, including business justification, "
        "risk assessment, implementation plan, rollback plan, and testing requirements."
    )
    pdf.bullet(
        "Phase 2 - Review: Change Manager conducts initial triage within 2 business days. Technical review "
        "is performed by the relevant application/infrastructure team lead."
    )
    pdf.bullet(
        "Phase 3 - Approve: Changes to SOX-relevant systems require CAB approval. Non-SOX standard changes "
        "require Change Manager and system owner approval."
    )
    pdf.bullet(
        "Phase 4 - Implement: Changes are deployed during approved maintenance windows. Implementation must "
        "follow the approved plan. Developer who wrote the code cannot be the same person who deploys to production."
    )
    pdf.bullet(
        "Phase 5 - Verify: Post-implementation verification (PIV) is performed within 24 hours. Change "
        "requestor and system owner confirm successful implementation."
    )

    pdf.sub_header("2.2", "Change Classification Matrix")
    pdf.add_table(
        ["Category", "Risk Level", "Approval Required", "Lead Time", "CAB Review"],
        [
            ["Standard", "Low", "Change Manager", "5 business days", "No"],
            ["Normal", "Medium", "Change Manager + Owner", "10 business days", "Yes"],
            ["Major", "High", "CAB + VP IT + CTO", "20 business days", "Yes"],
            ["Emergency", "Critical", "Emergency CAB", "Immediate", "Retroactive"],
        ],
        col_widths=[28, 24, 46, 36, 28],
    )

    pdf.section_header("3", "CHANGE ADVISORY BOARD (CAB)")
    pdf.sub_header("3.1", "CAB Composition and Meetings")
    pdf.body_text(
        "The CAB meets every Tuesday at 2:00 PM ET. Standing members include: VP of IT Operations (Chair), "
        "Enterprise Architecture Lead, Information Security Representative, Database Administration Lead, "
        "Network Engineering Lead, Application Development Manager, QA/Testing Manager, and Business Representative "
        "(rotating by department)."
    )

    pdf.sub_header("3.2", "CAB Meeting Minutes - December 17, 2025")
    pdf.body_text("Meeting called to order at 2:00 PM ET by Robert Kim. Quorum established (7 of 8 members present).")
    pdf.body_text("Changes reviewed:")
    pdf.add_table(
        ["RFC #", "Description", "System", "Decision"],
        [
            ["CHG-2025-1847", "SAP kernel upgrade to 7.93", "SAP ECC", "Approved"],
            ["CHG-2025-1852", "Oracle HFM patch 12.2.1.4.300", "Oracle HFM", "Approved w/ conditions"],
            ["CHG-2025-1855", "Firewall rule update - segment 10.4.x", "Network", "Approved"],
            ["CHG-2025-1861", "BlackLine module configuration change", "BlackLine", "Deferred - needs testing"],
            ["CHG-2025-1863", "AD GPO update for MFA enrollment", "Active Directory", "Approved"],
        ],
        col_widths=[30, 65, 35, 42],
    )
    pdf.body_text(
        "Action item: CHG-2025-1852 approved on condition that Oracle provides confirmation of compatibility "
        "with current consolidation hierarchies. Owner: Lisa Patel. Due date: December 24, 2025. "
        "Meeting adjourned at 3:15 PM ET."
    )

    pdf.add_page()
    pdf.section_header("4", "EMERGENCY CHANGE PROCEDURES")
    pdf.body_text(
        "Emergency changes are permitted only when a critical system outage, security incident, or regulatory "
        "deadline requires immediate action. Emergency changes must be:"
    )
    pdf.bullet("Authorized verbally by the VP of IT Operations or designee (IT Director).")
    pdf.bullet("Documented in ServiceNow within 24 hours of implementation.")
    pdf.bullet("Reviewed by the Emergency CAB within 48 hours (retroactive approval).")
    pdf.bullet("Subject to full post-implementation review including root cause analysis.")
    pdf.body_text(
        "In Q4 2025, two emergency changes were processed: ECH-2025-007 (critical SAP transport for month-end "
        "close on October 31) and ECH-2025-008 (security patch for CVE-2025-44228, deployed November 15). "
        "Both received retroactive CAB approval and passed post-implementation review."
    )

    pdf.section_header("5", "SEGREGATION OF DUTIES IN CHANGE MANAGEMENT")
    pdf.body_text("The following segregation requirements are enforced through ServiceNow workflow controls:")
    pdf.add_table(
        ["Role", "Request", "Develop", "Test", "Approve", "Deploy"],
        [
            ["Developer", "Yes", "Yes", "No", "No", "No"],
            ["QA Tester", "No", "No", "Yes", "No", "No"],
            ["Change Manager", "No", "No", "No", "Yes", "No"],
            ["Release Manager", "No", "No", "No", "No", "Yes"],
            ["System Owner", "Yes", "No", "No", "Yes", "No"],
        ],
        col_widths=[35, 28, 28, 28, 28, 28],
    )

    pdf.section_header("6", "DOCUMENT CONTROL")
    pdf.signature_block("Robert Kim", "VP of IT Operations", "March 1, 2025")
    pdf.signature_block("James Morton", "Chief Technology Officer", "March 1, 2025")

    pdf.output(os.path.join(OUTPUT_DIR, "change_management_procedures.pdf"))


def create_vulnerability_scan():
    pdf = AuditPDF("MFS-SEC-2025-Q4-VSR", "Vulnerability Assessment Report")
    pdf.alias_nb_pages()
    pdf.add_title_page(
        "Vulnerability Assessment Report",
        "Q4 2025 Quarterly Scan Results",
        "1.0",
        "January 5, 2026",
        "Anika Sharma, Security Operations Manager",
        "Sarah Chen, CISO",
    )

    pdf.add_page()
    pdf.section_header("1", "EXECUTIVE SUMMARY")
    pdf.body_text(
        "This report presents the findings of the Q4 2025 vulnerability assessment conducted across Meridian "
        "Financial Services' in-scope network environment. The assessment was performed using Qualys VMDR and "
        "Tenable Nessus Professional during the period of December 1-15, 2025. A total of 2,847 assets were "
        "scanned, identifying 342 unique vulnerabilities. Overall risk posture has improved 18% compared to "
        "Q3 2025, with critical findings reduced from 12 to 4."
    )

    pdf.section_header("2", "SCAN SCOPE AND METHODOLOGY")
    pdf.body_text(
        "The assessment scope includes all assets within the SOX compliance boundary, including production "
        "servers, network devices, databases, and endpoint systems. Both authenticated and unauthenticated "
        "scans were performed."
    )
    pdf.add_table(
        ["Network Segment", "IP Range", "Assets Scanned", "Scan Type"],
        [
            ["Production - Financial Apps", "10.1.0.0/16", "187", "Authenticated"],
            ["Production - Database Tier", "10.2.0.0/16", "42", "Authenticated"],
            ["Production - Web Tier", "10.3.0.0/16", "96", "Authenticated"],
            ["Corporate Network", "10.4.0.0/16", "1,847", "Authenticated"],
            ["DMZ", "172.16.0.0/24", "23", "Auth + Unauth"],
            ["Management Network", "10.250.0.0/24", "48", "Authenticated"],
            ["Cloud (AWS)", "VPC-prod-01", "604", "Agent-based"],
        ],
        col_widths=[48, 38, 32, 38],
    )

    pdf.section_header("3", "FINDINGS SUMMARY")
    pdf.add_table(
        ["Severity", "Count", "% of Total", "Avg CVSS", "SLA (Days)", "Within SLA"],
        [
            ["Critical (CVSS 9.0-10.0)", "4", "1.2%", "9.4", "15", "75% (3/4)"],
            ["High (CVSS 7.0-8.9)", "28", "8.2%", "7.8", "30", "89% (25/28)"],
            ["Medium (CVSS 4.0-6.9)", "127", "37.1%", "5.3", "90", "94% (119/127)"],
            ["Low (CVSS 0.1-3.9)", "183", "53.5%", "2.1", "180", "98% (179/183)"],
        ],
        col_widths=[40, 18, 22, 22, 24, 30],
    )

    pdf.sub_header("3.1", "Critical Findings Detail")
    pdf.add_table(
        ["Finding ID", "CVE", "Description", "Affected Assets", "Status"],
        [
            ["VF-2025-0891", "CVE-2025-38421", "OpenSSL RCE in TLS 1.3", "3 web servers", "Patched"],
            ["VF-2025-0894", "CVE-2025-22109", "Linux kernel priv escalation", "12 servers", "Patched"],
            ["VF-2025-0897", "CVE-2025-44228", "Apache Struts deserialization", "2 app servers", "Patched"],
            ["VF-2025-0903", "CVE-2025-15732", "SQL Server buffer overflow", "1 DB server", "Scheduled"],
        ],
        col_widths=[28, 32, 50, 32, 26],
    )
    pdf.body_text(
        "VF-2025-0903 remediation is scheduled for the January 11, 2026 maintenance window. Compensating "
        "control: the affected database server has been isolated to a restricted VLAN with enhanced monitoring "
        "via CrowdStrike Falcon and additional firewall rules limiting access to the application tier only."
    )

    pdf.add_page()
    pdf.section_header("4", "REMEDIATION TIMELINE AND SLA COMPLIANCE")
    pdf.body_text(
        "Overall SLA compliance for Q4 2025 is 93.6%, exceeding the 90% target established in the Vulnerability "
        "Management Policy (MFS-ITP-2025-007). Remediation trends over the past four quarters:"
    )
    pdf.add_table(
        ["Quarter", "Total Findings", "Critical", "SLA Compliance", "Mean Time to Remediate"],
        [
            ["Q1 2025", "467", "9", "87.2%", "34 days"],
            ["Q2 2025", "401", "7", "89.5%", "28 days"],
            ["Q3 2025", "378", "12", "91.3%", "24 days"],
            ["Q4 2025", "342", "4", "93.6%", "19 days"],
        ],
        col_widths=[28, 32, 28, 34, 40],
    )

    pdf.section_header("5", "NETWORK SEGMENTATION VERIFICATION")
    pdf.body_text(
        "As part of the quarterly assessment, network segmentation controls were validated to ensure proper "
        "isolation between security zones. Testing confirmed:"
    )
    pdf.bullet("Production financial application tier is isolated from corporate network; no unauthorized paths detected.")
    pdf.bullet("Database tier accepts connections only from authorized application servers on designated ports.")
    pdf.bullet("Management network (OOBM) is accessible only via dedicated jump hosts with MFA enforcement.")
    pdf.bullet("DMZ systems cannot initiate connections to internal production networks.")
    pdf.bullet("PCI cardholder data environment (CDE) segmentation validated per PCI DSS Requirement 11.3.4.")

    pdf.body_text(
        "Segmentation test results: 48 of 48 test cases passed. No unauthorized network paths were identified. "
        "Full test results are documented in Appendix C of the detailed technical report (MFS-SEC-2025-Q4-VSR-TECH)."
    )

    pdf.section_header("6", "SIGN-OFF")
    pdf.signature_block("Anika Sharma", "Security Operations Manager", "January 3, 2026")
    pdf.signature_block("Sarah Chen, CISSP, CISM", "Chief Information Security Officer", "January 5, 2026")

    pdf.output(os.path.join(OUTPUT_DIR, "vulnerability_scan_report.pdf"))


def create_financial_close_checklist():
    pdf = AuditPDF("MFS-FIN-2025-12-MCL", "Month-End Close Checklist")
    pdf.alias_nb_pages()
    pdf.add_title_page(
        "Month-End Financial Close\nProcess Checklist",
        "December 2025 Close Period",
        "1.0",
        "January 8, 2026",
        "Angela Rossi, Corporate Controller",
        "William Chang, CFO",
    )

    pdf.add_page()
    pdf.section_header("1", "OVERVIEW")
    pdf.body_text(
        "This document records the completion status of all month-end close activities for the December 2025 "
        "reporting period (fiscal year-end). All tasks were performed in accordance with the Meridian Financial "
        "Services Close Calendar and SOX Section 302/404 management assertion requirements. The close was "
        "completed on January 6, 2026 (Business Day 3), within the 5 business day target."
    )

    pdf.section_header("2", "RECONCILIATION CHECKLIST")
    pdf.add_table(
        ["Step", "Task Description", "Owner", "Due Date", "Completed", "Status"],
        [
            ["1", "Cash and bank reconciliations", "T. Brooks", "Jan 2", "Jan 2", "Complete"],
            ["2", "Accounts receivable aging review", "M. Santos", "Jan 3", "Jan 3", "Complete"],
            ["3", "Accounts payable subledger recon", "K. Novak", "Jan 3", "Jan 2", "Complete"],
            ["4", "Fixed asset register reconciliation", "J. Liu", "Jan 3", "Jan 3", "Complete"],
            ["5", "Inventory valuation and reserves", "R. Patel", "Jan 3", "Jan 3", "Complete"],
            ["6", "Prepaid and accrual review", "T. Brooks", "Jan 4", "Jan 4", "Complete"],
            ["7", "Debt and interest reconciliation", "A. Rossi", "Jan 4", "Jan 3", "Complete"],
            ["8", "Revenue recognition review", "M. Santos", "Jan 4", "Jan 4", "Complete"],
            ["9", "Tax provision and deferred taxes", "C. Webb", "Jan 5", "Jan 5", "Complete"],
            ["10", "Equity and reserves reconciliation", "A. Rossi", "Jan 5", "Jan 5", "Complete"],
        ],
        col_widths=[12, 52, 26, 24, 24, 24],
    )

    pdf.section_header("3", "JOURNAL ENTRY APPROVAL WORKFLOW")
    pdf.body_text(
        "All manual journal entries are subject to the following approval requirements based on materiality "
        "thresholds defined in the Financial Controls Framework:"
    )
    pdf.add_table(
        ["Entry Amount", "Preparer", "First Approval", "Second Approval"],
        [
            ["< $50,000", "Staff Accountant", "Senior Accountant", "N/A"],
            ["$50,000 - $250,000", "Senior Accountant", "Accounting Manager", "N/A"],
            ["$250,000 - $1,000,000", "Accounting Manager", "Controller", "N/A"],
            ["> $1,000,000", "Controller", "VP Finance", "CFO"],
        ],
        col_widths=[38, 38, 38, 38],
    )
    pdf.body_text(
        "During the December 2025 close period, 847 journal entries were processed. Breakdown: 724 automated "
        "(system-generated), 98 standard recurring manual entries, and 25 non-standard manual entries. "
        "All entries received required approvals per the matrix above. BlackLine Journal Entry module "
        "enforced workflow controls with complete audit trail."
    )

    pdf.sub_header("3.1", "Non-Standard Journal Entry Summary")
    pdf.add_table(
        ["JE Reference", "Description", "Amount", "Preparer", "Approver(s)"],
        [
            ["JE-2025-12-NS01", "Year-end bonus accrual", "$2,340,000", "A. Rossi", "VP Finance, CFO"],
            ["JE-2025-12-NS02", "Goodwill impairment adj.", "$1,875,000", "A. Rossi", "VP Finance, CFO"],
            ["JE-2025-12-NS03", "Lease modification (ASC 842)", "$945,000", "J. Liu", "Controller"],
            ["JE-2025-12-NS04", "FX translation adjustment", "$712,000", "M. Santos", "Controller"],
            ["JE-2025-12-NS05", "Legal reserve increase", "$425,000", "C. Webb", "Controller"],
        ],
        col_widths=[32, 42, 30, 26, 38],
    )

    pdf.add_page()
    pdf.section_header("4", "INTERCOMPANY ELIMINATION PROCEDURES")
    pdf.body_text(
        "Intercompany transactions were reconciled and eliminated using Oracle HFM consolidation rules. "
        "A total of 14 legal entities were included in the consolidation. Intercompany imbalances exceeding "
        "the $5,000 threshold were investigated and resolved prior to close."
    )
    pdf.add_table(
        ["Entity Pair", "IC Balance", "Elimination Entry", "Variance", "Status"],
        [
            ["MFS Corp <> MFS Europe", "$12,450,000", "$12,450,000", "$0", "Cleared"],
            ["MFS Corp <> MFS Asia", "$8,732,000", "$8,732,000", "$0", "Cleared"],
            ["MFS Europe <> MFS UK", "$3,215,000", "$3,215,000", "$0", "Cleared"],
            ["MFS Corp <> MFS Canada", "$5,891,000", "$5,891,000", "$0", "Cleared"],
            ["MFS Asia <> MFS Japan", "$2,104,000", "$2,104,000", "$0", "Cleared"],
        ],
        col_widths=[40, 30, 36, 26, 26],
    )

    pdf.section_header("5", "MANAGEMENT REVIEW AND SIGN-OFF")
    pdf.body_text(
        "The undersigned have reviewed the December 2025 month-end close results and confirm that all "
        "reconciliations, journal entries, and consolidation procedures were completed in accordance with "
        "company policy and applicable accounting standards (US GAAP). Financial statements are materially "
        "accurate and ready for external audit review."
    )
    pdf.signature_block("Angela Rossi, CPA", "Corporate Controller", "January 6, 2026")
    pdf.signature_block("Daniel Okafor", "VP of Finance", "January 7, 2026")
    pdf.signature_block("William Chang, CPA, CFA", "Chief Financial Officer", "January 8, 2026")

    pdf.output(os.path.join(OUTPUT_DIR, "financial_close_checklist.pdf"))


def create_business_continuity_plan():
    pdf = AuditPDF("MFS-BCP-2025-001", "Business Continuity Plan")
    pdf.alias_nb_pages()
    pdf.add_title_page(
        "Business Continuity and\nDisaster Recovery Plan",
        "Enterprise Resilience Program",
        "5.0",
        "July 1, 2025",
        "Robert Kim, VP of IT Operations",
        "James Morton, CTO",
    )

    pdf.add_page()
    pdf.section_header("1", "PURPOSE AND SCOPE")
    pdf.body_text(
        "This Business Continuity Plan (BCP) and Disaster Recovery (DR) Plan establishes the procedures "
        "for maintaining and restoring critical business functions and IT services following a disruptive "
        "event. It covers all Tier 1 and Tier 2 business processes and their supporting technology "
        "infrastructure. This plan is tested semi-annually and updated annually."
    )

    pdf.section_header("2", "RECOVERY OBJECTIVES")
    pdf.sub_header("2.1", "Recovery Time Objectives (RTO) and Recovery Point Objectives (RPO)")
    pdf.add_table(
        ["System/Process", "Tier", "RTO", "RPO", "Recovery Method"],
        [
            ["SAP ECC (Financial)", "1", "4 hours", "1 hour", "Active-active cluster"],
            ["Oracle HFM", "1", "4 hours", "1 hour", "Warm standby (DR site)"],
            ["BlackLine", "1", "8 hours", "4 hours", "SaaS - vendor SLA"],
            ["Core Banking Platform", "1", "2 hours", "15 min", "Active-active (multi-region)"],
            ["Email (Exchange Online)", "2", "8 hours", "1 hour", "Microsoft SLA"],
            ["Active Directory", "1", "2 hours", "15 min", "Multi-site replication"],
            ["VPN/Remote Access", "2", "4 hours", "N/A", "Redundant concentrators"],
            ["Workday HCM", "2", "24 hours", "4 hours", "SaaS - vendor SLA"],
            ["File Services", "3", "48 hours", "24 hours", "Backup restore"],
        ],
        col_widths=[42, 14, 24, 22, 48],
    )

    pdf.section_header("3", "BACKUP PROCEDURES AND TESTING")
    pdf.sub_header("3.1", "Backup Schedule")
    pdf.add_table(
        ["Data Classification", "Backup Type", "Frequency", "Retention", "Storage Location"],
        [
            ["Tier 1 - Critical", "Continuous replication", "Real-time", "90 days", "DR site + AWS S3"],
            ["Tier 1 - Critical", "Full backup", "Daily", "1 year", "DR site + AWS S3"],
            ["Tier 2 - Important", "Incremental", "Daily", "90 days", "AWS S3 + Glacier"],
            ["Tier 2 - Important", "Full backup", "Weekly", "1 year", "AWS S3 + Glacier"],
            ["Tier 3 - Standard", "Full backup", "Weekly", "30 days", "AWS S3"],
        ],
        col_widths=[32, 36, 28, 24, 38],
    )

    pdf.sub_header("3.2", "Backup Verification")
    pdf.body_text(
        "Automated backup verification runs daily for all Tier 1 systems. Monthly restore tests are conducted "
        "on a rotating sample of backup sets. In Q4 2025, 12 restore tests were performed with a 100% success "
        "rate. Mean time to restore for Tier 1 database backups: 47 minutes (within 1-hour RPO target)."
    )

    pdf.add_page()
    pdf.section_header("4", "LAST DR TEST RESULTS")
    pdf.sub_header("4.1", "DR Test Summary - November 8-9, 2025")
    pdf.body_text(
        "A full-scale DR test was conducted on November 8-9, 2025 at the secondary data center in Ashburn, VA. "
        "The test simulated complete loss of the primary data center in Chicago, IL."
    )
    pdf.add_table(
        ["Test Scenario", "Target RTO", "Actual Recovery", "Result", "Notes"],
        [
            ["SAP ECC failover", "4 hours", "2 hr 47 min", "PASS", ""],
            ["Oracle HFM activation", "4 hours", "3 hr 12 min", "PASS", ""],
            ["AD/DNS failover", "2 hours", "0 hr 18 min", "PASS", "Automatic"],
            ["Core Banking failover", "2 hours", "1 hr 05 min", "PASS", ""],
            ["VPN reconvergence", "4 hours", "1 hr 32 min", "PASS", ""],
            ["End-to-end transaction", "4 hours", "3 hr 45 min", "PASS", "Close to limit"],
            ["Email continuity", "8 hours", "0 hr 03 min", "PASS", "Cloud-native"],
        ],
        col_widths=[36, 24, 32, 20, 40],
    )
    pdf.body_text(
        "Overall test result: PASS. All Tier 1 systems were recovered within their defined RTO/RPO targets. "
        "One observation was noted: the end-to-end transaction processing test completed at 3 hours 45 minutes, "
        "which is within the 4-hour target but leaves limited margin. Action item: optimize SAP transport "
        "activation sequence to reduce failover time by 30 minutes. Owner: SAP Basis team. Due: February 28, 2026."
    )

    pdf.section_header("5", "COMMUNICATION PLAN DURING INCIDENTS")
    pdf.sub_header("5.1", "Notification Tiers")
    pdf.add_table(
        ["Severity", "Notify Within", "Communication Channel", "Recipients"],
        [
            ["P1 - Critical", "15 minutes", "PagerDuty + Bridge call", "IT Leadership, CTO, CISO"],
            ["P2 - High", "30 minutes", "PagerDuty + Email", "IT Directors, System Owners"],
            ["P3 - Medium", "2 hours", "Email + Slack #incidents", "IT Managers, Affected Teams"],
            ["P4 - Low", "4 hours", "Slack #incidents", "Operations Team"],
        ],
        col_widths=[30, 28, 46, 50],
    )

    pdf.sub_header("5.2", "Stakeholder Communication")
    pdf.body_text(
        "During a declared disaster, the Crisis Communications Team coordinates all external communications. "
        "Regulatory notifications (SEC, FINRA) are issued within required timeframes. Customer communications "
        "are managed through the dedicated status page (status.meridianfs.com) and direct outreach for "
        "affected clients. The Board of Directors is briefed within 4 hours of a P1 incident declaration."
    )

    pdf.section_header("6", "DOCUMENT CONTROL")
    pdf.body_text(
        "This plan is reviewed and updated annually, or immediately following a significant incident, DR test, "
        "or material change to the technology environment. Next scheduled review: July 1, 2026."
    )
    pdf.signature_block("Robert Kim", "VP of IT Operations", "July 1, 2025")
    pdf.signature_block("James Morton", "Chief Technology Officer", "July 1, 2025")
    pdf.signature_block("Sarah Chen, CISSP, CISM", "Chief Information Security Officer", "July 1, 2025")

    pdf.output(os.path.join(OUTPUT_DIR, "business_continuity_plan.pdf"))


if __name__ == "__main__":
    print("Generating PDF evidence files...")
    create_access_control_policy()
    print("  [1/6] access_control_policy.pdf")
    create_quarterly_access_review()
    print("  [2/6] quarterly_access_review.pdf")
    create_change_management()
    print("  [3/6] change_management_procedures.pdf")
    create_vulnerability_scan()
    print("  [4/6] vulnerability_scan_report.pdf")
    create_financial_close_checklist()
    print("  [5/6] financial_close_checklist.pdf")
    create_business_continuity_plan()
    print("  [6/6] business_continuity_plan.pdf")
    print(f"\nAll files written to: {OUTPUT_DIR}")
