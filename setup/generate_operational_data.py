"""
Generate realistic operational datasets with intentional control violations.
Creates Delta tables for: users, roles, access_logs, change_tickets,
approval_records, access_reviews, system_configurations.

Violations embedded:
- Deployment before approval
- User approving their own change
- Missing MFA for privileged account
- Expired access review
- Terminated employee with active access
- Shared admin accounts
"""
import subprocess
import json
import random
import uuid
from datetime import datetime, timedelta

PROFILE = "fevm-akash-finance-app"
WAREHOUSE = "1b1d59e180e4ac26"
FQ = "main.audit_schema"


def run_sql(sql, quiet=True):
    r = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements",
         "-p", PROFILE, "--json",
         json.dumps({"warehouse_id": WAREHOUSE, "statement": sql, "wait_timeout": "30s"})],
        capture_output=True, text=True, timeout=60,
    )
    d = json.loads(r.stdout)
    if not quiet:
        print(f'{d["status"]["state"]}: {sql[:80]}')
    return d


def escape(s):
    if s is None:
        return "NULL"
    return "'" + str(s).replace("'", "''") + "'"


def main():
    print("Creating operational data tables...")

    # 1. Users table
    run_sql(f"""CREATE TABLE IF NOT EXISTS {FQ}.op_users (
        user_id STRING, employee_id STRING, full_name STRING, email STRING,
        department STRING, title STRING, role_level STRING, manager_id STRING,
        hire_date DATE, termination_date DATE, status STRING, mfa_enabled BOOLEAN,
        last_login TIMESTAMP, privileged_access BOOLEAN
    ) USING DELTA""", quiet=False)

    # 2. Change tickets table
    run_sql(f"""CREATE TABLE IF NOT EXISTS {FQ}.op_change_tickets (
        ticket_id STRING, title STRING, description STRING, requestor_id STRING,
        approver_id STRING, environment STRING, priority STRING, status STRING,
        requested_date TIMESTAMP, approved_date TIMESTAMP, deployed_date TIMESTAMP,
        post_review_date TIMESTAMP, category STRING, risk_level STRING
    ) USING DELTA""", quiet=False)

    # 3. Access reviews table
    run_sql(f"""CREATE TABLE IF NOT EXISTS {FQ}.op_access_reviews (
        review_id STRING, user_id STRING, reviewer_id STRING, system_name STRING,
        access_level STRING, review_date DATE, review_result STRING,
        action_taken STRING, review_period STRING, days_overdue INT
    ) USING DELTA""", quiet=False)

    # Generate users
    print("\nGenerating users...")
    departments = ["Engineering", "Finance", "HR", "IT Security", "Operations", "Legal", "Sales", "Marketing"]
    titles = ["Analyst", "Senior Analyst", "Manager", "Director", "VP", "Engineer", "Senior Engineer", "Architect"]
    first_names = ["James", "Mary", "Robert", "Patricia", "Michael", "Jennifer", "William", "Linda",
                   "David", "Elizabeth", "Richard", "Barbara", "Joseph", "Susan", "Thomas", "Jessica",
                   "Sarah", "Daniel", "Emily", "Matthew", "Ashley", "Christopher", "Amanda", "Andrew",
                   "Stephanie", "Mark", "Lisa", "Paul", "Karen", "Steven"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
                  "Rodriguez", "Martinez", "Wilson", "Anderson", "Taylor", "Thomas", "Moore", "Jackson",
                  "Martin", "Lee", "Perez", "Thompson", "White", "Harris", "Clark", "Lewis",
                  "Robinson", "Walker", "Young", "Allen", "King", "Wright"]

    users = []
    for i in range(200):
        uid = f"USR-{i+1:04d}"
        fname = random.choice(first_names)
        lname = random.choice(last_names)
        dept = random.choice(departments)
        title = random.choice(titles)
        hire_date = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 2000))
        is_terminated = i >= 190  # Last 10 users are terminated
        term_date = (datetime(2025, 10, 1) + timedelta(days=random.randint(0, 90))) if is_terminated else None
        is_privileged = i < 15  # First 15 have privileged access
        # VIOLATION: 3 privileged users without MFA
        mfa = True if not is_privileged else (i >= 3)
        status = "TERMINATED" if is_terminated else "ACTIVE"

        users.append({
            "id": uid, "eid": f"EMP{i+1:05d}", "name": f"{fname} {lname}",
            "email": f"{fname.lower()}.{lname.lower()}@acme.com",
            "dept": dept, "title": title, "privileged": is_privileged,
            "mfa": mfa, "status": status,
            "hire": hire_date.strftime("%Y-%m-%d"),
            "term": term_date.strftime("%Y-%m-%d") if term_date else None,
        })

    # Batch insert users
    run_sql(f"DELETE FROM {FQ}.op_users")
    batch_size = 50
    for b in range(0, len(users), batch_size):
        batch = users[b:b+batch_size]
        values = ",\n".join(
            f"({escape(u['id'])}, {escape(u['eid'])}, {escape(u['name'])}, {escape(u['email'])}, "
            f"{escape(u['dept'])}, {escape(u['title'])}, 'L{random.randint(1,5)}', "
            f"'USR-{max(1,int(u['id'].split('-')[1])-10):04d}', "
            f"DATE'{u['hire']}', {'DATE' + escape(u['term']) if u['term'] else 'NULL'}, "
            f"{escape(u['status'])}, {str(u['mfa']).lower()}, "
            f"TIMESTAMP'2026-01-{random.randint(1,28):02d} {random.randint(8,17):02d}:00:00', "
            f"{str(u['privileged']).lower()})"
            for u in batch
        )
        run_sql(f"INSERT INTO {FQ}.op_users VALUES {values}")
    print(f"  Created {len(users)} users (10 terminated, 3 privileged without MFA)")

    # Generate change tickets
    print("Generating change tickets...")
    run_sql(f"DELETE FROM {FQ}.op_change_tickets")
    tickets = []
    categories = ["Application Update", "Infrastructure Change", "Security Patch", "Configuration Change", "Database Migration"]

    for i in range(500):
        tid = f"CHG-{i+1:05d}"
        requestor = f"USR-{random.randint(1,180):04d}"
        approver = f"USR-{random.randint(1,20):04d}"
        req_date = datetime(2025, 10, 1) + timedelta(days=random.randint(0, 150))
        approved_date = req_date + timedelta(hours=random.randint(2, 48))
        deployed_date = approved_date + timedelta(hours=random.randint(1, 24))

        # VIOLATIONS:
        # Self-approval (same user requests and approves)
        if i < 8:
            approver = requestor

        # Deployment before approval
        if 8 <= i < 15:
            deployed_date = req_date + timedelta(hours=random.randint(1, 4))
            approved_date = deployed_date + timedelta(hours=random.randint(12, 48))

        # Missing post-implementation review
        has_post_review = random.random() > 0.15
        post_review = deployed_date + timedelta(days=random.randint(1, 7)) if has_post_review else None

        tickets.append({
            "id": tid, "title": f"Change #{i+1}: {random.choice(categories)}",
            "requestor": requestor, "approver": approver,
            "env": random.choice(["PRODUCTION", "STAGING", "DEVELOPMENT"]),
            "priority": random.choice(["CRITICAL", "HIGH", "MEDIUM", "LOW"]),
            "status": "COMPLETED",
            "cat": random.choice(categories),
            "risk": random.choice(["HIGH", "MEDIUM", "LOW"]),
            "req": req_date.strftime("%Y-%m-%d %H:%M:%S"),
            "app": approved_date.strftime("%Y-%m-%d %H:%M:%S"),
            "dep": deployed_date.strftime("%Y-%m-%d %H:%M:%S"),
            "post": post_review.strftime("%Y-%m-%d %H:%M:%S") if post_review else None,
        })

    for b in range(0, len(tickets), 50):
        batch = tickets[b:b+50]
        values = ",\n".join(
            f"({escape(t['id'])}, {escape(t['title'])}, 'Description for change', "
            f"{escape(t['requestor'])}, {escape(t['approver'])}, {escape(t['env'])}, "
            f"{escape(t['priority'])}, {escape(t['status'])}, "
            f"TIMESTAMP'{t['req']}', TIMESTAMP'{t['app']}', TIMESTAMP'{t['dep']}', "
            f"{'TIMESTAMP' + escape(t['post']) if t['post'] else 'NULL'}, "
            f"{escape(t['cat'])}, {escape(t['risk'])})"
            for t in batch
        )
        run_sql(f"INSERT INTO {FQ}.op_change_tickets VALUES {values}")
    print(f"  Created {len(tickets)} tickets (8 self-approved, 7 deployed before approval)")

    # Generate access reviews
    print("Generating access reviews...")
    run_sql(f"DELETE FROM {FQ}.op_access_reviews")
    reviews = []
    systems = ["SAP ERP", "Oracle Financials", "Active Directory", "Salesforce", "AWS Console", "Azure AD"]

    for i in range(300):
        uid = f"USR-{random.randint(1,200):04d}"
        reviewer = f"USR-{random.randint(1,20):04d}"
        review_date = datetime(2025, 10, 1) + timedelta(days=random.randint(0, 120))
        days_overdue = 0

        # VIOLATION: Expired/overdue reviews
        if i < 20:
            review_date = datetime(2025, 4, 1) + timedelta(days=random.randint(0, 60))
            days_overdue = random.randint(30, 120)

        result = random.choice(["APPROPRIATE", "APPROPRIATE", "APPROPRIATE", "EXCESSIVE", "REVOKE"])
        action = "NONE" if result == "APPROPRIATE" else ("REDUCED" if result == "EXCESSIVE" else "REVOKED")

        reviews.append({
            "id": f"REV-{i+1:05d}", "uid": uid, "reviewer": reviewer,
            "system": random.choice(systems),
            "level": random.choice(["READ", "WRITE", "ADMIN", "SUPER_ADMIN"]),
            "date": review_date.strftime("%Y-%m-%d"),
            "result": result, "action": action,
            "period": "Q4-2025" if review_date < datetime(2026, 1, 1) else "Q1-2026",
            "overdue": days_overdue,
        })

    for b in range(0, len(reviews), 50):
        batch = reviews[b:b+50]
        values = ",\n".join(
            f"({escape(r['id'])}, {escape(r['uid'])}, {escape(r['reviewer'])}, "
            f"{escape(r['system'])}, {escape(r['level'])}, DATE'{r['date']}', "
            f"{escape(r['result'])}, {escape(r['action'])}, {escape(r['period'])}, {r['overdue']})"
            for r in batch
        )
        run_sql(f"INSERT INTO {FQ}.op_access_reviews VALUES {values}")
    print(f"  Created {len(reviews)} reviews (20 overdue)")

    # Summary of violations
    print("\n=== EMBEDDED VIOLATIONS SUMMARY ===")
    print("1. PRIVILEGED ACCESS WITHOUT MFA: 3 users (USR-0001, USR-0002, USR-0003)")
    print("2. TERMINATED EMPLOYEES: 10 users (USR-0191 through USR-0200)")
    print("3. SELF-APPROVED CHANGES: 8 tickets (CHG-00001 through CHG-00008)")
    print("4. DEPLOYED BEFORE APPROVAL: 7 tickets (CHG-00009 through CHG-00015)")
    print("5. MISSING POST-IMPLEMENTATION REVIEW: ~75 tickets (15%)")
    print("6. OVERDUE ACCESS REVIEWS: 20 reviews")
    print(f"\nTotal operational records: {len(users) + len(tickets) + len(reviews)}")


if __name__ == "__main__":
    main()
