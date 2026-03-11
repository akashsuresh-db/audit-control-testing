"""
Seed the Lakebase database with sample audit data for demonstration.
Run this script locally after creating the pgvector schema.
"""
import subprocess
import json
import uuid
import csv
import os
import psycopg2

LAKEBASE_INSTANCE = "akash-finance-app"
LAKEBASE_HOST = "instance-383773af-2ab5-4bfd-971d-9dba95011ab4.database.cloud.databricks.com"
LAKEBASE_DB = "audit_platform"
PROFILE = "fevm-akash-finance-app"

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

def main():
    conn = get_conn()
    cur = conn.cursor()

    # Create sample audit
    audit_id = "AUD-2026-001"
    cur.execute("""
        INSERT INTO audit_engagements (audit_id, audit_name, framework, client_name, description, status, created_by)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (audit_id) DO NOTHING
    """, (audit_id, "Q1 2026 SOX Compliance Review", "SOX", "Acme Corporation",
          "Annual SOX compliance audit covering IT general controls, financial reporting, and access management.", "CREATED", "admin@firm.com"))

    # Load controls from CSV
    csv_path = os.path.join(os.path.dirname(__file__), "..", "sample_test_data", "sox_controls.csv")
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            ctrl_id = str(uuid.uuid4())
            cur.execute("""
                INSERT INTO controls (control_id, audit_id, control_code, framework, control_title,
                    control_description, control_category, risk_level, frequency, control_owner,
                    uploaded_by, source_file)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'seed_script', 'sox_controls.csv')
            """, (ctrl_id, audit_id, row["control_code"], row["framework"], row["control_title"],
                  row["control_description"], row["control_category"], row["risk_level"],
                  row["frequency"], row["control_owner"]))

    # Load sample evidence text files
    evidence_dir = os.path.join(os.path.dirname(__file__), "..", "sample_test_data")
    for fname in os.listdir(evidence_dir):
        if fname.startswith("evidence_") and fname.endswith(".txt"):
            fpath = os.path.join(evidence_dir, fname)
            with open(fpath) as f:
                content = f.read()
            doc_id = str(uuid.uuid4())
            cur.execute("""
                INSERT INTO evidence_documents (document_id, audit_id, original_filename, file_type,
                    file_path, file_size_bytes, extracted_text, parse_status, uploaded_by)
                VALUES (%s, %s, %s, 'txt', %s, %s, %s, 'COMPLETED', 'seed_script')
            """, (doc_id, audit_id, fname, f"/seed/{fname}", len(content), content))

    conn.commit()
    print(f"Seeded audit {audit_id} with controls and evidence")

    # Count what we inserted
    cur.execute("SELECT COUNT(*) FROM controls WHERE audit_id = %s", (audit_id,))
    print(f"  Controls: {cur.fetchone()[0]}")
    cur.execute("SELECT COUNT(*) FROM evidence_documents WHERE audit_id = %s", (audit_id,))
    print(f"  Evidence documents: {cur.fetchone()[0]}")

    conn.close()

if __name__ == "__main__":
    main()
