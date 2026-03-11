"""
Sync Lakebase data to Databricks SQL (Delta Lake) tables.
The deployed app uses SQL Statement API fallback, so it reads from Delta tables.
"""
import subprocess
import json
import time
import psycopg2
import psycopg2.extras

LAKEBASE_INSTANCE = "akash-finance-app"
LAKEBASE_HOST = "instance-383773af-2ab5-4bfd-971d-9dba95011ab4.database.cloud.databricks.com"
LAKEBASE_DB = "audit_platform"
PROFILE = "fevm-akash-finance-app"
AUDIT_ID = "AUD-2026-001"
FQ = "main.audit_schema"


def get_lb_token():
    result = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/database/credentials",
         "-p", PROFILE, "--json",
         json.dumps({"request_id": "sync", "instance_names": [LAKEBASE_INSTANCE]})],
        capture_output=True, text=True, timeout=15,
    )
    return json.loads(result.stdout)["token"]


def get_lb_conn():
    return psycopg2.connect(
        host=LAKEBASE_HOST, port=5432, database=LAKEBASE_DB,
        user="akash.s@databricks.com", password=get_lb_token(), sslmode="require",
    )


def run_dbsql(sql):
    """Execute SQL on Databricks SQL warehouse."""
    result = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements",
         "-p", PROFILE, "--json",
         json.dumps({"warehouse_id": "1b1d59e180e4ac26", "statement": sql, "wait_timeout": "30s"})],
        capture_output=True, text=True, timeout=60,
    )
    resp = json.loads(result.stdout)
    state = resp.get("status", {}).get("state", "UNKNOWN")
    if state != "SUCCEEDED":
        err = resp.get("status", {}).get("error", {}).get("message", "Unknown error")
        print(f"  SQL Warning: {state} - {err[:100]}")
    return resp


def escape(s):
    if s is None:
        return "NULL"
    return "'" + str(s).replace("'", "''") + "'"


def main():
    conn = get_lb_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Sync audit_engagements
    cur.execute("SELECT * FROM audit_engagements")
    for row in cur.fetchall():
        sql = f"""MERGE INTO {FQ}.audit_engagements t
            USING (SELECT {escape(row['audit_id'])} AS audit_id) s ON t.audit_id = s.audit_id
            WHEN NOT MATCHED THEN INSERT VALUES (
                {escape(row['audit_id'])}, {escape(row['audit_name'])}, {escape(row['framework'])},
                {escape(row['client_name'])}, {escape(row['description'])}, {escape(row['status'])},
                {escape(row['created_by'])}, current_timestamp(), current_timestamp()
            )
            WHEN MATCHED THEN UPDATE SET status = {escape(row['status'])}, updated_at = current_timestamp()"""
        run_dbsql(sql)
    print("Synced audit_engagements")

    # Sync evaluation_results
    cur.execute("SELECT * FROM evaluation_results WHERE audit_id = %s", (AUDIT_ID,))
    results = cur.fetchall()
    # Clear and re-insert
    run_dbsql(f"DELETE FROM {FQ}.evaluation_results WHERE audit_id = '{AUDIT_ID}'")
    time.sleep(1)
    for row in results:
        doc_ids = row.get('matched_document_ids') or []
        doc_ids_sql = "ARRAY(" + ",".join(escape(d) for d in doc_ids) + ")" if doc_ids else "ARRAY()"
        sql = f"""INSERT INTO {FQ}.evaluation_results VALUES (
            {escape(str(row['evaluation_id']))}, {escape(str(row['control_id']))}, {escape(row['audit_id'])},
            {escape(row['ai_verdict'])}, {row['ai_confidence']},
            {escape(row['ai_reasoning'])}, {escape(row['evidence_summary'])},
            {doc_ids_sql}, ARRAY(),
            {escape(row.get('auditor_verdict'))}, {escape(row.get('auditor_notes'))},
            {escape(row.get('auditor_id'))}, {'current_timestamp()' if row.get('reviewed_at') else 'NULL'},
            {escape(row['model_used'])}, {escape(row['prompt_version'])},
            current_timestamp(), current_timestamp()
        )"""
        run_dbsql(sql)
    print(f"Synced {len(results)} evaluation_results")

    # Sync document_chunks
    cur.execute("SELECT * FROM document_chunks WHERE audit_id = %s", (AUDIT_ID,))
    chunks = cur.fetchall()
    run_dbsql(f"DELETE FROM {FQ}.document_chunks WHERE audit_id = '{AUDIT_ID}'")
    time.sleep(1)
    for row in chunks:
        sql = f"""INSERT INTO {FQ}.document_chunks
            (chunk_id, document_id, audit_id, chunk_index, chunk_text, token_count, _created_at)
            VALUES (
                {escape(str(row['chunk_id']))}, {escape(str(row['document_id']))}, {escape(row['audit_id'])},
                {row['chunk_index']}, {escape(row['chunk_text'])},
                {row.get('token_count') or 0}, current_timestamp()
            )"""
        run_dbsql(sql)
    print(f"Synced {len(chunks)} document_chunks")

    # Sync control_evidence_matches
    cur.execute("SELECT * FROM control_evidence_matches WHERE audit_id = %s", (AUDIT_ID,))
    matches = cur.fetchall()
    run_dbsql(f"DELETE FROM {FQ}.control_evidence_matches WHERE audit_id = '{AUDIT_ID}'")
    time.sleep(1)
    for row in matches:
        sql = f"""INSERT INTO {FQ}.control_evidence_matches VALUES (
            {escape(str(row['match_id']))}, {escape(str(row['control_id']))},
            {escape(str(row['chunk_id']))}, {escape(str(row['document_id']))},
            {escape(row['audit_id'])}, {row['similarity_score']}, {row['match_rank']},
            current_timestamp()
        )"""
        run_dbsql(sql)
    print(f"Synced {len(matches)} control_evidence_matches")

    # Sync audit_log
    cur.execute("SELECT * FROM audit_log WHERE audit_id = %s", (AUDIT_ID,))
    logs = cur.fetchall()
    run_dbsql(f"DELETE FROM {FQ}.audit_log WHERE audit_id = '{AUDIT_ID}'")
    time.sleep(1)
    for row in logs:
        sql = f"""INSERT INTO {FQ}.audit_log VALUES (
            {escape(str(row['log_id']))}, {escape(row['audit_id'])}, {escape(row['user_id'])},
            {escape(row['action'])}, {escape(row['entity_type'])}, {escape(row['entity_id'])},
            {escape(row['details'])}, current_timestamp()
        )"""
        run_dbsql(sql)
    print(f"Synced {len(logs)} audit_log entries")

    conn.close()
    print("\nAll data synced to Databricks SQL!")


if __name__ == "__main__":
    main()
