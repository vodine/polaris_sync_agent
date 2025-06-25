import json
import subprocess
import shutil
import psycopg2
from tqdm import tqdm
from pg_conn import get_pg_connection

# --- CONFIG ---
PG_SCHEMA_PATH = "C:/polaris_sync_agent/sync_tables/accelo_pg_schema.json"
MYSQL_SCHEMA_PATH = "C:/polaris_sync_agent/sync_tables/accelo_mysql_schema.json"
SCHEMA_DIFF_REPORT_PATH = "C:/polaris_sync_agent/sync_tables/schema_diff_report.txt"
BACKUP_BEFORE = "C:/polaris_sync_agent/sync_tables/schema_diff_report_before.txt"
BACKUP_AFTER = "C:/polaris_sync_agent/sync_tables/schema_diff_report_after.txt"
COMPARE_SCRIPT = "C:/polaris_sync_agent/sync_tables/compare_db.py"
LOG_PATH = "C:/polaris_sync_agent/sync_tables/create_missing_tables.log"

# --- TYPE MAPPING ---
def mysql_to_pg_type(mysql_type):
    mysql_type = mysql_type.lower()
    if mysql_type.startswith("int"):
        return "INTEGER"
    elif mysql_type.startswith("tinyint(1)"):
        return "BOOLEAN"
    elif mysql_type.startswith("bigint"):
        return "BIGINT"
    elif mysql_type.startswith("smallint"):
        return "SMALLINT"
    elif mysql_type.startswith("varchar"):
        return "VARCHAR"
    elif mysql_type.startswith("char"):
        return "CHAR"
    elif mysql_type == "text":
        return "TEXT"
    elif mysql_type.startswith("enum("):
        return "TEXT"
    elif mysql_type in ["datetime", "timestamp"]:
        return "TIMESTAMP"
    elif mysql_type.startswith("decimal"):
        return "DECIMAL"
    elif mysql_type.startswith("double"):
        return "DOUBLE PRECISION"
    elif mysql_type.startswith("float"):
        return "REAL"
    elif mysql_type.startswith("date"):
        return "DATE"
    else:
        return "TEXT"

# --- PARSE DIFF REPORT ---
def get_missing_tables_from_report(report_path):
    missing = []
    with open(report_path, "r", encoding="utf-8") as f:
        in_missing_section = False
        for line in f:
            line = line.strip()
            if "MISSING TABLES" in line:
                in_missing_section = True
                continue
            elif line.startswith("\U0001f4e9") or line.startswith("\U0001f4e6"):
                break
            elif in_missing_section and line.startswith("-"):
                missing.append(line.strip("- ").strip().lower())
    return missing

# --- RUN COMPARISON SCRIPT ---
def run_compare_script():
    subprocess.run(["python", COMPARE_SCRIPT], check=True)

# --- MAIN ---
def create_missing_tables():
    # Run comparison first and backup initial state
    run_compare_script()
    shutil.copyfile(SCHEMA_DIFF_REPORT_PATH, BACKUP_BEFORE)

    with open(MYSQL_SCHEMA_PATH, "r", encoding="utf-8") as f:
        mysql_schema_raw = json.load(f)

    mysql_schema = {k.lower(): v for k, v in mysql_schema_raw.items()}
    missing_tables = get_missing_tables_from_report(SCHEMA_DIFF_REPORT_PATH)

    conn = get_pg_connection()
    cursor = conn.cursor()

    created = []
    skipped = []
    failed = []

    with open(LOG_PATH, "w", encoding="utf-8") as log:
        for table in tqdm(missing_tables, desc="Creating missing tables"):
            if table not in mysql_schema:
                msg = f"❌ {table} not found in MySQL schema JSON."
                print(msg)
                log.write(msg + "\n")
                skipped.append(table)
                continue

            columns = mysql_schema[table]
            if not columns:
                msg = f"⚠️ Skipping empty table definition: {table}"
                print(msg)
                log.write(msg + "\n")
                skipped.append(table)
                continue

            try:
                col_defs = [f'"{col["name"]}" {mysql_to_pg_type(col["type"])}' for col in columns]
                sql = f'CREATE TABLE IF NOT EXISTS accelo_tap."{table}" (\n  ' + ",\n  ".join(col_defs) + "\n);"
                cursor.execute(sql)
                conn.commit()
                created.append(table)
            except Exception as e:
                msg = f"❌ Failed to create {table}: {e}"
                print(msg)
                log.write(msg + "\n")
                conn.rollback()
                failed.append(table)

    cursor.close()
    conn.close()

    # Run comparison again and take after snapshot
    run_compare_script()
    shutil.copyfile(SCHEMA_DIFF_REPORT_PATH, BACKUP_AFTER)

    # Check difference
    before = set(get_missing_tables_from_report(BACKUP_BEFORE))
    after = set(get_missing_tables_from_report(BACKUP_AFTER))
    resolved = sorted(before - after)

    with open(LOG_PATH, "a", encoding="utf-8") as log:
        log.write("\n=== TABLES RESOLVED ===\n")
        for table in resolved:
            log.write(f"✅ {table}\n")

    print("\n✅ Completed.")
    print(f"🆕 Tables created: {len(created)}")
    print(f"⚠️ Skipped: {len(skipped)}")
    print(f"❌ Failed: {len(failed)}")
    print(f"✅ Resolved tables: {len(resolved)}")
    print(f"📄 Log written to: {LOG_PATH}")

# --- ENTRY ---
if __name__ == "__main__":
    create_missing_tables()