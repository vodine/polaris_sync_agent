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

# --- Helpers ---
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
    elif mysql_type in ["text", "mediumtext"]:
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
    return "TEXT"

def run_compare_script():
    subprocess.run(["python", COMPARE_SCRIPT], check=True)

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def get_table_diffs(report_path):
    diffs = {}
    current_table = None
    section = None
    with open(report_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.startswith("📦 Table: "):
                current_table = line.replace("📦 Table: ", "").strip().lower()
                diffs[current_table] = {"missing_columns": [], "type_mismatches": []}
            elif line.startswith("🚫 Missing Columns"):
                section = "missing"
            elif line.startswith("⚠️ Mismatched Types"):
                section = "mismatch"
            elif line.startswith("-"):
                if section == "missing":
                    diffs[current_table]["missing_columns"].append(line.strip("- "))
                elif section == "mismatch":
                    col = line.strip("- ").split(":")[0].strip()
                    diffs[current_table]["type_mismatches"].append(col)
    return diffs

# --- MAIN ---
def create_and_update_tables():
    run_compare_script()
    shutil.copyfile(SCHEMA_DIFF_REPORT_PATH, BACKUP_BEFORE)

    mysql_schema = {k.lower(): v for k, v in load_json(MYSQL_SCHEMA_PATH).items()}
    pg_schema = {k.lower(): v for k, v in load_json(PG_SCHEMA_PATH).items()}
    diffs = get_table_diffs(SCHEMA_DIFF_REPORT_PATH)

    conn = get_pg_connection()
    cursor = conn.cursor()

    created = []
    added_cols = []
    fixed_types = []
    failed = []

    with open(LOG_PATH, "w", encoding="utf-8") as log:
        # --- Create any fully missing tables ---
        missing_tables = [t for t in mysql_schema if t not in pg_schema]
        for table in tqdm(missing_tables, desc="Creating missing tables"):
            try:
                columns = mysql_schema.get(table, [])
                if not columns:
                    log.write(f"⚠️ Skipped empty definition: {table}\n")
                    continue
                col_defs = [f'"{col["name"]}" {mysql_to_pg_type(col["type"])}' for col in columns]
                sql = f'CREATE TABLE IF NOT EXISTS accelo_tap."{table}" (\n  ' + ",\n  ".join(col_defs) + "\n);"
                cursor.execute(sql)
                conn.commit()
                created.append(table)
                log.write(f"✅ Created table: {table}\n")
            except Exception as e:
                conn.rollback()
                failed.append((table, str(e)))
                log.write(f"❌ Failed to create table {table}: {e}\n")

        # --- Force-add all missing columns from MySQL schema ---
        for table, columns in tqdm(mysql_schema.items(), desc="Ensuring all columns"):
            for col in columns:
                col_name = col["name"]
                col_type = mysql_to_pg_type(col["type"])
                try:
                    alter_sql = f'ALTER TABLE accelo_tap."{table}" ADD COLUMN IF NOT EXISTS "{col_name}" {col_type};'
                    cursor.execute(alter_sql)
                    conn.commit()
                    added_cols.append(f"{table}.{col_name}")
                    log.write(f"🛠️ Ensured column: {table}.{col_name}\n")
                except Exception as e:
                    conn.rollback()
                    failed.append((table, col_name, str(e)))
                    log.write(f"❌ Failed to add column {table}.{col_name}: {e}\n")

        # --- Handle type mismatches explicitly ---
        for table, detail in tqdm(diffs.items(), desc="Fixing type mismatches"):
            for col_name in detail.get("type_mismatches", []):
                match = next((c for c in mysql_schema.get(table, []) if c["name"] == col_name), None)
                if not match:
                    log.write(f"⚠️ No definition found for type mismatch {col_name} in {table}\n")
                    continue
                pg_type = mysql_to_pg_type(match["type"])
                try:
                    alter_type_sql = f'ALTER TABLE accelo_tap."{table}" ALTER COLUMN "{col_name}" TYPE {pg_type};'
                    cursor.execute(alter_type_sql)
                    conn.commit()
                    fixed_types.append(f"{table}.{col_name}")
                    log.write(f"🔁 Fixed type: {table}.{col_name} to {pg_type}\n")
                except Exception as e:
                    conn.rollback()
                    failed.append((table, col_name, str(e)))
                    log.write(f"❌ Failed to alter type {table}.{col_name}: {e}\n")

    cursor.close()
    conn.close()

    run_compare_script()
    shutil.copyfile(SCHEMA_DIFF_REPORT_PATH, BACKUP_AFTER)

    print("\n✅ Done.")
    print(f"🆕 Tables created: {len(created)}")
    print(f"🛠️ Columns added: {len(added_cols)}")
    print(f"🔁 Types fixed: {len(fixed_types)}")
    print(f"❌ Failures: {len(failed)}")
    print(f"📄 See log: {LOG_PATH}")

# --- ENTRY ---
if __name__ == "__main__":
    create_and_update_tables()
