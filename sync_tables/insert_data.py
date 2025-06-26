import os
import re
import json
import logging
import sqlparse
import psycopg2
from psycopg2.extras import execute_values
from pg_conn import get_pg_connection
from tqdm import tqdm

# --- CONFIG ---
MYSQL_SCHEMA_PATH = "C:/polaris_sync_agent/sync_tables/accelo_mysql_schema.json"
PG_SCHEMA_PATH = "C:/polaris_sync_agent/sync_tables/accelo_pg_schema.json"
SQL_DUMP_DIR = r"C:/polaris_sync_agent/sync_tables/polarisforensics.accelo.com-2025-06-16T23_11_46"
LOG_FILE = "C:/polaris_sync_agent/data_import.log"

# --- LOGGER ---
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

# --- HELPERS ---
def load_schema(path):
    with open(path, "r", encoding="utf-8") as f:
        return {k.lower(): v for k, v in json.load(f).items()}

def extract_inserts(stmt):
    """Split the VALUES section into row tuples using sqlparse."""
    try:
        values_section = stmt.split("VALUES", 1)[1].rsplit(";", 1)[0].strip()
        parsed = sqlparse.parse("INSERT INTO dummy VALUES " + values_section + ";")[0]
        tokens = parsed.tokens[-2].tokens if hasattr(parsed.tokens[-2], 'tokens') else []
        raw_rows = []
        for token in tokens:
            if token.ttype is None and str(token).startswith("("):
                raw_rows.append(str(token))
        return raw_rows
    except Exception as e:
        logging.error(f"Failed to extract inserts: {e}")
        return []

def clean_and_cast_values(values_str, col_types):
    raw_values = re.split(r",(?![^()]*'\))", values_str.strip("()"))  # handles commas inside quotes
    cleaned = []
    for val, typ in zip(raw_values, col_types):
        val = val.strip()
        try:
            if val.upper() == "NULL":
                cleaned.append(None)
            elif val.startswith("'") and val.endswith("'"):
                cleaned.append(val[1:-1].replace("''", "'"))
            elif typ.lower() == "boolean":
                cleaned.append(val in ("1", "true", "TRUE", "t", "T"))
            elif typ.lower() in ("integer", "bigint", "smallint"):
                cleaned.append(int(val))
            elif typ.lower() in ("real", "float", "decimal", "double precision"):
                cleaned.append(float(val))
            else:
                cleaned.append(val.strip("'").replace("''", "'"))
        except Exception as e:
            logging.warning(f"Type coercion failed for value '{val}' as {typ}: {e}")
            cleaned.append(None)
    return cleaned

def insert_data_into_postgres(table, columns, values, pg_types, cursor):
    cols = ', '.join(f'"{c}"' for c in columns)
    sql = f'INSERT INTO accelo_tap."{table}" ({cols}) VALUES %s'
    try:
        execute_values(cursor, sql, values)
        return True
    except Exception as e:
        logging.error(f"Insert failed for {table}: {e}")
        return False

# --- MAIN ---
def main():
    mysql_schema = load_schema(MYSQL_SCHEMA_PATH)
    pg_schema = load_schema(PG_SCHEMA_PATH)
    conn = get_pg_connection()
    cursor = conn.cursor()

    total_inserted = 0
    failed_tables = []

    sql_files = [f for f in os.listdir(SQL_DUMP_DIR) if f.endswith(".sql")]

    for filename in tqdm(sql_files, desc="Importing SQL files"):
        table = filename.replace(".sql", "").lower()
        if table not in mysql_schema or table not in pg_schema:
            logging.warning(f"Skipping unknown or mismatched table: {table}")
            continue

        logging.info(f"Processing table: {table}")
        col_defs = pg_schema[table]
        all_rows = []

        file_path = os.path.join(SQL_DUMP_DIR, filename)
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()

        insert_statements = re.findall(r"INSERT INTO .*?VALUES\s*\(.*?\);", content, re.DOTALL | re.IGNORECASE)
        for stmt in insert_statements:
            match = re.match(r"INSERT INTO [`\"]?(\w+)[`\"]?(?: \((.*?)\))? VALUES", stmt, re.IGNORECASE)
            if not match:
                continue

            col_str = match.group(2)
            if col_str:
                insert_columns = [c.strip(' `"') for c in col_str.split(",")]
            else:
                insert_columns = [col["name"] for col in col_defs]

            insert_types = []
            for col in insert_columns:
                match = next((c for c in col_defs if c["name"] == col), None)
                if match:
                    insert_types.append(match["type"])
                else:
                    logging.warning(f"Column {col} not found in PG schema for {table}")
                    insert_types.append("text")  # fallback

            row_texts = extract_inserts(stmt)
            for row_text in row_texts:
                row = clean_and_cast_values(row_text, insert_types)
                all_rows.append(row)

            if len(all_rows) >= 500:
                success = insert_data_into_postgres(table, insert_columns, all_rows, insert_types, cursor)
                if success:
                    total_inserted += len(all_rows)
                all_rows.clear()

        if all_rows:
            success = insert_data_into_postgres(table, insert_columns, all_rows, insert_types, cursor)
            if success:
                total_inserted += len(all_rows)

        conn.commit()
        logging.info(f"✅ Finished table: {table}")

    cursor.close()
    conn.close()

    logging.info("🎉 Import Complete")
    logging.info(f"📊 Total rows inserted: {total_inserted}")
    if failed_tables:
        logging.warning(f"❌ Tables with errors: {set(failed_tables)}")

if __name__ == "__main__":
    main()
