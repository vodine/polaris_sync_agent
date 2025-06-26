import os
import re
import json
import logging
import sqlparse
import csv
import io
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

def parse_insert_statements(file_path):
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if line.upper().startswith("INSERT INTO"):
                yield line.strip()

def extract_inserts(stmt):
    """Extract VALUES tuples using sqlparse, safe for huge blobs."""
    try:
        values_blob = stmt.split("VALUES", 1)[1].rsplit(";", 1)[0]
        parsed = sqlparse.parse("INSERT INTO dummy VALUES " + values_blob + ";")[0]
        tokens = parsed.tokens[-2].tokens if hasattr(parsed.tokens[-2], 'tokens') else []
        raw_rows = [str(token) for token in tokens if token.ttype is None and str(token).startswith("(")]
        return raw_rows
    except Exception as e:
        logging.error(f"Failed to extract inserts: {e}")
        return []

def robust_split_row(values_str):
    """Split SQL VALUES clause using csv reader logic to handle commas/quotes."""
    cleaned_str = values_str.strip().strip('()')
    cleaned_str = re.sub(r"''", "§§QUOTE§§", cleaned_str)
    cleaned_str = re.sub(r"'([^']*)'", r'"\1"', cleaned_str)
    cleaned_str = cleaned_str.replace("§§QUOTE§§", "'")
    try:
        return next(csv.reader(io.StringIO(cleaned_str), skipinitialspace=True))
    except Exception as e:
        logging.warning(f"Failed to split row: {e}")
        return []

def clean_and_cast_values(values_str, col_types):
    raw_values = robust_split_row(values_str)
    cleaned = []
    for val, typ in zip(raw_values, col_types):
        val = val.strip()
        try:
            if val.upper() == "NULL":
                cleaned.append(None)
            elif typ.lower() == "boolean":
                cleaned.append(val.lower() in ("1", "true", "t"))
            elif typ.lower() in ("integer", "bigint", "smallint"):
                cleaned.append(int(val))
            elif typ.lower() in ("real", "float", "decimal", "double precision"):
                cleaned.append(float(val))
            elif typ.lower() == "json" and val.startswith("{"):
                cleaned.append(json.loads(val.replace("'", '"')))
            else:
                cleaned.append(val)
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

        try:
            for stmt in parse_insert_statements(os.path.join(SQL_DUMP_DIR, filename)):
                match = re.match(r"INSERT INTO [`\"]?(\w+)[`\"]? \((.*?)\) VALUES", stmt, re.IGNORECASE)
                if not match:
                    continue

                col_str = match.group(2)
                insert_columns = [c.strip(' `"') for c in col_str.split(",")]

                insert_types = []
                for col in insert_columns:
                    col_def = next((c for c in col_defs if c["name"] == col), None)
                    insert_types.append(col_def["type"] if col_def else "text")

                row_texts = extract_inserts(stmt)
                for row_text in row_texts:
                    row = clean_and_cast_values(row_text, insert_types)
                    all_rows.append(row)

                if len(all_rows) >= 500:
                    success = insert_data_into_postgres(table, insert_columns, all_rows, insert_types, cursor)
                    if success:
                        total_inserted += len(all_rows)
                    all_rows.clear()

        except Exception as e:
            logging.error(f"Error while processing table {table}: {e}")
            failed_tables.append(table)

        if all_rows:
            success = insert_data_into_postgres(table, insert_columns, all_rows, insert_types, cursor)
            if success:
                total_inserted += len(all_rows)

        conn.commit()
        logging.info(f"Finished table: {table}")

    cursor.close()
    conn.close()

    logging.info("Import Complete")
    logging.info(f"Total rows inserted: {total_inserted}")
    if failed_tables:
        logging.warning(f"Tables with errors: {set(failed_tables)}")

if __name__ == "__main__":
    main()
