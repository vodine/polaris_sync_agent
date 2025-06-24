import os
import json
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

OUTPUT_FILE = "accelo_pg_schema.json"
SCHEMA_NAME = "accelo_tap"

def get_pg_connection():
    return psycopg2.connect(
        dbname=os.getenv("DBNAME"),
        user=os.getenv("DBUSER"),
        password=os.getenv("DBPASSWORD"),
        host=os.getenv("DBHOST"),
        port=os.getenv("DBPORT")
    )

def get_schema_structure():
    conn = get_pg_connection()
    cursor = conn.cursor()

    # Fetch all table names
    cursor.execute(sql.SQL("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """), [SCHEMA_NAME])
    
    tables = cursor.fetchall()
    schema = {}

    for (table_name,) in tables:
        cursor.execute(sql.SQL("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """), [SCHEMA_NAME, table_name])
        
        columns = cursor.fetchall()
        schema[table_name] = [
            {"name": col[0], "type": col[1]} for col in columns
        ]

    conn.close()
    return schema

def export_schema():
    print(f"📦 Exporting schema for schema: {SCHEMA_NAME}")
    structure = get_schema_structure()
    with open(OUTPUT_FILE, "w") as f:
        json.dump(structure, f, indent=2)
    print(f"✅ Schema exported to {OUTPUT_FILE}")

if __name__ == "__main__":
    export_schema()
