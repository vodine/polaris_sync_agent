import os
import mysql.connector
from mysql.connector import Error

# CONFIG
SQL_DIR = r"C:/polaris_sync_agent/sql_parts"
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "1qa@WS3ed!QA2ws#ED",
    "database": "accelo_dev",
}

def execute_sql_file(cursor, file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        sql_script = f.read()

    # Naive split by semicolon. Works if no compound blocks.
    statements = [stmt.strip() for stmt in sql_script.split(';') if stmt.strip()]

    for stmt in statements:
        try:
            cursor.execute(stmt)
        except Error as e:
            print(f"❌ SQL Error: {e}\n  In: {stmt[:100]}...")
            with open("sql_import_errors.log", "a", encoding='utf-8') as log:
                log.write(f"{file_path} -> {e}\n  SQL: {stmt[:300]}\n\n")

def main():
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()  # ✅ Default CMySQLCursor
        print(f"✅ Connected to MySQL at {MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}")

        for filename in sorted(os.listdir(SQL_DIR)):
            if filename.endswith(".sql"):
                full_path = os.path.join(SQL_DIR, filename)
                print(f"⏳ Executing: {filename}")
                try:
                    execute_sql_file(cursor, full_path)
                    conn.commit()
                    print(f"✅ Done: {filename}")
                except Error as e:
                    print(f"❌ File Error: {e}")
        
        cursor.close()
        conn.close()
        print("🏁 All SQL files processed.")

    except Error as conn_err:
        print(f"❌ Connection error: {conn_err}")

if __name__ == "__main__":
    main()
