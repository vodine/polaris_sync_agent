import os
from mysql_conn import get_mysql_connection

def import_sql_from_folder(folder_path):
    import glob

    sql_files = sorted(glob.glob(os.path.join(folder_path, "*.sql")))
    if not sql_files:
        print("[!] No SQL files found to import.")
        return False

    conn = get_mysql_connection()
    cursor = conn.cursor()
    total = len(sql_files)

    print(f"[+] Found {total} SQL files. Starting import...")

    for i, file_path in enumerate(sql_files, 1):
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            sql = f.read()

        print(f"  → Importing {os.path.basename(file_path)} ({i}/{total})... ", end="")
        try:
            cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
            for statement in sql.split(";"):
                statement = statement.strip()
                if statement:
                    cursor.execute(statement)
            conn.commit()
            print("Done")
        except Exception as e:
            print(f"Failed: {e}")
            conn.rollback()

    cursor.close()
    conn.close()
    return True
