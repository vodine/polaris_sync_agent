import os
import glob
import subprocess
from dotenv import load_dotenv

load_dotenv()
DBUSER = os.getenv("MYSQL_USER")
DBPASS = os.getenv("MYSQL_PASSWORD")
DBNAME = os.getenv("MYSQL_DATABASE")

def import_sql_from_folder(folder_path):
    sql_files = sorted(glob.glob(os.path.join(folder_path, "*.sql")))
    if not sql_files:
        print("[!] No SQL files found to import.")
        return False

    total = len(sql_files)
    print(f"[+] Found {total} SQL files. Starting import...")

    for i, file_path in enumerate(sql_files, 1):
        print(f"  → Importing {os.path.basename(file_path)} ({i}/{total})... ", end="")
        try:
            result = subprocess.run(
                ["mysql", f"-u{DBUSER}", f"-p{DBPASS}", DBNAME, "-e", f"source {file_path}"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                shell=False
            )
            if result.returncode == 0:
                print("Done")
            else:
                print(f"Failed:\n{result.stderr}")
        except Exception as e:
            print(f"Exception: {e}")

    return True
