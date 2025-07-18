import os
import json
import time
import zipfile
import subprocess
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from import_sql_dump import import_sql_from_folder


load_dotenv()
LOGIN_PAGE = os.getenv("LOGIN_PAGE")
SAVE_FOLDER = os.getenv("SAVE_FOLDER")
ZIP_PATH = os.path.join(SAVE_FOLDER, "accelo_download.zip")
BAT_FILE = os.getenv("BAT_FILE")

def trigger_sql_export():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        # Load cookies from saved session
        with open("accelo_cookies.json", "r") as f:
            cookies = json.load(f)
        context.add_cookies(cookies)

        # Navigate directly to SQL Export page
        page.goto(f"https://{LOGIN_PAGE}/?action=export_sql&target=export", wait_until="domcontentloaded")

        # Select "Entire Database" and export
        page.check("input[name='all']")
        page.click("button:has-text('Export')")

        print("SQL export triggered.")
        browser.close()

def unzip_latest_sql():
    if not os.path.exists(ZIP_PATH):
        print(f"[!] File not found: {ZIP_PATH}")
        return None

    extract_path = os.path.splitext(ZIP_PATH)[0]

    try:
        with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
            temp_extract_path = extract_path + "_temp"
            zip_ref.extractall(temp_extract_path)

        # Look for first subdirectory
        subdirs = [os.path.join(temp_extract_path, d) for d in os.listdir(temp_extract_path) if os.path.isdir(os.path.join(temp_extract_path, d))]
        if not subdirs:
            print("No subdirectory found inside the zip.")
            return None

        inner_folder = subdirs[0]

        # Move files from inner_folder to extract_path
        if not os.path.exists(extract_path):
            os.makedirs(extract_path)

        for filename in os.listdir(inner_folder):
            src_file = os.path.join(inner_folder, filename)
            dst_file = os.path.join(extract_path, filename)
            os.rename(src_file, dst_file)

        # Clean up temp directory
        for root, dirs, files in os.walk(temp_extract_path, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(temp_extract_path)

        print(f"Unzipped to: {extract_path}")
        return extract_path
    except zipfile.BadZipFile:
        print("Failed: Bad zip file.")
        return None


def run_clean_up(max_retries=12, wait_seconds=300):
    extract_path = os.path.splitext(ZIP_PATH)[0]

    print("Starting cleanup and SQL insert...")
    retries = 0

    while retries < max_retries:
        success = import_sql_from_folder(extract_path)
        if success:
            print("SQL insert completed successfully.")
            # proceed with cleanup
            try:
                os.remove(ZIP_PATH)
                print(f"Deleted ZIP: {ZIP_PATH}")
                if os.path.exists(extract_path):
                    for root, dirs, files in os.walk(extract_path, topdown=False):
                        for name in files:
                            os.remove(os.path.join(root, name))
                        for name in dirs:
                            os.rmdir(os.path.join(root, name))
                    os.rmdir(extract_path)
                    print(f"Deleted extracted folder: {extract_path}")
            except Exception as e:
                print(f"Cleanup failed: {e}")
            return True
        else:
            print(f"Insert failed (attempt {retries + 1}/{max_retries}). Retrying in {wait_seconds} sec...")
            retries += 1
            time.sleep(wait_seconds)

    print("Max retries exceeded. Manual cleanup may be required.")
    return False
        
    