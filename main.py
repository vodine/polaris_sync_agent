import os
import argparse
from dotenv import load_dotenv
from accelo_scraper import login_and_save_cookies
from playwright.sync_api import sync_playwright
import json

load_dotenv()
LOGIN_PAGE = os.getenv("LOGIN_PAGE")

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

def main():
    parser = argparse.ArgumentParser(description="Accelo Sync Agent CLI")
    parser.add_argument("--sql_dump", action="store_true", help="Trigger full SQL export")
    args = parser.parse_args()

    if args.sql_dump:
        login_and_save_cookies()
        trigger_sql_export()

if __name__ == "__main__":
    main()