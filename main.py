import time
import argparse
from accelo_scraper import login_and_save_cookies
from main_helper import trigger_sql_export, unzip_latest_sql, run_clean_up
from sql_watchdog import run_watchdog

INTERVAL = 300

def wait_for_sql_and_extract():
    success = False
    while not success:
        success = run_watchdog()
        if not success:
            time.sleep(INTERVAL)
    if success:
        unzip_latest_sql()
        run_clean_up()

def main():
    parser = argparse.ArgumentParser(description="Accelo Sync Agent CLI")
    parser.add_argument("--sql_dump", action="store_true", help="Trigger full SQL export")
    parser.add_argument("--email_side", action="store_true", help="Start from email side")
    parser.add_argument("--unzip", action="store_true", help="Unzip latest SQL export")
    parser.add_argument("--cleanup", action="store_true", help="Run cleanup and SQL insert")
    args = parser.parse_args()

    if args.sql_dump:
        login_and_save_cookies()
        trigger_sql_export()
        wait_for_sql_and_extract()
    
    elif args.email_side:
        wait_for_sql_and_extract()
        
    elif args.unzip:
        unzip_latest_sql()
        run_clean_up()
        
    elif args.cleanup:
        run_clean_up()
                
if __name__ == "__main__":
    main()