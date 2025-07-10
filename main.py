import time
import argparse
from accelo_scraper import login_and_save_cookies
from main_helper import trigger_sql_export
from sql_watchdog import run_watchdog

INTERVAL = 300

def main():
    parser = argparse.ArgumentParser(description="Accelo Sync Agent CLI")
    parser.add_argument("--sql_dump", action="store_true", help="Trigger full SQL export")
    args = parser.parse_args()

    if args.sql_dump:
        login_and_save_cookies()
        trigger_sql_export()
        success = False
        while not success:
            success = run_watchdog()
            if not success:
                time.sleep(INTERVAL)

if __name__ == "__main__":
    main()