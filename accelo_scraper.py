from playwright.sync_api import sync_playwright
from dotenv import load_dotenv
import json
import os

load_dotenv()
LOGIN_PAGE = os.getenv("LOGIN_PAGE")
LOGIN = os.getenv("ACCELO_LOGIN")
PASSWORD = os.getenv("ACCELO_PASSWORD")

def login_and_save_cookies():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)  # Set to True once stable
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0 Safari/537.36"
        )
        page = context.new_page()

        # Go to login page
        page.goto(f"https://{LOGIN_PAGE}", wait_until="domcontentloaded")
      
        # Fill in login form
        page.fill("input#username", LOGIN)
        page.fill("input#password", PASSWORD)

        # Submit via Enter (form has no visible button)
        page.keyboard.press("Enter")

        # Wait for successful nav or dashboard
        page.wait_for_load_state("networkidle")
        
        # Save cookies
        cookies = context.cookies()
        with open("accelo_cookies.json", "w") as f:
            json.dump(cookies, f)

        print("Headless login complete. Cookies saved.")
        browser.close()

if __name__ == "__main__":
    login_and_save_cookies()
