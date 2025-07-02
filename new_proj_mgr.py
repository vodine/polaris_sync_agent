# ---- IMPORTS ----

import requests
import json
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from auth_grant import get_service_token
from mysql_conn import get_mysql_connection

# ---- CONFIGURATION ----

load_dotenv()
last_modified = os.getenv("LAST_MODIFIED")
DEPLOYMENT = os.getenv("ACCELO_DEPLOYMENT")
print(f"Deployment: {DEPLOYMENT}")

ACCELO_ACCESS_TOKEN = get_service_token()
print(f"Access Token: {ACCELO_ACCESS_TOKEN}")

# ---- API REQUEST ----

ACCELO_BASE_URL = f"https://{DEPLOYMENT}/api/v0/"
headers = {
    "Authorization": f"Bearer {ACCELO_ACCESS_TOKEN}",
    "Accept": "application/json"
}

params = {
    "date_modified__gt": last_modified,
    "limit": 50,
    "start": 0
}
response = requests.get(f"{ACCELO_BASE_URL}jobs", headers=headers, params=params)

# ---- PROCESS RESPONSE ----

def update_env_last_modified(new_value, env_path=".env"):
    lines = []
    replaced = False

    with open(env_path, "r") as file:
        for line in file:
            if line.startswith("LAST_MODIFIED="):
                lines.append(f"LAST_MODIFIED={new_value}\n")
                replaced = True
            else:
                lines.append(line)

    if not replaced:
        lines.append(f"LAST_MODIFIED={new_value}\n")

    with open(env_path, "w") as file:
        file.writelines(lines)

    print(f"✅ LAST_MODIFIED updated to {new_value}")


# ---- MAIN EXECUTION ----
