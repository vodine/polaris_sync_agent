import os, time, re, requests
from msal import ConfidentialClientApplication
from dotenv import load_dotenv

load_dotenv()
TENANT_ID = os.getenv("TENANT_ID_WATCHDOG")
CLIENT_ID = os.getenv("CLIENT_ID_WATCHDOG")
CLIENT_SECRET = os.getenv("WATCHDOG_SECRET")
EMAIL_ADDRESS = os.getenv("WATCHDOG_EMAIL")
SAVE_FOLDER = os.getenv("SAVE_FOLDER")

def get_token():
    app = ConfidentialClientApplication(
        client_id=CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}",
        client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    return result["access_token"]

def find_latest_sql_email(token):
    endpoint = f"https://graph.microsoft.com/v1.0/users/{EMAIL_ADDRESS}/messages"
    params = {"$search": '"SQL Export Complete"', "$top": 5}
    headers = {
        "Authorization": f"Bearer {token}",
        "Prefer": 'outlook.body-content-type="text"',
    }
    response = requests.get(endpoint, headers=headers, params=params)
    response.raise_for_status()
    return response.json().get("value", [])

def extract_download_link(html):
    match = re.search(r'https://polarisforensics\.accelo\.com/redirect\?s3url=[^"]+', html)
    return match.group(0) if match else None

def download_sql(link, dump_dir=SAVE_FOLDER):
    os.makedirs(dump_dir, exist_ok=True)
    r = requests.get(link)
    r.raise_for_status()

    # Extract filename from Content-Disposition header
    content_disp = r.headers.get('Content-Disposition', '')
    match = re.search(r'filename="?([^";]+)"?', content_disp)
    if match:
        fname = match.group(1)
    else:
        # fallback if header not found
        fname = "accelo_download.zip"

    path = os.path.join(dump_dir, fname)
    with open(path, "wb") as f:
        f.write(r.content)
    
    print(f"SQL export saved: {path}")
    return path

def delete_email(token, msg_id):
    endpoint = f"https://graph.microsoft.com/v1.0/users/{EMAIL_ADDRESS}/messages/{msg_id}"
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.delete(endpoint, headers=headers)
    if r.status_code == 204:
        print(f"Deleted email {msg_id}")
    else:
        print(f"Failed to delete email: {r.text}")

def run_watchdog():
    print("Accelo watchdog started.")
    token = get_token()
    messages = find_latest_sql_email(token)

    for msg in messages:
        if (
            msg.get("from", {}).get("emailAddress", {}).get("address") == "noreply@accelo.com" and
            "SQL Export Complete" in msg.get("subject", "")
        ):
            msg_id = msg["id"]
            html = msg.get("body", {}).get("content", "")
            link = extract_download_link(html)
            if link:
                download_sql(link)
                delete_email(token, msg_id)
                print("Watchdog success: SQL export downloaded and email deleted.")
                return True
            else:
                print("Watchdog error: Link not found.")
                return False

    print("Watchdog error: No matching SQL export email found.")
    return False

if __name__ == "__main__":
    run_watchdog()
