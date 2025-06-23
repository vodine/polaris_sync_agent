# get_token.py
import os
import requests
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
DEPLOYMENT = os.getenv("ACCELO_DEPLOYMENT", "polarisforensics.api.accelo.com")
SCOPE = os.getenv("ACCELO_SCOPE", "read(all)")

TOKEN_URL = f"https://{DEPLOYMENT}/oauth2/v0/token"

def get_service_token():
    """Fetches an access token using the service app's client credentials."""
    response = requests.post(TOKEN_URL, data={
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": SCOPE
    })

    if response.status_code != 200:
        raise Exception(f"Token request failed: {response.status_code}\n{response.text}")

    return response.json()["access_token"]

if __name__ == "__main__":
    token = get_service_token()
    print("✅ Service token acquired:")
    print(token)
