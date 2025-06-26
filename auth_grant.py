import os
import requests
import logging
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# Load environment variables
load_dotenv()

# Environment Variables
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
DEPLOYMENT = os.getenv("ACCELO_DEPLOYMENT")
SCOPE = os.getenv("ACCELO_SCOPE", "read(all)")

# Token endpoint for service app
TOKEN_URL = f"https://{DEPLOYMENT}/oauth2/v0/token"

def get_service_token():
    response = requests.post(TOKEN_URL, data={
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": SCOPE
    })

    if response.status_code != 200:
        logging.error(f"Token request failed: {response.status_code} - {response.text}")
        raise RuntimeError("Failed to obtain access token.")

    token_data = response.json()
    logging.info("Service token successfully retrieved.")
    return token_data["access_token"]

def validate_token(token):
    """Optional connectivity check, not required for production flow."""
    api_url = f"https://{DEPLOYMENT}/api/v0/companies"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        logging.info("Token validation succeeded via /companies endpoint.")
    else:
        logging.warning(f"Token validation failed: {response.status_code} - {response.text}")

if __name__ == "__main__":
    try:
        token = get_service_token()
        # validate_token(token)  # Optional connectivity check
    except Exception as e:
        logging.exception("Token generation failed.")
