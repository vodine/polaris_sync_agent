# token_store.py
import os
import json
import time
import requests
from dotenv import load_dotenv

# Load .env into environment
load_dotenv()

# Read from environment
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")
TOKEN_FILE = os.getenv("TOKEN_FILE", "token_store.json")  # fallback if missing

# Accelo deployment host. Allows refreshing tokens against the correct server.
ACCELO_HOST = os.getenv("ACCELO_HOST", "polarisforensics.api.accelo.com")
AUTH_URL = f"https://{ACCELO_HOST}/oauth2/token"

def save_tokens(data):
    data["expires_at"] = int(time.time()) + int(data["expires_in"])
    with open(TOKEN_FILE, 'w') as f:
        json.dump(data, f)

def load_tokens():
    if not os.path.exists(TOKEN_FILE):
        return None
    with open(TOKEN_FILE, 'r') as f:
        return json.load(f)

def refresh_tokens():
    tokens = load_tokens()
    if not tokens or "refresh_token" not in tokens:
        raise Exception("No refresh token available. Authenticate manually.")

    resp = requests.post(AUTH_URL, data={
        "grant_type": "refresh_token",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": tokens["refresh_token"],
        "redirect_uri": REDIRECT_URI
    })

    if resp.status_code != 200:
        raise Exception(f"Failed to refresh tokens: {resp.text}")

    new_tokens = resp.json()
    save_tokens(new_tokens)
    return new_tokens

def get_valid_access_token():
    tokens = load_tokens()
    if not tokens or "access_token" not in tokens:
        raise Exception("No access token found. Please authenticate.")

    if int(time.time()) >= tokens["expires_at"]:
        tokens = refresh_tokens()

    return tokens["access_token"]
