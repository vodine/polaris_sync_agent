# oauth_server.py
import os
import json
import requests
from flask import Flask, request
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")
TOKEN_FILE = os.getenv("TOKEN_FILE", "token_store.json")

AUTH_BASE = "https://polarisforensics.accelo.com/oauth2"

app = Flask(__name__)

@app.route("/callback")
def oauth_callback():
    code = request.args.get("code")
    if not code:
        return "Missing code in callback.", 400

    # Exchange code for token
    token_url = f"{AUTH_BASE}/token"
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI
    }

    response = requests.post(token_url, data=data)
    token_data = response.json()

    if "access_token" in token_data:
        with open(TOKEN_FILE, "w") as f:
            json.dump(token_data, f, indent=2)
        return "✅ Access token saved!"
    else:
        return f"❌ Token error: {token_data}", 400

if __name__ == "__main__":
    print(f"➡️ Open this URL in a browser:")
    print(f"https://polarisforensics.accelo.com/oauth2/v0/authorize?client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}&response_type=code&scope=read write")
    app.run(port=8000)
