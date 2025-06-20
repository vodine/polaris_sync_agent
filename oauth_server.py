# oauth_server.py
import os
import json
import time
import requests
from dotenv import load_dotenv
from flask import Flask, request, redirect

# Load environment variables from a .env file if present. This mirrors the
# behavior of the other modules and ensures CLIENT_ID, CLIENT_SECRET and
# REDIRECT_URI are populated when running the server locally.
load_dotenv()

app = Flask(__name__)

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")
TOKEN_URL = "https://api.accelo.com/oauth2/token"
TOKEN_FILE = "auth/tokens.json"

def save_tokens(data):
    os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)
    data["expires_at"] = int(time.time()) + int(data["expires_in"])
    with open(TOKEN_FILE, "w") as f:
        json.dump(data, f, indent=2)

@app.route("/callback")
def callback():
    code = request.args.get("code")
    if not code:
        return "Missing authorization code", 400

    print(f"🔑 Received code: {code}")

    response = requests.post(TOKEN_URL, data={
        "grant_type": "authorization_code",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI,
        "code": code
    })

    if response.status_code == 200:
        tokens = response.json()
        save_tokens(tokens)
        print("✅ Access and refresh tokens saved.")
        return "✅ Authorization complete. You may close this tab."
    else:
        print("❌ Token exchange failed:", response.text)
        return f"Token exchange failed: {response.text}", 500

if __name__ == "__main__":
    print(f"🌐 Server running at {REDIRECT_URI}")
    print("🔗 Visit this URL in a browser:")
    print(f"https://api.accelo.com/oauth2/authorize?client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}&response_type=code&scope=read(all)+write(all)")
    app.run(host="0.0.0.0", port=8000)
