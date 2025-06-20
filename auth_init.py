# auth_init.py
import os
import requests
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlencode, urlparse, parse_qs
from dotenv import load_dotenv
from token_store import save_tokens

load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")

AUTH_URL = "https://api.accelo.com/oauth2/authorize"
TOKEN_URL = "https://api.accelo.com/oauth2/token"

# Build the auth URL
params = {
    "client_id": CLIENT_ID,
    "redirect_uri": REDIRECT_URI,
    "response_type": "code",
    "scope": "read(all) write(all)",
}
auth_url = f"{AUTH_URL}?{urlencode(params)}"
print(f"🔗 Opening browser for OAuth consent:\n{auth_url}")
webbrowser.open(auth_url)

# HTTP server to catch redirect
class CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        auth_code = query.get("code", [None])[0]

        if not auth_code:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Missing ?code param in callback.")
            return

        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Auth successful. You can close this window.")

        # Exchange for tokens
        resp = requests.post(TOKEN_URL, data={
            "grant_type": "authorization_code",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "code": auth_code,
            "redirect_uri": REDIRECT_URI
        })

        if resp.status_code == 200:
            save_tokens(resp.json())
            print("✅ Token exchange successful. Tokens saved.")
        else:
            print(f"❌ Failed to exchange code:\n{resp.text}")

        # Shut down server
        import threading
        threading.Thread(target=self.server.shutdown).start()

# Run local server
print("📡 Listening on http://localhost:8000/callback ...")
server = HTTPServer(('', 8000), CallbackHandler)
server.serve_forever()
