# accelo_client.py
import requests
from token_store import get_valid_access_token

BASE_URL = "https://polarisforensics.api.accelo.com/api/v0"

class AcceloClient:
    def __init__(self):
        self.token = get_valid_access_token()
        self.headers = {
            "Authorization": f"Bearer {self.token}"
        }

    def get(self, endpoint, params=None):
        url = f"{BASE_URL}/{endpoint.lstrip('/')}"
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def list_objects(self, module_name, limit=100):
        return self.get(f"{module_name}", params={"_limit": limit})

    def get_metadata(self):
        return self.get("meta")

    def get_table_sample(self, table_name, limit=5):
        return self.get(f"{table_name}", params={"_limit": limit})
