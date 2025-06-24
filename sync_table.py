# sync_table.py
import os
import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from get_token import get_service_token
from pg_conn import get_pg_connection

load_dotenv()

API_BASE = os.getenv("API_BASE", "https://polarisforensics.api.accelo.com/api/v0")

def fetch_table_data(table_name, token, last_sync_time=None):
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{API_BASE}/{table_name}"
    page = 0
    per_page = 100
    results = []

    while True:
        params = {"_limit": per_page, "_page": page}
        if last_sync_time:
            params["updated_since"] = last_sync_time

        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch {table_name}: {response.text}")

        data = response.json().get("response", [])
        if not data:
            break

        results.extend(data)
        page += 1

    return results

def upsert_postgres(table_name, records):
    if not records:
        print(f"⚠️ No records to sync for {table_name}")
        return

    keys = records[0].keys()
    columns = ",".join(keys)
    values = [[r[k] for k in keys] for r in records]

    query = f"""
        INSERT INTO {table_name} ({columns})
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
        {','.join([f"{k}=EXCLUDED.{k}" for k in keys if k != 'id'])}
    """

    conn = get_pg_connection()
    with conn.cursor() as cur:
        execute_values(cur, query, values)
    conn.commit()
    print(f"✅ Synced {len(records)} rows to {table_name}")

def sync_table(table_name, last_sync_time=None):
    print(f"🔄 Syncing table: {table_name}")
    token = get_service_token()
    records = fetch_table_data(table_name, token, last_sync_time)
    upsert_postgres(table_name, records)

if __name__ == "__main__":
    import sys
    table = sys.argv[1] if len(sys.argv) > 1 else "company"
    sync_table(table)
