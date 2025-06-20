# pg_conn.py
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()  # Load .env file

def get_pg_connection():
    conn = psycopg2.connect(
        dbname=os.getenv("DBNAME"),
        user=os.getenv("DBUSER"),
        password=os.getenv("DBPASSWORD"),
        host=os.getenv("DBHOST"),
        port=os.getenv("DBPORT")
    )
    return conn
