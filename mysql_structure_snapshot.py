import json
import argparse
from datetime import datetime
from mysql_conn import get_mysql_connection
from pg_conn import get_pg_connection

VALID_BACKUP_TYPES = {"backup sql export", "daily update", "sync event"}

def extract_mysql_structure():
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = [row[0] for row in cursor.fetchall()]

    structure = {}
    for table in tables:
        cursor.execute(f"SHOW COLUMNS FROM `{table}`")
        columns = cursor.fetchall()
        structure[table] = [
            {
                'Field': col[0],
                'Type': col[1],
                'Null': col[2],
                'Key': col[3],
                'Default': col[4],
                'Extra': col[5]
            } for col in columns
        ]
    
    cursor.close()
    conn.close()
    return structure

def insert_snapshot_to_postgres(structure_dict, backup_type):
    conn = get_pg_connection()
    cur = conn.cursor()

    now = datetime.now()
    sql_backup_date = now if backup_type == "backup sql export" else None
    sync_event_dt = now if backup_type == "sync event" else None
    daily_update_dt = now if backup_type == "daily update" else None

    cur.execute("""
        INSERT INTO accelo_monitor.accelo_backups
        (backup_type, sql_backup_date, sync_event_date_time, daily_update_date_time, mysql_structure)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        backup_type,
        sql_backup_date,
        sync_event_dt,
        daily_update_dt,
        json.dumps(structure_dict)
    ))

    conn.commit()
    cur.close()
    conn.close()

def main():
    parser = argparse.ArgumentParser(description="Capture MySQL structure snapshot and log to PostgreSQL.")
    parser.add_argument('-t', '--type', required=True, choices=VALID_BACKUP_TYPES,
                        help="Type of backup event: 'backup sql export', 'daily update', or 'sync event'")
    args = parser.parse_args()

    structure = extract_mysql_structure()
    insert_snapshot_to_postgres(structure, backup_type=args.type)

if __name__ == "__main__":
    main()
