import os
import json
import re

SQL_FOLDER = r"C:\polaris_migration\sql_scripts\polarisforensics.accelo.com-2025-06-16T23_11_46"
OUTPUT_JSON = "accelo_mysql_schema.json"

def extract_columns(column_block):
    lines = column_block.strip().splitlines()
    cleaned_lines = []
    buffer = ""
    
    for line in lines:
        line = line.strip()
        if not line or line.startswith('--'):
            continue
        buffer += " " + line
        if line.endswith(",") or line.endswith(")"):
            cleaned_lines.append(buffer.strip().rstrip(','))
            buffer = ""
    
    columns = []
    for line in cleaned_lines:
        if line.upper().startswith(("PRIMARY KEY", "KEY", "UNIQUE", "CONSTRAINT", "INDEX", "FULLTEXT")):
            continue

        col_match = re.match(r"`(?P<name>\w+)`\s+(?P<type>.+)", line)
        if col_match:
            col_name = col_match.group("name")
            col_type = col_match.group("type").split()[0]  # just the type, drop NOT NULL etc.
            columns.append({
                "name": col_name,
                "type": col_type.lower()
            })
    return columns

def parse_sql_file(filepath):
    with open(filepath, encoding='utf-8', errors='ignore') as f:
        content = f.read()

    match = re.search(r"CREATE TABLE\s+`?(\w+)`?\s*\((.*?)\)\s*(ENGINE|DEFAULT|CHARSET)", content, re.DOTALL | re.IGNORECASE)
    if not match:
        return None

    table_name = match.group(1)
    column_block = match.group(2)
    columns = extract_columns(column_block)
    return table_name, columns

def parse_all_sql_files(folder):
    schema = {}
    for filename in os.listdir(folder):
        if not filename.endswith(".sql"):
            continue
        full_path = os.path.join(folder, filename)
        parsed = parse_sql_file(full_path)
        if parsed:
            table_name, columns = parsed
            schema[table_name] = columns
    return schema

def export_schema_json(schema):
    with open(OUTPUT_JSON, "w") as f:
        json.dump(schema, f, indent=2)
    print(f"✅ Exported schema to {OUTPUT_JSON}")

if __name__ == "__main__":
    schema = parse_all_sql_files(SQL_FOLDER)
    export_schema_json(schema)
