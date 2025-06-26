import os
import json
import re
from tqdm import tqdm

SQL_FOLDER = r"C:/polaris_sync_agent/sql_parts"
OUTPUT_JSON = "accelo_mysql_schema.json"

def extract_columns(column_block):
    columns = []

    # Match every line that starts with a column definition using backticks
    col_defs = re.findall(r"^\s*`(?P<name>\w+)`\s+(?P<type>.*?)(?:,|\n)", column_block, re.MULTILINE | re.DOTALL)

    for name, type_expr in col_defs:
        cleaned_type = re.sub(r"\s+", " ", type_expr.strip().rstrip(','))
        columns.append({
            "name": name,
            "type": cleaned_type
        })

    return columns

def parse_sql_file(filepath):
    with open(filepath, encoding="utf-8", errors="ignore") as f:
        content = f.read()

    # Extract full CREATE TABLE block, multi-line safe
    match = re.search(
        r"CREATE TABLE\s+`?(\w+)`?\s*\((.*?)\)[^\)]*(ENGINE|DEFAULT|CHARSET)",
        content,
        re.DOTALL | re.IGNORECASE
    )
    if not match:
        return None

    table_name = match.group(1)
    column_block = match.group(2)
    columns = extract_columns(column_block)
    return table_name, columns

def parse_all_sql_files(folder):
    schema = {}
    files = [f for f in os.listdir(folder) if f.lower().endswith(".sql")]
    for filename in tqdm(files, desc="Parsing SQL files"):
        full_path = os.path.join(folder, filename)
        parsed = parse_sql_file(full_path)
        if parsed:
            table_name, columns = parsed
            schema[table_name] = columns
    return schema

def export_schema_json(schema):
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2)
    print(f"\n✅ Exported schema to {OUTPUT_JSON}")

if __name__ == "__main__":
    schema = parse_all_sql_files(SQL_FOLDER)
    export_schema_json(schema)
