import os
import json
import re

# Input and output paths
input_path = r"C:/polaris_migration/accelo_schema_definitions.txt"
output_path = r"C:/polaris_sync_agent/sync_tables/accelo_tables.json"

# Ensure output directory exists
os.makedirs(os.path.dirname(output_path), exist_ok=True)

table_names = []

# Updated regex handles optional backticks, optional schema, spacing
pattern = re.compile(r'^CREATE TABLE\s+[`"]?(?:\w+\.)?([\w\d_]+)[`"]?\s*\(', re.IGNORECASE)

with open(input_path, 'r', encoding='utf-8') as f:
    for line in f:
        match = pattern.match(line)
        if match:
            table_name = match.group(1)
            table_names.append(table_name)

# Output structure
output_json = {"tables": sorted(table_names)}

# Write the output
with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(output_json, f, indent=2)

print(f"✅ Extracted {len(table_names)} tables and saved to:\n{output_path}")
