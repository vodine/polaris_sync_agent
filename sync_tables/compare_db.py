import json

PG_FILE = r"C:/polaris_sync_agent/sync_tables/accelo_pg_schema.json"
MYSQL_FILE = r"C:/polaris_sync_agent/sync_tables/accelo_mysql_schema.json"
OUTPUT_REPORT = r"C:/polaris_sync_agent/sync_tables/schema_diff_report.txt"

def load_schema(path):
    with open(path, 'r') as f:
        return json.load(f)

def is_compatible(mysql_type, pg_type):
    mysql_type = mysql_type.lower()
    pg_type = pg_type.lower()

    rules = [
        (lambda m, p: m.startswith("int") and p in ["integer", "bigint", "smallint"], "compatible"),
        (lambda m, p: m.startswith("tinyint(1)") and p == "boolean", "compatible"),
        (lambda m, p: m.startswith("varchar") and (p.startswith("character varying") or p == "text"), "compatible"),
        (lambda m, p: m == "text" and p == "text", "compatible"),
        (lambda m, p: m.startswith("enum(") and p in ["text", "character varying"], "compatible"),
        (lambda m, p: m in ["datetime", "timestamp"] and p.startswith("timestamp"), "compatible"),
        (lambda m, p: m.startswith("decimal") and p in ["numeric", "decimal"], "compatible"),
        (lambda m, p: m.startswith("double") and p == "double precision", "compatible"),
        (lambda m, p: m.startswith("float") and p in ["real", "float"], "compatible"),
    ]

    for rule, label in rules:
        if rule(mysql_type, pg_type):
            return True, label

    return False, "incompatible"

def compare_schemas(pg_schema, mysql_schema):
    missing_tables = []
    table_diffs = {}

    for table, mysql_columns in mysql_schema.items():
        if table not in pg_schema:
            missing_tables.append(table)
            continue

        pg_columns = {col["name"]: col["type"] for col in pg_schema[table]}
        missing_columns = []
        mismatched_columns = []

        for col in mysql_columns:
            name, m_type = col["name"], col["type"]
            if name not in pg_columns:
                missing_columns.append(name)
            else:
                pg_type = pg_columns[name]
                if pg_type != m_type:
                    compatible, reason = is_compatible(m_type, pg_type)
                    mismatched_columns.append({
                        "column": name,
                        "pg_type": pg_type,
                        "mysql_type": m_type,
                        "compatibility": reason
                    })

        if missing_columns or mismatched_columns:
            table_diffs[table] = {
                "missing_columns": missing_columns,
                "type_mismatches": mismatched_columns
            }

    return missing_tables, table_diffs

def write_report(missing_tables, table_diffs, output_path):
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("🔍 MISSING TABLES:\n")
        for t in missing_tables:
            f.write(f"  - {t}\n")

        f.write("\n🧩 COLUMN DIFFERENCES:\n")
        for table, diffs in table_diffs.items():
            f.write(f"\n📦 Table: {table}\n")
            if diffs["missing_columns"]:
                f.write("  🚫 Missing Columns:\n")
                for col in diffs["missing_columns"]:
                    f.write(f"    - {col}\n")
            if diffs["type_mismatches"]:
                f.write("  ⚠️ Mismatched Types:\n")
                for m in diffs["type_mismatches"]:
                    f.write(f"    - {m['column']}: PG={m['pg_type']} | MySQL={m['mysql_type']} | Compatibility={m['compatibility']}\n")

    print(f"✅ Report saved to: {output_path}")

if __name__ == "__main__":
    pg_schema = load_schema(PG_FILE)
    mysql_schema = load_schema(MYSQL_FILE)
    missing_tables, table_diffs = compare_schemas(pg_schema, mysql_schema)
    write_report(missing_tables, table_diffs, OUTPUT_REPORT)
