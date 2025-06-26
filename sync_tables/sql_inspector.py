import os
import json
import sqlparse
from sqlparse.sql import Statement
from datetime import datetime

OUTPUT_DIR = "C:/polaris_sync_agent/sql_inspector_output"
SQL_DIR = "C:/polaris_sync_agent/sql_parts"
JSON_LOG_PATH = os.path.join(OUTPUT_DIR, "sql_inspection_log.json")
HTML_REPORT_PATH = os.path.join(OUTPUT_DIR, "sql_inspection_report.html")

def analyze_statement(stmt: Statement):
    issues = []
    str_stmt = str(stmt).strip()

    if not str_stmt:
        return None

    upper = str_stmt.upper()
    if "LOCK TABLES" in upper:
        issues.append(("warning", "LOCK TABLES found – may not be portable or supported in target DB"))
    if upper.startswith("SET "):
        issues.append(("info", "SET statement – might be MySQL-specific"))
    if "AUTO_INCREMENT" in upper:
        issues.append(("warning", "AUTO_INCREMENT detected – check PostgreSQL SERIAL or IDENTITY usage"))
    if "ENGINE=" in upper:
        issues.append(("warning", "ENGINE= syntax may not be supported in PostgreSQL"))
    if "UNLOCK TABLES" in upper:
        issues.append(("info", "UNLOCK TABLES found – possibly not needed"))
    if upper.startswith("UPDATE") or upper.startswith("DELETE"):
        if "WHERE" not in upper:
            issues.append(("error", "Potential UPDATE/DELETE without WHERE clause"))

    return {"statement": str_stmt, "issues": issues}


def analyze_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    parsed = sqlparse.parse(content)
    results = []

    for stmt in parsed:
        result = analyze_statement(stmt)
        if result:
            results.append(result)
    return results


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(JSON_LOG_PATH, 'w', encoding='utf-8') as json_file, \
         open(HTML_REPORT_PATH, 'w', encoding='utf-8') as html_file:

        # HTML header
        html_file.write("<html><head><title>SQL Inspection Report</title><style>")
        html_file.write("""
        <html><head><title>SQL Inspection Report</title><style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .filename {{ font-weight: bold; font-size: 1.2em; margin-top: 20px; }}
        .statement {{ margin-left: 20px; white-space: pre-wrap; }}
        .issue {{ margin-left: 40px; }}
        .info {{ color: blue; }}
        .warning {{ color: orange; }}
        .error {{ color: red; font-weight: bold; }}
        </style></head><body>
        <h1>SQL Inspection Report</h1>
        <p>Generated: {}</p>
        """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

        json_file.write('{\n')

        first = True
        for file in os.listdir(SQL_DIR):
            if not file.endswith('.sql'):
                continue

            print(f"Analyzing {file}...")
            full_path = os.path.join(SQL_DIR, file)
            file_results = analyze_file(full_path)

            # JSON: write as stream
            if not first:
                json_file.write(',\n')
            json.dump(file, json_file)
            json_file.write(': ')
            json.dump(file_results, json_file, indent=2)
            first = False

            # HTML: flush per file
            html_file.write(f"<div class='filename'>{file}</div>\n")
            for entry in file_results:
                html_file.write(f"<div class='statement'>{entry['statement']}</div>\n")
                for level, msg in entry['issues']:
                    html_file.write(f"<div class='issue {level}'>[{level.upper()}] {msg}</div>\n")

        json_file.write('\n}\n')
        html_file.write('</body></html>\n')

    print(f"\n✅ Logs written:\n- {JSON_LOG_PATH}\n- {HTML_REPORT_PATH}")


if __name__ == '__main__':
    main()
