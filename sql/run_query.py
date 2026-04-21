"""
sql/run_query.py

Runs any SQL file in this folder against local CSV data via DuckDB.

Usage:
    python sql/run_query.py wau
    python sql/run_query.py mrr
    python sql/run_query.py churn
    python sql/run_query.py funnel
"""

import sys
import duckdb
from pathlib import Path

ROOT     = Path(__file__).resolve().parent.parent
SQL_DIR  = ROOT / "sql"
DATA_DIR = ROOT / "data"

def run(query_name: str):
    sql_file = SQL_DIR / f"{query_name}.sql"
    if not sql_file.exists():
        print(f"ERROR: {sql_file} not found.")
        sys.exit(1)

    sql = sql_file.read_text()
    sql = sql.replace(
        "read_csv_auto('data/",
        f"read_csv_auto('{DATA_DIR}/"
    )

    con = duckdb.connect()
    df  = con.execute(sql).df()

    print(f"\n{query_name.upper()}")
    print(df.to_string(index=False))
    print(f"\n{len(df)} rows\n")
    return df

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python sql/run_query.py <query_name>")
        sys.exit(1)
    run(sys.argv[1])