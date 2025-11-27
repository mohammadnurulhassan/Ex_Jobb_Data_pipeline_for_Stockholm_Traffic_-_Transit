import duckdb
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "warehouse" / "trafiklab_realtime.duckdb"

print("Using DuckDB file:", DB_PATH)

con = duckdb.connect(str(DB_PATH))

# 1) Which databases/schemas/objects exist? (duckdb built-in)
print("\n=== DESCRIBE; (duckdb objects) ===")
try:
    describe_df = con.execute("DESCRIBE;").fetchdf()
    print(describe_df)
except Exception as e:
    print("Error running DESCRIBE:", e)

# 2) Information_schema.tables: full list of tables with schema
print("\n=== information_schema.tables ===")
try:
    tables_info = con.execute("""
        SELECT table_catalog, table_schema, table_name
        FROM information_schema.tables
        ORDER BY table_catalog, table_schema, table_name
    """).fetchdf()
    print(tables_info)
except Exception as e:
    print("Error querying information_schema.tables:", e)

# 3) Try to find any table that looks like our departures
print("\n=== Try to preview any 'departures' table ===")
try:
    # Try to find a table whose name contains 'departures'
    tables_like = con.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_name ILIKE '%depart%'
        ORDER BY table_schema, table_name
    """).fetchdf()
    print("Candidate departures tables:")
    print(tables_like)

    if not tables_like.empty:
        # take first candidate
        schema = tables_like.loc[0, "table_schema"]
        name = tables_like.loc[0, "table_name"]
        print(f"\nShowing 10 rows from {schema}.{name}:\n")
        df = con.execute(f"""
            SELECT *
            FROM "{schema}"."{name}"
            LIMIT 10
        """).fetchdf()
        print(df)
    else:
        print("No table with 'depart' in the name found yet.")
except Exception as e:
    print("Error while trying to preview departures table:", e)




