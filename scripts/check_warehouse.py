import duckdb
from pathlib import Path

# Path to your DuckDB warehouse file
db_path = Path(__file__).resolve().parent.parent / "warehouse" / "stockholm_traffic.duckdb"
print("Using DB:", db_path)

con = duckdb.connect(str(db_path))

print("stg rows:", con.execute("SELECT COUNT(*) FROM analytics.stg_trafiklab_departures").fetchall())
print("fct rows:", con.execute("SELECT COUNT(*) FROM analytics.fct_departure_delays").fetchall())
