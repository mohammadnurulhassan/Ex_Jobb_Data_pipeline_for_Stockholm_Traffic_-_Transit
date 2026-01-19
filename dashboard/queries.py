from pathlib import Path
import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "warehouse" / "trafiklab_realtime.duckdb"


def _connect():
    return duckdb.connect(str(DB_PATH))


def load_base_data() -> pd.DataFrame:
    con = _connect()
    df = con.execute("""
        SELECT
            service_date,
            hour_of_day,
            day_of_week,
            route_designation,
            route_transport_mode,
            stop_name,
            delay_seconds,
            is_delayed
        FROM analytics.fct_departure_delays
    """).fetchdf()
    con.close()
    return df


def compute_kpis(df: pd.DataFrame, delay_threshold: int) -> dict:
    if df.empty:
        return {"total": 0, "delayed": 0, "avg_delay": 0.0}

    total = len(df)
    delayed = int((df["delay_seconds"].fillna(0) > delay_threshold).sum())
    avg_delay = float(df["delay_seconds"].mean()) if df["delay_seconds"].notna().any() else 0.0
    return {"total": total, "delayed": delayed, "avg_delay": avg_delay}


def delays_by_hour(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["hour_of_day", "avg_delay", "count"])

    return (
        df.groupby("hour_of_day", as_index=False)
          .agg(avg_delay=("delay_seconds", "mean"), count=("delay_seconds", "count"))
          .sort_values("hour_of_day")
    )


def delays_by_line(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["route_designation", "avg_delay", "count"])

    return (
        df.groupby("route_designation", as_index=False)
          .agg(avg_delay=("delay_seconds", "mean"), count=("delay_seconds", "count"))
          .sort_values("avg_delay", ascending=False)
          .head(20)
    )
