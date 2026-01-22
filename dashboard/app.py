from pathlib import Path
import duckdb
import pandas as pd
from taipy.gui import Gui
import plotly.express as px

# -----------------------------
# Config
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "warehouse" / "trafiklab_realtime.duckdb"

SCHEMA = "analytics_analytics"
TBL_DAILY = f"{SCHEMA}.mart_mobility_kpis_daily"
TBL_HOURLY = f"{SCHEMA}.mart_mobility_kpis_hourly"
TBL_FRESH = f"{SCHEMA}.mart_data_freshness"

# -----------------------------
# Helpers
# -----------------------------
def q(sql: str, params=None) -> pd.DataFrame:
    """Run SQL against DuckDB and return pandas DataFrame."""
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        return con.execute(sql, params).df() if params else con.execute(sql).df()
    finally:
        con.close()

def table_exists(table_name: str) -> bool:
    """DuckDB: check if table/view exists (schema.table)."""
    schema, tbl = table_name.split(".", 1)
    df = q(
        """
        select 1
        from information_schema.tables
        where table_schema = ? and table_name = ?
        limit 1
        """,
        [schema, tbl],
    )
    return not df.empty

# -----------------------------
# Data loaders
# -----------------------------
def load_available_dates() -> list[str]:
    if not table_exists(TBL_DAILY):
        return []
    df = q(f"select distinct service_date from {TBL_DAILY} order by service_date desc;")
    return [str(d) for d in df["service_date"].tolist()] if not df.empty else []

def load_kpis(service_date: str | None) -> dict:
    # Defaults
    out = {
        "departures_total": 0,
        "departures_canceled": 0,
        "avg_delay_seconds": None,
        "delay_rate": None,
        "on_time_rate": None,
        "realtime_coverage": None,
        "minutes_since_last_update": None,
        "latest_response_timestamp": None,
    }

    # Freshness (optional)
    if table_exists(TBL_FRESH):
        fresh = q(f"select * from {TBL_FRESH} limit 1;")
        if not fresh.empty:
            out["minutes_since_last_update"] = int(fresh.loc[0, "minutes_since_last_update"])
            out["latest_response_timestamp"] = str(fresh.loc[0, "latest_response_timestamp"])

    if service_date is None or (not table_exists(TBL_DAILY)):
        return out

    df = q(
        f"""
        select
            service_date,
            departures_total,
            departures_canceled,
            avg_delay_seconds,
            delay_rate,
            on_time_rate,
            realtime_coverage
        from {TBL_DAILY}
        where service_date = ?
        limit 1
        """,
        [service_date],
    )

    if df.empty:
        return out

    row = df.iloc[0].to_dict()
    # merge freshness
    row["minutes_since_last_update"] = out["minutes_since_last_update"]
    row["latest_response_timestamp"] = out["latest_response_timestamp"]
    return row

def load_hourly_today(service_date: str | None) -> pd.DataFrame:
    if service_date is None or (not table_exists(TBL_HOURLY)):
        return pd.DataFrame()

    return q(
        f"""
        select
            service_date,
            day_of_week,
            hour_of_day,
            transport_category,
            departures_total,
            avg_delay_seconds,
            delay_rate,
            on_time_rate
        from {TBL_HOURLY}
        where service_date = ?
        """,
        [service_date],
    )

# -----------------------------
# Plot builders (Plotly)
# -----------------------------
def build_trend(df_hourly: pd.DataFrame):
    d = (
        df_hourly.groupby("hour_of_day", as_index=False)
        .agg(avg_delay_seconds=("avg_delay_seconds", "mean"))
        .sort_values("hour_of_day")
    )
    fig = px.line(d, x="hour_of_day", y="avg_delay_seconds", markers=True)
    fig.update_layout(title="Average delay by hour (seconds)", xaxis_title="Hour", yaxis_title="Avg delay (s)")
    return fig

def build_bar_mode(df_hourly: pd.DataFrame):
    d = (
        df_hourly.groupby("transport_category", as_index=False)
        .agg(
            on_time_rate=("on_time_rate", "mean"),
            avg_delay_seconds=("avg_delay_seconds", "mean"),
            departures_total=("departures_total", "sum"),
        )
        .sort_values("on_time_rate")
    )
    fig = px.bar(d, x="transport_category", y="on_time_rate", hover_data=["avg_delay_seconds", "departures_total"])
    fig.update_layout(title="On-time rate by transport mode", xaxis_title="Mode", yaxis_title="On-time rate")
    return fig

def build_heatmap(df_hourly: pd.DataFrame):
    d = (
        df_hourly.groupby(["day_of_week", "hour_of_day"], as_index=False)
        .agg(avg_delay_seconds=("avg_delay_seconds", "mean"))
    )
    pivot = d.pivot(index="day_of_week", columns="hour_of_day", values="avg_delay_seconds").fillna(0)
    fig = px.imshow(pivot, aspect="auto")
    fig.update_layout(title="Delay heatmap (day of week x hour)", xaxis_title="Hour of day", yaxis_title="Day of week (0=Sun)")
    return fig

# -----------------------------
# Formatters
# -----------------------------
def fmt_pct(x):
    if x is None or pd.isna(x):
        return "-"
    return f"{x*100:.1f}%"

def fmt_num(x):
    if x is None or pd.isna(x):
        return "-"
    return f"{x:,.0f}"

def fmt_delay(x):
    if x is None or pd.isna(x):
        return "-"
    return f"{x/60:.1f} min"

# -----------------------------
# Taipy state init (SAFE)
# -----------------------------
print(">>> app.py started")
print("DB_PATH =", DB_PATH)
print("DB exists?", DB_PATH.exists())
print("TBL_DAILY exists?", table_exists(TBL_DAILY) if DB_PATH.exists() else False)
print("TBL_HOURLY exists?", table_exists(TBL_HOURLY) if DB_PATH.exists() else False)
print("TBL_FRESH exists?", table_exists(TBL_FRESH) if DB_PATH.exists() else False)

available_dates = load_available_dates()
selected_date = available_dates[0] if available_dates else None

kpi = load_kpis(selected_date)
df_hourly = load_hourly_today(selected_date)

fig_trend = build_trend(df_hourly) if not df_hourly.empty else None
fig_bar = build_bar_mode(df_hourly) if not df_hourly.empty else None
fig_heatmap = build_heatmap(df_hourly) if not df_hourly.empty else None

def on_date_change(state):
    state.kpi = load_kpis(state.selected_date)
    state.df_hourly = load_hourly_today(state.selected_date)
    if not state.df_hourly.empty:
        state.fig_trend = build_trend(state.df_hourly)
        state.fig_bar = build_bar_mode(state.df_hourly)
        state.fig_heatmap = build_heatmap(state.df_hourly)
    else:
        state.fig_trend = None
        state.fig_bar = None
        state.fig_heatmap = None

# -----------------------------
# Taipy page (render charts conditionally)
# -----------------------------
page = """
# Stockholm Mobility Overview (Page 1)

## Status
DB file: **<|{str(DB_PATH)}|>**  
Selected date: **<|{selected_date if selected_date else "No data"}|>**

<|{("✅ marts found" if selected_date else "⚠️ No dates found in mart_mobility_kpis_daily. Run dbt build and check DB path.")}|>

---

## Date
<|{selected_date}|selector|lov={available_dates}|on_change=on_date_change|dropdown|>

---

## Data freshness
Latest timestamp: **<|{kpi.get('latest_response_timestamp','-')}|>**  
Minutes since update: **<|{kpi.get('minutes_since_last_update','-')}|>**

---

## KPIs
Departures: **<|{fmt_num(kpi.get('departures_total'))}|>**  
On-time rate: **<|{fmt_pct(kpi.get('on_time_rate'))}|>**  
Avg delay: **<|{fmt_delay(kpi.get('avg_delay_seconds'))}|>**  
Delay rate (>60s): **<|{fmt_pct(kpi.get('delay_rate'))}|>**  
Canceled: **<|{fmt_num(kpi.get('departures_canceled'))}|>**  
Realtime coverage: **<|{fmt_pct(kpi.get('realtime_coverage'))}|>**

---

## Charts

<|{fig_trend is not None}|text|>

### Average delay by hour
<|{fig_trend}|chart|height=350px|>

<|{fig_bar is not None}|text|>

### On-time rate by transport mode
<|{fig_bar}|chart|height=350px|>

<|{fig_heatmap is not None}|text|>

### Delay heatmap (day x hour)
<|{fig_heatmap}|chart|height=420px|>

---

## Hourly table preview
<|{df_hourly}|table|page_size=15|>
"""

if __name__ == "__main__":
    print(">>> launching Taipy GUI...")
    Gui(page).run(title="Stockholm Mobility Dashboard", use_reloader=True, port=5000)
