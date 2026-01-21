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


def q(sql: str, params=None) -> pd.DataFrame:
    """Run SQL against DuckDB and return pandas DataFrame."""
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        if params:
            df = con.execute(sql, params).df()
        else:
            df = con.execute(sql).df()
        return df
    finally:
        con.close()


# -----------------------------
# Data loaders
# -----------------------------
def load_available_dates() -> list[str]:
    df = q(f"select distinct service_date from {TBL_DAILY} order by service_date desc;")
    if df.empty:
        return []
    return [str(d) for d in df["service_date"].tolist()]


def load_kpis(service_date: str) -> dict:
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
        """,
        [service_date],
    )

    fresh = q(f"select * from {TBL_FRESH};")
    minutes_lag = None
    latest_ts = None
    if not fresh.empty:
        minutes_lag = int(fresh.loc[0, "minutes_since_last_update"])
        latest_ts = str(fresh.loc[0, "latest_response_timestamp"])

    if df.empty:
        return {
            "departures_total": 0,
            "departures_canceled": 0,
            "avg_delay_seconds": None,
            "delay_rate": None,
            "on_time_rate": None,
            "realtime_coverage": None,
            "minutes_since_last_update": minutes_lag,
            "latest_response_timestamp": latest_ts,
        }

    row = df.iloc[0].to_dict()
    row["minutes_since_last_update"] = minutes_lag
    row["latest_response_timestamp"] = latest_ts
    return row


def load_hourly_today(service_date: str) -> pd.DataFrame:
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
    # aggregate across transport_category for a single line
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
        .agg(on_time_rate=("on_time_rate", "mean"),
             avg_delay_seconds=("avg_delay_seconds", "mean"),
             departures_total=("departures_total", "sum"))
        .sort_values("on_time_rate")
    )
    fig = px.bar(d, x="transport_category", y="on_time_rate", hover_data=["avg_delay_seconds", "departures_total"])
    fig.update_layout(title="On-time rate by transport mode", xaxis_title="Mode", yaxis_title="On-time rate")
    return fig


def build_heatmap(df_hourly: pd.DataFrame):
    # Use avg_delay_seconds across transport_category (mean)
    d = (
        df_hourly.groupby(["day_of_week", "hour_of_day"], as_index=False)
        .agg(avg_delay_seconds=("avg_delay_seconds", "mean"))
    )

    # pivot for heatmap
    pivot = d.pivot(index="day_of_week", columns="hour_of_day", values="avg_delay_seconds").fillna(0)

    fig = px.imshow(pivot, aspect="auto")
    fig.update_layout(
        title="Delay heatmap (day of week x hour)",
        xaxis_title="Hour of day",
        yaxis_title="Day of week (0=Sun)",
    )
    return fig


# -----------------------------
# Taipy state
# -----------------------------
available_dates = load_available_dates()
selected_date = available_dates[0] if available_dates else None

kpi = load_kpis(selected_date) if selected_date else {}
df_hourly = load_hourly_today(selected_date) if selected_date else pd.DataFrame()

fig_trend = build_trend(df_hourly) if not df_hourly.empty else None
fig_bar = build_bar_mode(df_hourly) if not df_hourly.empty else None
fig_heatmap = build_heatmap(df_hourly) if not df_hourly.empty else None


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
    # seconds -> minutes
    return f"{x/60:.1f} min"


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
# Taipy page (Markdown)
# -----------------------------
page = """
# Stockholm Mobility Overview (Page 1)

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
### Average delay by hour
<|{fig_trend}|chart|height=350px|>

### On-time rate by transport mode
<|{fig_bar}|chart|height=350px|>

### Delay heatmap (day x hour)
<|{fig_heatmap}|chart|height=420px|>

---

## Hourly table preview
<|{df_hourly}|table|page_size=15|>
"""


