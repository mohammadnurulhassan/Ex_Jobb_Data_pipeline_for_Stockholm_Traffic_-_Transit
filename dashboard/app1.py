from pathlib import Path
import duckdb
import pandas as pd
import streamlit as st
import plotly.express as px


# -----------------------------
# Config
# -----------------------------
st.set_page_config(page_title="Stockholm Mobility Dashboard", layout="wide")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "warehouse" / "trafiklab_realtime.duckdb"

SCHEMA = "analytics_analytics"
TBL_DAILY = f"{SCHEMA}.mart_mobility_kpis_daily"
TBL_HOURLY = f"{SCHEMA}.mart_mobility_kpis_hourly"
TBL_FRESH = f"{SCHEMA}.mart_data_freshness"


# -----------------------------
# DB helpers
# -----------------------------
@st.cache_data(ttl=30)
def q(sql: str, params=None) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        return con.execute(sql, params).df() if params else con.execute(sql).df()
    finally:
        con.close()


def table_exists(full_name: str) -> bool:
    schema, table = full_name.split(".", 1)
    df = q(
        """
        select 1
        from information_schema.tables
        where table_schema = ? and table_name = ?
        limit 1
        """,
        [schema, table],
    )
    return not df.empty


@st.cache_data(ttl=60)
def load_available_dates() -> list[str]:
    if not table_exists(TBL_DAILY):
        return []
    df = q(f"select distinct service_date from {TBL_DAILY} order by service_date desc;")
    return [str(d) for d in df["service_date"].tolist()] if not df.empty else []


@st.cache_data(ttl=30)
def load_freshness() -> dict:
    if not table_exists(TBL_FRESH):
        return {"latest_response_timestamp": None, "minutes_since_last_update": None}
    df = q(f"select * from {TBL_FRESH} limit 1;")
    if df.empty:
        return {"latest_response_timestamp": None, "minutes_since_last_update": None}
    return {
        "latest_response_timestamp": str(df.loc[0, "latest_response_timestamp"]),
        "minutes_since_last_update": int(df.loc[0, "minutes_since_last_update"]),
    }


@st.cache_data(ttl=30)
def load_kpis(service_date: str) -> dict:
    if not table_exists(TBL_DAILY):
        return {}
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
    return df.iloc[0].to_dict() if not df.empty else {}


@st.cache_data(ttl=30)
def load_hourly(service_date: str) -> pd.DataFrame:
    if not table_exists(TBL_HOURLY):
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
# Formatting
# -----------------------------
def fmt_pct(x):
    if x is None or pd.isna(x):
        return "-"
    return f"{x*100:.1f}%"


def fmt_num(x):
    if x is None or pd.isna(x):
        return "-"
    return f"{x:,.0f}"


def fmt_delay_sec(x):
    if x is None or pd.isna(x):
        return "-"
    return f"{x/60:.1f} min"


# -----------------------------
# Page 1 UI
# -----------------------------
st.title("Stockholm Mobility Overview (Page 1)")

# Basic checks
if not DB_PATH.exists():
    st.error(f"DuckDB file not found: {DB_PATH}")
    st.stop()

if not table_exists(TBL_DAILY):
    st.error(f"Missing mart table: {TBL_DAILY}. Run `dbt build` and confirm schema/table name.")
    st.stop()

dates = load_available_dates()
if not dates:
    st.warning("No dates found in mart_mobility_kpis_daily. Load data with DLT + run dbt build.")
    st.stop()

# Sidebar controls
st.sidebar.header("Filters")
selected_date = st.sidebar.selectbox("Service date", dates, index=0)

fresh = load_freshness()
kpi = load_kpis(selected_date)
df_hourly = load_hourly(selected_date)

# Freshness block
with st.expander("Data freshness", expanded=True):
    st.write(f"**Latest timestamp:** {fresh.get('latest_response_timestamp') or '-'}")
    st.write(f"**Minutes since update:** {fresh.get('minutes_since_last_update') if fresh.get('minutes_since_last_update') is not None else '-'}")

# KPI Row
c1, c2, c3, c4, c5, c6 = st.columns(6)

c1.metric("Departures", fmt_num(kpi.get("departures_total")))
c2.metric("On-time rate", fmt_pct(kpi.get("on_time_rate")))
c3.metric("Avg delay", fmt_delay_sec(kpi.get("avg_delay_seconds")))
c4.metric("Delay rate (>60s)", fmt_pct(kpi.get("delay_rate")))
c5.metric("Canceled", fmt_num(kpi.get("departures_canceled")))
c6.metric("Realtime coverage", fmt_pct(kpi.get("realtime_coverage")))

st.divider()

if df_hourly.empty:
    st.warning("No hourly data for this date in mart_mobility_kpis_hourly.")
    st.dataframe(df_hourly)
    st.stop()

# -----------------------------
# Charts
# -----------------------------
left, right = st.columns(2)

# Trend line (avg delay by hour)
d_trend = (
    df_hourly.groupby("hour_of_day", as_index=False)
    .agg(avg_delay_seconds=("avg_delay_seconds", "mean"))
    .sort_values("hour_of_day")
)
fig_trend = px.line(d_trend, x="hour_of_day", y="avg_delay_seconds", markers=True, title="Average delay by hour (seconds)")
left.plotly_chart(fig_trend, use_container_width=True)

# Bar by mode (on-time rate)
d_mode = (
    df_hourly.groupby("transport_category", as_index=False)
    .agg(
        on_time_rate=("on_time_rate", "mean"),
        avg_delay_seconds=("avg_delay_seconds", "mean"),
        departures_total=("departures_total", "sum"),
    )
    .sort_values("on_time_rate")
)
fig_mode = px.bar(
    d_mode,
    x="transport_category",
    y="on_time_rate",
    hover_data=["avg_delay_seconds", "departures_total"],
    title="On-time rate by transport mode",
)
right.plotly_chart(fig_mode, use_container_width=True)

# Heatmap (day x hour)
st.subheader("Delay heatmap (day of week x hour)")
d_heat = (
    df_hourly.groupby(["day_of_week", "hour_of_day"], as_index=False)
    .agg(avg_delay_seconds=("avg_delay_seconds", "mean"))
)
pivot = d_heat.pivot(index="day_of_week", columns="hour_of_day", values="avg_delay_seconds").fillna(0)
fig_heat = px.imshow(pivot, aspect="auto", title="Avg delay (seconds)")
st.plotly_chart(fig_heat, use_container_width=True)

# Table
st.subheader("Hourly table preview")
st.dataframe(df_hourly, use_container_width=True)
