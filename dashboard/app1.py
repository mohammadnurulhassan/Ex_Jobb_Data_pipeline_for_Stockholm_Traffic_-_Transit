from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta

import duckdb
import pandas as pd
import streamlit as st
import plotly.express as px


# -----------------------------
# Helpers
# -----------------------------
def sql_escape(value: str) -> str:
    """Escape single quotes for SQL string literals."""
    return value.replace("'", "''")


# -----------------------------
# App config
# -----------------------------
st.set_page_config(
    page_title="Stockholm Traffic Analytics",
    page_icon="🚇",
    layout="wide",
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "warehouse" / "stockholm_traffic.duckdb"

STG = "analytics_analytics_staging.stg_departures"
MART_HOURLY = "analytics_analytics_marts.fact_hourly_delays"
MART_STATION = "analytics_analytics_marts.fact_station_performance"
MART_CONG = "analytics_analytics_marts.fact_congestion_score"


@st.cache_resource
def get_con():
    return duckdb.connect(str(DB_PATH), read_only=True)


@st.cache_data(ttl=30)
def get_station_list(con) -> list[str]:
    df = con.execute(f"""
        select distinct station_name
        from {STG}
        where station_name is not null
        order by 1
    """).fetchdf()
    return df["station_name"].tolist()


@st.cache_data(ttl=30)
def get_kpis(con, since_ts: str, station: str | None):
    where = f"expected_datetime >= timestamptz '{since_ts}'"
    if station and station != "All":
        station_sql = sql_escape(station)
        where += f" and station_name = '{station_sql}'"

    q = f"""
    select
      count(*) as departures,
      avg(delay_minutes) as avg_delay,
      quantile_cont(delay_minutes, 0.95) as p95_delay,
      round(100.0 * sum(case when is_delayed then 1 else 0 end) / count(*), 2) as delayed_pct,
      sum(case when has_deviation then 1 else 0 end) as deviations
    from {STG}
    where {where}
      and delay_minutes is not null
    """
    return con.execute(q).fetchdf().iloc[0].to_dict()


@st.cache_data(ttl=30)
def get_hourly_series(con, since_ts: str, station: str | None):
    where = f"hour >= timestamptz '{since_ts}'"
    if station and station != "All":
        station_sql = sql_escape(station)
        where += f" and station_name = '{station_sql}'"

    q = f"""
    select
      hour,
      sum(total_departures) as total_departures,
      avg(avg_delay_minutes) as avg_delay_minutes,
      avg(delay_percentage) as delay_percentage
    from {MART_HOURLY}
    where {where}
    group by 1
    order by 1
    """
    return con.execute(q).fetchdf()


@st.cache_data(ttl=30)
def get_congestion_latest(con, station: str | None):
    where = "1=1"
    if station and station != "All":
        station_sql = sql_escape(station)
        where += f" and station_name = '{station_sql}'"

    q = f"""
    with latest as (
      select max(hour) as max_hour
      from {MART_CONG}
      where {where}
    )
    select
      c.*
    from {MART_CONG} c
    join latest l on c.hour = l.max_hour
    where {where}
    order by congestion_score desc
    """
    return con.execute(q).fetchdf()


@st.cache_data(ttl=30)
def get_station_hotspots(con, since_days: int = 7):
    q = f"""
    select
      station_name,
      total_departures,
      avg_delay_minutes,
      p95_delay_minutes,
      overall_delay_rate,
      deviation_rate
    from {MART_STATION}
    where last_departure >= current_timestamp - interval '{since_days} days'
    order by p95_delay_minutes desc
    limit 15
    """
    return con.execute(q).fetchdf()


@st.cache_data(ttl=30)
def get_delay_distribution(con, since_ts: str, station: str | None):
    where = f"expected_datetime >= timestamptz '{since_ts}'"
    if station and station != "All":
        station_sql = sql_escape(station)
        where += f" and station_name = '{station_sql}'"

    q = f"""
    select delay_minutes, transport_mode
    from {STG}
    where {where}
      and delay_minutes between -10 and 60
      and delay_minutes is not null
    """
    return con.execute(q).fetchdf()


@st.cache_data(ttl=30)
def get_stop_hotspots(con, since_ts: str, station: str | None):
    where = f"expected_datetime >= timestamptz '{since_ts}'"
    if station and station != "All":
        station_sql = sql_escape(station)
        where += f" and station_name = '{station_sql}'"

    q = f"""
    select
      station_name,
      stop_point_name,
      count(*) as departures,
      avg(delay_minutes) as avg_delay,
      quantile_cont(delay_minutes, 0.95) as p95_delay,
      round(100.0 * sum(case when is_delayed then 1 else 0 end) / count(*), 2) as delayed_pct
    from {STG}
    where {where}
      and stop_point_name is not null
      and delay_minutes is not null
    group by 1,2
    having count(*) >= 25
    order by p95_delay desc
    limit 20
    """
    return con.execute(q).fetchdf()


# -----------------------------
# Sidebar filters
# -----------------------------
st.title("🚇 Stockholm Traffic Analytics")

con = get_con()

with st.sidebar:
    st.header("Filters")

    if not DB_PATH.exists():
        st.error(f"DuckDB not found: {DB_PATH}")
        st.stop()

    station_list = ["All"] + get_station_list(con)
    station = st.selectbox("Station", station_list, index=0)

    window = st.selectbox(
        "Time window",
        ["Last 1 hour", "Last 6 hours", "Last 24 hours", "Last 7 days"],
        index=2,
    )

    now = datetime.now()
    if window == "Last 1 hour":
        since = now - timedelta(hours=1)
    elif window == "Last 6 hours":
        since = now - timedelta(hours=6)
    elif window == "Last 24 hours":
        since = now - timedelta(hours=24)
    else:
        since = now - timedelta(days=7)

    since_ts = since.isoformat()
    st.caption(f"DB: {DB_PATH}")


# -----------------------------
# KPIs
# -----------------------------
kpis = get_kpis(con, since_ts, station)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Departures", f"{int(kpis['departures']):,}")
c2.metric("Avg delay (min)", f"{(kpis['avg_delay'] or 0):.2f}")
c3.metric("P95 delay (min)", f"{(kpis['p95_delay'] or 0):.2f}")
c4.metric("Delayed (%)", f"{(kpis['delayed_pct'] or 0):.2f}")
c5.metric("Deviations", f"{int(kpis['deviations'] or 0):,}")

st.divider()

# -----------------------------
# Row 1: time series + congestion table
# -----------------------------
left, right = st.columns([2, 1])

with left:
    st.subheader("⏱️ Hourly trend")
    df_ts = get_hourly_series(con, since_ts, station)
    if df_ts.empty:
        st.info("No data for selected window.")
    else:
        fig = px.line(df_ts, x="hour", y="avg_delay_minutes", markers=True)
        fig.update_layout(xaxis_title="Hour", yaxis_title="Avg delay (min)")
        st.plotly_chart(fig, use_container_width=True)

with right:
    st.subheader("🚦 Latest congestion snapshot")
    df_cong = get_congestion_latest(con, station)
    if df_cong.empty:
        st.info("No congestion data yet.")
    else:
        show_cols = [
            "station_name",
            "hour",
            "congestion_score",
            "congestion_level",
            "traffic_status",
            "departure_count",
            "delayed_vehicles",
            "disruption_count",
        ]
        show_cols = [c for c in show_cols if c in df_cong.columns]
        st.dataframe(df_cong[show_cols], use_container_width=True, height=420)

st.divider()

# -----------------------------
# Row 2: delay distribution + stop hotspots
# -----------------------------
l2, r2 = st.columns(2)

with l2:
    st.subheader("📉 Delay distribution (-10 to +60)")
    df_dist = get_delay_distribution(con, since_ts, station)
    if df_dist.empty:
        st.info("No delay records.")
    else:
        fig = px.histogram(df_dist, x="delay_minutes", nbins=40)
        fig.update_layout(xaxis_title="Delay minutes", yaxis_title="Count")
        st.plotly_chart(fig, use_container_width=True)

with r2:
    st.subheader("🔥 Stop-level hotspots (top p95)")
    df_hot = get_stop_hotspots(con, since_ts, station)
    if df_hot.empty:
        st.info("No hotspot results.")
    else:
        fig = px.scatter(
            df_hot,
            x="avg_delay",
            y="p95_delay",
            size="departures",
            hover_data=["station_name", "stop_point_name", "departures", "delayed_pct"],
        )
        fig.update_layout(xaxis_title="Avg delay", yaxis_title="P95 delay")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df_hot, use_container_width=True)

st.divider()

# -----------------------------
# Row 3: station performance table
# -----------------------------
st.subheader("🏁 Station performance (last 7 days, from marts)")
df_station = get_station_hotspots(con, since_days=7)
if df_station.empty:
    st.info("No station performance table yet.")
else:
    st.dataframe(df_station, use_container_width=True)
