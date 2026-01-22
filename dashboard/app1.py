from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta, timezone

import duckdb
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px


# -----------------------------
# Config
# -----------------------------
st.set_page_config(
    page_title="Stockholm Mobility Dashboard",
    layout="wide",
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "warehouse" / "trafiklab_realtime.duckdb"

SCHEMA = "analytics_analytics"
FCT = f"{SCHEMA}.fct_departure_delays"
MART_DAILY = f"{SCHEMA}.mart_mobility_kpis_daily"
MART_HOURLY = f"{SCHEMA}.mart_mobility_kpis_hourly"
MART_STOP = f"{SCHEMA}.mart_stop_kpis_daily"
MART_ROUTE = f"{SCHEMA}.mart_route_kpis_daily"
MART_TS = f"{SCHEMA}.mart_timeseries_daily"
MART_FRESH = f"{SCHEMA}.mart_data_freshness"

DEFAULT_CATEGORIES = ["ALL", "SL & Regional Bus", "Metro (Green/Red/Blue)", "Pendeltåg", "National Rail (SJ)", "Train (Other)"]


# -----------------------------
# DuckDB helpers
# -----------------------------
@st.cache_resource
def get_con() -> duckdb.DuckDBPyConnection:
    # Read-only connection for dashboard
    return duckdb.connect(str(DB_PATH), read_only=True)


def q(sql: str, params: list | None = None) -> pd.DataFrame:
    con = get_con()
    if params:
        return con.execute(sql, params).df()
    return con.execute(sql).df()


def fmt_int(x) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "-"
    return f"{int(x):,}"


def fmt_pct(x) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "-"
    return f"{x*100:.1f}%"


def fmt_min_from_seconds(x) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "-"
    return f"{(x/60):.1f} min"


# -----------------------------
# Data freshness
# -----------------------------
def load_freshness() -> dict:
    df = q(f"select * from {MART_FRESH} limit 1;")
    if df.empty:
        return {"latest_response_timestamp": "-", "minutes_since_last_update": "-"}
    return {
        "latest_response_timestamp": str(df.loc[0, "latest_response_timestamp"]),
        "minutes_since_last_update": int(df.loc[0, "minutes_since_last_update"]),
    }


# -----------------------------
# Filters
# -----------------------------
def available_service_dates() -> list[str]:
    df = q(f"select distinct service_date from {MART_DAILY} order by service_date desc;")
    return [str(x) for x in df["service_date"].tolist()] if not df.empty else []


def sidebar_filters() -> tuple[str, str]:
    st.sidebar.header("Filters")

    dates = available_service_dates()
    selected_date = st.sidebar.selectbox(
        "Service date",
        options=dates,
        index=0 if dates else None,
    )

    transport_category = st.sidebar.selectbox(
        "Transport category",
        options=DEFAULT_CATEGORIES,
        index=0,
        help="Choose a mode to filter KPIs/plots. 'ALL' shows everything together.",
    )

    st.sidebar.caption("Run the app with: `streamlit run dashboard/app.py`")
    return selected_date, transport_category


def where_category(alias: str, category: str) -> tuple[str, list]:
    """Build WHERE filter for transport_category with params."""
    if not category or category == "ALL":
        return "1=1", []
    return f"{alias}.transport_category = ?", [category]


# -----------------------------
# PAGE 1: Overview KPIs (last 4 hours)
# -----------------------------
def load_last_4h_kpis(category: str) -> dict:
    # We assume fct_departure_delays contains:
    # response_timestamp, scheduled_time, realtime_time, delay_seconds, canceled, is_realtime, transport_category
    # If your column names differ, adjust here.
    cond, params = where_category("f", category)

    sql = f"""
    with base as (
      select *
      from {FCT} f
      where {cond}
        and f.response_timestamp >= (now() - interval 4 hour)
    )
    select
      count(*) as departures_total,
      sum(case when canceled then 1 else 0 end) as departures_canceled,
      avg(case when canceled then null else delay_seconds end) as avg_delay_seconds,
      avg(case when canceled then null else case when coalesce(delay_seconds,0) > 60 then 1 else 0 end end) as delay_rate,
      avg(case when canceled then null else case when coalesce(delay_seconds,0) <= 60 then 1 else 0 end end) as on_time_rate
    from base;
    """
    df = q(sql, params)
    if df.empty:
        return {}
    return df.iloc[0].to_dict()


def load_last_7d_summary(selected_date: str, category: str) -> dict:
    cond, params = where_category("d", category)

    sql = f"""
    with d as (
      select *
      from {MART_DAILY} d
      where {cond}
        and d.service_date between (?::date - interval 6 day) and ?::date
    )
    select
      sum(departures_total) as departures_7d,
      avg(avg_delay_seconds) as avg_delay_seconds_7d,
      avg(on_time_rate) as on_time_rate_7d
    from d;
    """
    df = q(sql, params + [selected_date, selected_date])
    if df.empty:
        return {}
    return df.iloc[0].to_dict()


def load_peak_delay_hour(selected_date: str, category: str) -> dict:
    cond, params = where_category("h", category)
    sql = f"""
    select
      hour_of_day,
      avg_delay_seconds
    from {MART_HOURLY} h
    where {cond}
      and h.service_date = ?::date
    order by avg_delay_seconds desc
    limit 1;
    """
    df = q(sql, params + [selected_date])
    if df.empty:
        return {"hour_of_day": None, "avg_delay_seconds": None}
    return df.iloc[0].to_dict()


def fig_hourly_delay(selected_date: str, category: str):
    cond, params = where_category("h", category)
    sql = f"""
    select hour_of_day, avg_delay_seconds
    from {MART_HOURLY} h
    where {cond}
      and h.service_date = ?::date
    order by hour_of_day;
    """
    df = q(sql, params + [selected_date])
    if df.empty:
        return None
    fig = px.line(df, x="hour_of_day", y="avg_delay_seconds", markers=True, title="Avg delay by hour (seconds)")
    fig.update_layout(xaxis_title="Hour", yaxis_title="Avg delay (s)")
    return fig


def fig_hourly_on_time(selected_date: str, category: str):
    cond, params = where_category("h", category)
    sql = f"""
    select hour_of_day, on_time_rate
    from {MART_HOURLY} h
    where {cond}
      and h.service_date = ?::date
    order by hour_of_day;
    """
    df = q(sql, params + [selected_date])
    if df.empty:
        return None
    fig = px.line(df, x="hour_of_day", y="on_time_rate", markers=True, title="On-time rate by hour")
    fig.update_layout(xaxis_title="Hour", yaxis_title="On-time rate")
    return fig


# -----------------------------
# PAGE 2: Stops
# -----------------------------
def load_top_stops(selected_date: str, category: str) -> pd.DataFrame:
    cond, params = where_category("s", category)
    sql = f"""
    select
      service_date,
      stop_id,
      stop_name,
      transport_category,
      departures_total,
      on_time_rate,
      avg_delay_seconds,
      delay_rate
    from {MART_STOP} s
    where {cond}
      and s.service_date = ?::date
    order by avg_delay_seconds desc, departures_total desc
    limit 25;
    """
    return q(sql, params + [selected_date])


# -----------------------------
# PAGE 3: Routes + Forecast
# -----------------------------
def load_top_routes(selected_date: str, category: str) -> pd.DataFrame:
    cond, params = where_category("r", category)
    sql = f"""
    select
      service_date,
      route_key,
      route_designation,
      route_transport_mode,
      route_direction,
      transport_category,
      departures_total,
      on_time_rate,
      avg_delay_seconds,
      delay_rate
    from {MART_ROUTE} r
    where {cond}
      and r.service_date = ?::date
    order by avg_delay_seconds desc, departures_total desc
    limit 25;
    """
    return q(sql, params + [selected_date])


def build_next_day_prediction(selected_date: str, category: str) -> pd.DataFrame:
    """
    Simple baseline forecast (acts like ML baseline):
    - Take last 14 days from mart_timeseries_daily
    - Predict next day as moving average of last 7 days
    """
    cond, params = where_category("t", category)
    sql = f"""
    select service_date, departures_total
    from {MART_TS} t
    where {cond}
      and t.service_date between (?::date - interval 13 day) and ?::date
    order by service_date;
    """
    df = q(sql, params + [selected_date, selected_date])
    if df.empty:
        return df

    df["service_date"] = pd.to_datetime(df["service_date"])
    df["ma7"] = df["departures_total"].rolling(7, min_periods=1).mean()

    next_day = df["service_date"].max() + pd.Timedelta(days=1)
    pred = float(df["ma7"].iloc[-1])

    out = df[["service_date", "departures_total"]].copy()
    out["type"] = "actual"
    out2 = pd.DataFrame({"service_date": [next_day], "departures_total": [pred], "type": ["prediction"]})
    return pd.concat([out, out2], ignore_index=True)


def fig_forecast(df_pred: pd.DataFrame, category: str):
    if df_pred.empty:
        return None
    title = f"Next-day prediction (baseline) — {category if category!='ALL' else 'ALL modes'}"
    fig = px.line(df_pred, x="service_date", y="departures_total", color="type", markers=True, title=title)
    fig.update_layout(xaxis_title="Date", yaxis_title="Departures")
    return fig


# -----------------------------
# UI
# -----------------------------
st.title("🚇🚌🚆 Stockholm Mobility Dashboard")
fresh = load_freshness()
st.caption(f"Data freshness: latest={fresh['latest_response_timestamp']} | minutes_since_update={fresh['minutes_since_last_update']}")

selected_date, category = sidebar_filters()

tabs = st.tabs(["Page 1 — Overview", "Page 2 — Stops", "Page 3 — Routes + Prediction"])

# --- Page 1
with tabs[0]:
    st.subheader("Last 4 hours (live)")

    k4 = load_last_4h_kpis(category)
    c1, c2, c3, c4, c5 = st.columns(5)

    c1.metric("Departures (4h)", fmt_int(k4.get("departures_total")))
    c2.metric("On-time rate (≤60s)", fmt_pct(k4.get("on_time_rate")))
    c3.metric("Avg delay", fmt_min_from_seconds(k4.get("avg_delay_seconds")))
    c4.metric("Delay rate (>60s)", fmt_pct(k4.get("delay_rate")))
    c5.metric("Canceled", fmt_int(k4.get("departures_canceled")))

    st.divider()
    st.subheader("Last 7 days (context)")

    s7 = load_last_7d_summary(selected_date, category)
    c1, c2, c3 = st.columns(3)
    c1.metric("Departures (7d total)", fmt_int(s7.get("departures_7d")))
    c2.metric("Avg delay (7d)", fmt_min_from_seconds(s7.get("avg_delay_seconds_7d")))
    c3.metric("On-time rate (7d)", fmt_pct(s7.get("on_time_rate_7d")))

    peak = load_peak_delay_hour(selected_date, category)
    st.info(
        f"Most delayed hour (selected date): **{peak.get('hour_of_day')}** "
        f"with avg delay **{fmt_min_from_seconds(peak.get('avg_delay_seconds'))}**"
    )

    colA, colB = st.columns(2)
    with colA:
        fig1 = fig_hourly_delay(selected_date, category)
        if fig1 is not None:
            st.plotly_chart(fig1, width="stretch")
        else:
            st.warning("No hourly delay data for this selection/date.")
    with colB:
        fig2 = fig_hourly_on_time(selected_date, category)
        if fig2 is not None:
            st.plotly_chart(fig2, width="stretch")
        else:
            st.warning("No hourly on-time data for this selection/date.")

# --- Page 2
with tabs[1]:
    st.subheader("Stops — worst delays")
    df_stops = load_top_stops(selected_date, category)
    if df_stops.empty:
        st.warning("No stop KPI data found for this selection/date.")
    else:
        fig = px.bar(
            df_stops.sort_values("avg_delay_seconds", ascending=True),
            x="avg_delay_seconds",
            y="stop_name",
            orientation="h",
            title="Top 25 stops by avg delay (seconds)",
            hover_data=["departures_total", "on_time_rate", "delay_rate", "transport_category"],
        )
        st.plotly_chart(fig, width="stretch")
        st.dataframe(df_stops, width="stretch")

# --- Page 3
with tabs[2]:
    st.subheader("Routes — worst delays")
    df_routes = load_top_routes(selected_date, category)
    if df_routes.empty:
        st.warning("No route KPI data found for this selection/date.")
    else:
        fig = px.bar(
            df_routes.sort_values("avg_delay_seconds", ascending=True),
            x="avg_delay_seconds",
            y="route_designation",
            orientation="h",
            title="Top 25 routes by avg delay (seconds)",
            hover_data=["departures_total", "on_time_rate", "delay_rate", "route_direction", "transport_category"],
        )
        st.plotly_chart(fig, width="stretch")
        st.dataframe(df_routes, width="stretch")

    st.divider()
    st.subheader("Next-day prediction (baseline / ML-ready)")
    df_pred = build_next_day_prediction(selected_date, category)
    figp = fig_forecast(df_pred, category)
    if figp is not None:
        st.plotly_chart(figp, width="stretch")
    else:
        st.warning("No timeseries data available for prediction yet.")

st.caption("Attribution: data provided by Trafiklab.se (CC-BY).")
