from __future__ import annotations

from pathlib import Path
from datetime import date
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
st.markdown("""
<style>
/* tighter page like your screenshot */
.block-container { padding-top: 1.2rem; padding-bottom: 2rem; max-width: 1200px; }

/* chart card */
.chart-card {
  background: #ffffff;
  border: 1px solid #eef2f7;
  border-radius: 18px;
  padding: 18px 18px 6px 18px;
  box-shadow: 0 1px 2px rgba(16,24,40,0.04);
}
.chart-title {
  font-size: 11px;
  font-weight: 700;
  letter-spacing: .12em;
  text-transform: uppercase;
  color: #94a3b8;
  margin: 0 0 10px 4px;
}
</style>
""", unsafe_allow_html=True)

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

DEFAULT_CATEGORIES = ["ALL", "SL BUS", "Metro (Green/Red/Blue)", "Pendeltåg", "Train (Other)"]


# -----------------------------
# DuckDB helpers
# -----------------------------
@st.cache_resource
def get_con() -> duckdb.DuckDBPyConnection:
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
    st.sidebar.markdown("### ⚙️ Filters")

    dates = available_service_dates()
    selected_date = st.sidebar.selectbox(
        "Service date",
        options=dates,
        index=0 if dates else None,
    )

    transport_category = st.sidebar.radio(
        "Transport category",
        options=DEFAULT_CATEGORIES,
        index=0,
        help="Choose a mode to filter KPIs/plots. 'ALL' shows everything together.",
    )

    st.sidebar.divider()
    st.sidebar.code("streamlit run dashboard/app.py", language="bash")
    st.sidebar.caption("Trafiklab.se Data License CC-BY")
    return selected_date, transport_category


def where_category(alias: str, category: str) -> tuple[str, list]:
    if not category or category == "ALL":
        return "1=1", []
    return f"{alias}.transport_category = ?", [category]


# -----------------------------
# KPIs + Charts
# -----------------------------
def load_last_4h_kpis(category: str) -> dict:
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


import plotly.graph_objects as go

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

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["hour_of_day"],
        y=df["avg_delay_seconds"],
        mode="lines+markers",
        line=dict(width=3, shape="spline"),   # smooth like screenshot
        marker=dict(size=8),
        hovertemplate="Hour=%{x}<br>Delay=%{y:.0f}s<extra></extra>",
        name="Delay"
    ))

    fig.update_layout(
        height=320,
        margin=dict(l=30, r=20, t=10, b=30),
        paper_bgcolor="white",
        plot_bgcolor="white",
        showlegend=False,
        xaxis=dict(
            title="",
            showline=False,
            zeroline=False,
            tickfont=dict(size=10, color="#64748b"),
            gridcolor="#eef2f7"
        ),
        yaxis=dict(
            title="",
            showline=False,
            zeroline=False,
            tickfont=dict(size=10, color="#64748b"),
            gridcolor="#eef2f7"
        ),
    )
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

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["hour_of_day"],
        y=df["on_time_rate"],
        mode="lines+markers",
        line=dict(width=3, shape="spline"),
        marker=dict(size=7),
        hovertemplate="Hour=%{x}<br>On-time=%{y:.1%}<extra></extra>",
        name="On-time"
    ))

    fig.update_layout(
        height=320,
        margin=dict(l=30, r=20, t=10, b=30),
        paper_bgcolor="white",
        plot_bgcolor="white",
        showlegend=False,
        xaxis=dict(
            title="",
            showline=False,
            zeroline=False,
            tickfont=dict(size=10, color="#64748b"),
            gridcolor="#eef2f7"
        ),
        yaxis=dict(
            title="",
            showline=False,
            zeroline=False,
            tickfont=dict(size=10, color="#64748b"),
            gridcolor="#eef2f7",
            range=[0, 1]
        ),
    )
    return fig



# -----------------------------
# Stops + Routes
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


# -----------------------------
# Forecast
# -----------------------------
def build_next_day_prediction(selected_date: str, category: str) -> pd.DataFrame:
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
    title = f"Mobility Forecast (Next Day) — {category if category!='ALL' else 'ALL modes'}"
    fig = px.line(df_pred, x="service_date", y="departures_total", color="type", markers=True, title=title)
    fig.update_layout(xaxis_title="Date", yaxis_title="Departures")
    return fig


# -----------------------------
# UI (React-like single page)
# -----------------------------
fresh = load_freshness()
selected_date, category = sidebar_filters()

# Header block (similar vibe)
st.markdown("# 🚇 Stockholm Mobility Dashboard")
st.caption(
    f"Freshness: {fresh['latest_response_timestamp']}  •  minutes_since_update={fresh['minutes_since_last_update']}  •  Live Updates Active"
)
st.write(f"Showing data for category: **{category}** on **{selected_date}**")

# Status pill row
c_status, c_dl = st.columns([0.85, 0.15], vertical_alignment="center")
with c_status:
    st.markdown("**Status:** 🟠 Running…")
with c_dl:
    st.download_button(
        "Download (Stops CSV)",
        data=b"",
        disabled=True,
        help="Enabled after loading stops table below."
    )

st.divider()

# --- SECTION 1: KPIs (Last 4h)
st.subheader("⏱️ Real-time Performance (Last 4h)")

k4 = load_last_4h_kpis(category)
kpi_cols = st.columns(5)

kpi_cols[0].metric("Departures", fmt_int(k4.get("departures_total")), help="Last 4 hours")
kpi_cols[1].metric("On-time Rate", fmt_pct(k4.get("on_time_rate")), help="≤ 60 seconds")
kpi_cols[2].metric("Avg Delay", fmt_min_from_seconds(k4.get("avg_delay_seconds")), help="Network avg")
kpi_cols[3].metric("Delay Rate", fmt_pct(k4.get("delay_rate")), help="> 60 seconds")
kpi_cols[4].metric("Canceled", fmt_int(k4.get("departures_canceled")), help="Last 4 hours")

# --- 7d context (like your React)
st.markdown("### 7-day Context")

s7 = load_last_7d_summary(selected_date, category)
c1, c2, c3 = st.columns(3)
c1.metric("Departures (7d total)", fmt_int(s7.get("departures_7d")))
c2.metric("Avg delay (7d)", fmt_min_from_seconds(s7.get("avg_delay_seconds_7d")))
c3.metric("On-time rate (7d)", fmt_pct(s7.get("on_time_rate_7d")))

peak = load_peak_delay_hour(selected_date, category)
if peak.get("hour_of_day") is not None:
    st.info(
        f"Peak delay detected on **{selected_date}** at **{peak.get('hour_of_day')}** "
        f"with avg delay **{fmt_min_from_seconds(peak.get('avg_delay_seconds'))}** for **{category}**."
    )

# Charts row
colA, colB = st.columns(2)
with colA:
    fig1 = fig_hourly_delay(selected_date, category)
    if fig1 is not None:
        st.plotly_chart(fig1, use_container_width=True)
    else:
        st.warning("No hourly delay data for this selection/date.")
with colB:
    fig2 = fig_hourly_on_time(selected_date, category)
    if fig2 is not None:
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.warning("No hourly on-time data for this selection/date.")

st.divider()

# --- SECTION 2: Granular Delay Analysis (Stops + Routes)
st.subheader("📍 Granular Delay Analysis")

df_stops = load_top_stops(selected_date, category)
df_routes = load_top_routes(selected_date, category)

g1, g2 = st.columns(2)

with g1:
    st.markdown(f"#### Worst Delay Stops ({category})")
    if df_stops.empty:
        st.warning("No stop KPI data found for this selection/date.")
    else:
        fig = px.bar(
            df_stops.sort_values("avg_delay_seconds", ascending=True),
            x="avg_delay_seconds",
            y="stop_name",
            orientation="h",
            title=None,
            hover_data=["departures_total", "on_time_rate", "delay_rate", "transport_category"],
        )
        fig.update_layout(xaxis_title="Avg delay (s)", yaxis_title="")
        st.plotly_chart(fig, use_container_width=True)

with g2:
    st.markdown("#### Critical Routes by Delay Volume")
    if df_routes.empty:
        st.warning("No route KPI data found for this selection/date.")
    else:
        # choose top 3 worst avg delay, similar to your mock's “critical routes”
        top3 = df_routes.sort_values(["avg_delay_seconds", "departures_total"], ascending=[False, False]).head(3)
        fig = px.bar(
            top3.sort_values("avg_delay_seconds", ascending=True),
            x="avg_delay_seconds",
            y="route_designation",
            orientation="h",
            title=None,
            hover_data=["departures_total", "on_time_rate", "delay_rate", "route_direction", "transport_category"],
        )
        fig.update_layout(xaxis_title="Avg delay (s)", yaxis_title="")
        st.plotly_chart(fig, use_container_width=True)

# Detailed stop table + download
if not df_stops.empty:
    st.markdown(f"#### Detailed Stop KPIs — {category}")
    df_stop_view = df_stops[[
        "stop_name",
        "transport_category",
        "departures_total",
        "avg_delay_seconds",
        "on_time_rate",
        "delay_rate",
    ]].copy()

    df_stop_view = df_stop_view.rename(columns={
        "stop_name": "Stop Name",
        "transport_category": "Category",
        "departures_total": "Departures",
        "avg_delay_seconds": "Avg Delay (s)",
        "on_time_rate": "On-time",
        "delay_rate": "Delay Rate",
    })

    df_stop_view["Departures"] = df_stop_view["Departures"].map(lambda x: int(x) if pd.notnull(x) else x)
    df_stop_view["On-time"] = df_stop_view["On-time"].map(lambda x: f"{x:.0%}" if pd.notnull(x) else "-")
    df_stop_view["Delay Rate"] = df_stop_view["Delay Rate"].map(lambda x: f"{x:.0%}" if pd.notnull(x) else "-")
    df_stop_view["Avg Delay (s)"] = df_stop_view["Avg Delay (s)"].map(lambda x: round(float(x), 1) if pd.notnull(x) else x)

    st.dataframe(df_stop_view, use_container_width=True, hide_index=True)

    csv_bytes = df_stop_view.to_csv(index=False).encode("utf-8")
    st.download_button(
        "⬇️ Download stop KPIs (CSV)",
        data=csv_bytes,
        file_name=f"stop_kpis_{category.replace(' ', '_')}_{selected_date}.csv",
        mime="text/csv",
    )

st.divider()

# --- SECTION 3: Forecast
st.subheader("🧭 Mobility Forecast (Next Day)")
st.caption("ML Readiness: High (baseline MA7 from mart_timeseries_daily)")

df_pred = build_next_day_prediction(selected_date, category)
figp = fig_forecast(df_pred, category)
if figp is not None:
    st.plotly_chart(figp, use_container_width=True)
else:
    st.warning("No timeseries data available for prediction yet.")

st.caption("Attribution: data provided by Trafiklab.se (CC-BY).")
