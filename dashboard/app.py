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

# Page 2 marts
TBL_STOP_DAILY = f"{SCHEMA}.mart_stop_kpis_daily"
TBL_ROUTE_DAILY = f"{SCHEMA}.mart_route_kpis_daily"

# Page 3 mart
TBL_TS_DAILY = f"{SCHEMA}.mart_timeseries_daily"


# -----------------------------
# Helpers
# -----------------------------
@st.cache_data(ttl=60)
def q(sql: str, params=None) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH))
    try:
        if params:
            return con.execute(sql, params).df()
        return con.execute(sql).df()
    finally:
        con.close()


def fmt_pct(x):
    if x is None or pd.isna(x):
        return "-"
    return f"{x*100:.1f}%"


def fmt_num(x):
    if x is None or pd.isna(x):
        return "-"
    return f"{x:,.0f}"


def fmt_delay_seconds_to_min(x):
    if x is None or pd.isna(x):
        return "-"
    return f"{x/60:.1f} min"


def load_dates() -> list[str]:
    df = q(f"select distinct service_date from {TBL_DAILY} order by service_date desc;")
    if df.empty:
        return []
    return [str(x) for x in df["service_date"].tolist()]


def load_freshness():
    df = q(f"select * from {TBL_FRESH};")
    if df.empty:
        return ("-", "-")
    return (str(df.loc[0, "latest_response_timestamp"]), str(df.loc[0, "minutes_since_last_update"]))


# -----------------------------
# Sidebar
# -----------------------------
st.sidebar.title("Filters")

dates = load_dates()
if not dates:
    st.error("No dates found in mart_mobility_kpis_daily. Run dbt build first.")
    st.stop()

selected_date = st.sidebar.selectbox("Service date", options=dates, index=0)

page = st.sidebar.radio(
    "Page",
    ["Page 1 — Overview", "Page 2 — Stops & Routes", "Page 3 — Forecast & Anomalies"],
)

# -----------------------------
# PAGE 1 — Overview
# -----------------------------
if page == "Page 1 — Overview":
    st.title("Stockholm Mobility Overview")

    latest_ts, minutes_lag = load_freshness()
    c1, c2 = st.columns(2)
    with c1:
        st.metric("Latest response timestamp", latest_ts)
    with c2:
        st.metric("Minutes since update", minutes_lag)

    kpi = q(
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
        [selected_date],
    )

    if kpi.empty:
        st.warning("No KPI row for selected date.")
        st.stop()

    r = kpi.iloc[0]
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Departures", fmt_num(r["departures_total"]))
    k2.metric("On-time rate", fmt_pct(r["on_time_rate"]))
    k3.metric("Avg delay", fmt_delay_seconds_to_min(r["avg_delay_seconds"]))
    k4.metric("Delay rate (>60s)", fmt_pct(r["delay_rate"]))
    k5.metric("Canceled", fmt_num(r["departures_canceled"]))
    k6.metric("Realtime coverage", fmt_pct(r["realtime_coverage"]))

    df_hourly = q(
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
        [selected_date],
    )

    if df_hourly.empty:
        st.warning("No hourly data for selected date.")
        st.stop()

    # Trend
    trend = (
        df_hourly.groupby("hour_of_day", as_index=False)
        .agg(avg_delay_seconds=("avg_delay_seconds", "mean"))
        .sort_values("hour_of_day")
    )
    fig_trend = px.line(trend, x="hour_of_day", y="avg_delay_seconds", markers=True, title="Average delay by hour (seconds)")
    st.plotly_chart(fig_trend, width="stretch")

    # Mode bar
    mode = (
        df_hourly.groupby("transport_category", as_index=False)
        .agg(on_time_rate=("on_time_rate", "mean"), avg_delay_seconds=("avg_delay_seconds", "mean"), departures_total=("departures_total", "sum"))
        .sort_values("on_time_rate")
    )
    fig_mode = px.bar(mode, x="transport_category", y="on_time_rate", hover_data=["avg_delay_seconds", "departures_total"], title="On-time rate by transport mode")
    st.plotly_chart(fig_mode, width="stretch")

    # Heatmap
    heat = (
        df_hourly.groupby(["day_of_week", "hour_of_day"], as_index=False)
        .agg(avg_delay_seconds=("avg_delay_seconds", "mean"))
    )
    pivot = heat.pivot(index="day_of_week", columns="hour_of_day", values="avg_delay_seconds").fillna(0)
    fig_heat = px.imshow(pivot, aspect="auto", title="Delay heatmap (day of week x hour)")
    st.plotly_chart(fig_heat, width="stretch")

    st.subheader("Hourly table preview")
    st.dataframe(df_hourly, width="stretch", height=350)


# -----------------------------
# PAGE 2 — Stops & Routes Drilldown
# -----------------------------
elif page == "Page 2 — Stops & Routes":
    st.title("Stops & Routes Drilldown (Operational Insights)")

    # Load stop + route marts for the date
    stop_df = q(
        f"""
        select *
        from {TBL_STOP_DAILY}
        where service_date = ?
        """,
        [selected_date],
    )
    route_df = q(
        f"""
        select *
        from {TBL_ROUTE_DAILY}
        where service_date = ?
        """,
        [selected_date],
    )

    if stop_df.empty and route_df.empty:
        st.error("No data in mart_stop_kpis_daily / mart_route_kpis_daily for this date. Run dbt build.")
        st.stop()

    # Filters
    all_modes = sorted(set(stop_df["transport_category"].dropna().tolist() + route_df["transport_category"].dropna().tolist()))
    mode = st.sidebar.selectbox("Transport category", ["ALL"] + all_modes, index=0)

    if mode != "ALL":
        stop_df = stop_df[stop_df["transport_category"] == mode]
        route_df = route_df[route_df["transport_category"] == mode]

    c1, c2, c3, c4 = st.columns(4)

    if not stop_df.empty:
        c1.metric("Stops (rows)", fmt_num(len(stop_df)))
        c2.metric("Stop departures", fmt_num(stop_df["departures_total"].sum()))
        c3.metric("Stop avg delay (weighted)", fmt_delay_seconds_to_min(
            (stop_df["avg_delay_seconds"].fillna(0) * stop_df["departures_total"]).sum() / max(stop_df["departures_total"].sum(), 1)
        ))
        c4.metric("Stop canceled", fmt_num(stop_df["departures_canceled"].sum()))
    else:
        c1.metric("Stops (rows)", "0")
        c2.metric("Stop departures", "0")
        c3.metric("Stop avg delay (weighted)", "-")
        c4.metric("Stop canceled", "0")

    st.divider()

    left, right = st.columns(2)

    with left:
        st.subheader("Top 15 delayed stops")
        if stop_df.empty:
            st.info("No stop data after filters.")
        else:
            top_stops = stop_df.sort_values("avg_delay_seconds", ascending=False).head(15)
            fig = px.bar(
                top_stops,
                x="avg_delay_seconds",
                y="stop_name",
                orientation="h",
                title="Stops with highest average delay (seconds)",
                hover_data=["departures_total", "delay_rate", "on_time_rate", "departures_canceled"],
            )
            st.plotly_chart(fig, width="stretch")
            st.dataframe(top_stops[["stop_name","transport_category","departures_total","avg_delay_seconds","delay_rate","on_time_rate","departures_canceled"]], width="stretch")

    with right:
        st.subheader("Top 15 worst routes (by delay_rate)")
        if route_df.empty:
            st.info("No route data after filters.")
        else:
            top_routes = route_df.sort_values("delay_rate", ascending=False).head(15)
            top_routes["route_label"] = top_routes["route_designation"].fillna("") + " " + top_routes["route_name"].fillna("")
            fig = px.bar(
                top_routes,
                x="delay_rate",
                y="route_label",
                orientation="h",
                title="Routes with highest delay rate",
                hover_data=["departures_total", "avg_delay_seconds", "on_time_rate", "departures_canceled"],
            )
            st.plotly_chart(fig, width="stretch")
            st.dataframe(top_routes[["route_label","transport_category","departures_total","avg_delay_seconds","delay_rate","on_time_rate","departures_canceled"]], width="stretch")

    st.divider()
    st.subheader("Stop drilldown")
    if stop_df.empty:
        st.info("No stop data.")
    else:
        stop_choice = st.selectbox("Select stop", options=sorted(stop_df["stop_name"].dropna().unique().tolist()))
        one = stop_df[stop_df["stop_name"] == stop_choice].sort_values("transport_category")
        st.dataframe(one, width="stretch", height=250)

        # Compare stop vs overall (simple)
        overall = stop_df.copy()
        overall_row = {
            "departures_total": overall["departures_total"].sum(),
            "departures_canceled": overall["departures_canceled"].sum(),
            "avg_delay_seconds": (overall["avg_delay_seconds"].fillna(0) * overall["departures_total"]).sum() / max(overall["departures_total"].sum(), 1),
            "delay_rate": (overall["delay_rate"].fillna(0) * overall["departures_total"]).sum() / max(overall["departures_total"].sum(), 1),
            "on_time_rate": (overall["on_time_rate"].fillna(0) * overall["departures_total"]).sum() / max(overall["departures_total"].sum(), 1),
        }
        st.caption(
            f"Overall (filtered) — departures={fmt_num(overall_row['departures_total'])}, "
            f"avg_delay={fmt_delay_seconds_to_min(overall_row['avg_delay_seconds'])}, "
            f"delay_rate={fmt_pct(overall_row['delay_rate'])}, on_time={fmt_pct(overall_row['on_time_rate'])}"
        )


# -----------------------------
# PAGE 3 — Forecast & Anomalies (simple but solid)
# -----------------------------
else:
    st.title("Forecast & Anomalies (Simple Baseline)")

    # Timeseries base
    ts = q(
        f"""
        select *
        from {TBL_TS_DAILY}
        order by service_date
        """
    )
    if ts.empty:
        st.error("No data in mart_timeseries_daily. Run dbt build -s mart_timeseries_daily")
        st.stop()

    modes = sorted(ts["transport_category"].dropna().unique().tolist())
    mode = st.sidebar.selectbox("Transport category (forecast)", ["ALL"] + modes, index=0)

    if mode != "ALL":
        ts = ts[ts["transport_category"] == mode]

    metric = st.sidebar.selectbox(
        "Metric",
        ["avg_delay_seconds", "delay_rate", "on_time_rate", "departures_total"],
        index=0,
    )

    ts_plot = ts[["service_date", metric]].copy()
    ts_plot["service_date"] = pd.to_datetime(ts_plot["service_date"])
    ts_plot = ts_plot.sort_values("service_date")

    # Rolling baseline (7-day)
    ts_plot["rolling_7"] = ts_plot[metric].rolling(7, min_periods=3).mean()

    # Very simple anomaly score: |x - rolling_7| / rolling_std
    rolling_std = ts_plot[metric].rolling(7, min_periods=3).std()
    ts_plot["anomaly_score"] = (ts_plot[metric] - ts_plot["rolling_7"]).abs() / (rolling_std.replace(0, pd.NA))

    st.subheader("Timeseries with 7-day baseline")
    fig = px.line(ts_plot, x="service_date", y=[metric, "rolling_7"], title=f"{metric} vs 7-day baseline")
    st.plotly_chart(fig, width="stretch")

    st.subheader("Top anomalies (highest deviation from baseline)")
    anomalies = ts_plot.dropna(subset=["anomaly_score"]).sort_values("anomaly_score", ascending=False).head(15)
    st.dataframe(anomalies[["service_date", metric, "rolling_7", "anomaly_score"]], width="stretch")

    st.divider()
    st.subheader("Forecast (next day) — baseline extrapolation")
    # Forecast next day = last rolling_7
    if ts_plot["rolling_7"].dropna().empty:
        st.info("Not enough history yet to forecast.")
    else:
        last_date = ts_plot["service_date"].max()
        forecast_date = last_date + pd.Timedelta(days=1)
        forecast_value = ts_plot["rolling_7"].dropna().iloc[-1]
        st.metric("Forecast date", str(forecast_date.date()))
        st.metric("Forecast value", f"{forecast_value:.4f}" if metric in ["delay_rate", "on_time_rate"] else f"{forecast_value:,.2f}")

