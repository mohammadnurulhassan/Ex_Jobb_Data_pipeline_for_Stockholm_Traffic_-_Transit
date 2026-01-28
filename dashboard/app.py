"""
FILE: dashboard/app.py
Stockholm Traffic Analytics Dashboard (Aligned with dagster_app.py + config.py)

Fixes:
- Ensures project root is on sys.path so `import config` works when running from /dashboard.

Run:
  streamlit run dashboard/app.py
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# ---------------------------------------------------------------------
# ✅ Make project root importable (so `import config` works)
# dashboard/app.py -> repo root is parents[1]
# ---------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# ---------------------------------------------------------------------
# ✅ Single source of truth (config.py in project root)
# ---------------------------------------------------------------------
from config import DUCKDB_DATABASE, DLT_DATASET_NAME, STOCKHOLM_STATIONS

# ---------------------------------------------------------------------
# Page config + styling
# ---------------------------------------------------------------------
st.set_page_config(
    page_title="Stockholm Traffic Analytics ",
    page_icon="🚇",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
<style>
    .main {padding: 0rem 1rem;}
    .stMetric {background-color: #f0f2f6; padding: 15px; border-radius: 10px;}
    h1 {color: #1f77b4;}
    .prediction-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    </style>
""", unsafe_allow_html=True,
)

# ---------------------------------------------------------------------
# Schemas/tables aligned with dagster_app.py
# ---------------------------------------------------------------------
RAW_SCHEMA = DLT_DATASET_NAME  # usually "raw_traffic"
MART_SCHEMA = "analytics_analytics_marts"

FACT_HOURLY = f"{MART_SCHEMA}.fact_hourly_delays"
FACT_CONGESTION = f"{MART_SCHEMA}.fact_congestion_score"
FACT_STATION = f"{MART_SCHEMA}.fact_station_performance"

# Optional predictions: if you later materialize into DuckDB, set env var
# Example: set PREDICTIONS_TABLE=analytics_analytics_marts.congestion_predictions
PREDICTIONS_TABLE = os.getenv("PREDICTIONS_TABLE", "").strip()

# Fallback CSV produced by your Dagster ML asset
PROJECT_ROOT = REPO_ROOT
PREDICTIONS_CSV_FALLBACK = PROJECT_ROOT / "predictions_sample.csv"
MODEL_PATH = PROJECT_ROOT / "ml_models" / "saved_models" / "congestion_predictor.pkl"


# =============================================================================
# DuckDB helpers
# =============================================================================
@st.cache_resource
def get_db_connection() -> duckdb.DuckDBPyConnection | None:
    try:
        return duckdb.connect(DUCKDB_DATABASE, read_only=True)
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return None


def table_exists(con: duckdb.DuckDBPyConnection, full_name: str) -> bool:
    if "." not in full_name:
        return False
    schema, name = full_name.split(".", 1)
    q = """
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = ? AND table_name = ?
    LIMIT 1
    """
    try:
        return con.execute(q, [schema, name]).fetchone() is not None
    except Exception:
        return False


def safe_df(con: duckdb.DuckDBPyConnection, query: str) -> pd.DataFrame:
    try:
        return con.execute(query).fetchdf()
    except Exception:
        return pd.DataFrame()


def find_raw_table(con: duckdb.DuckDBPyConnection, schema: str) -> str | None:
    df = safe_df(
        con,
        f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{schema}'
          AND table_type IN ('BASE TABLE','VIEW')
        ORDER BY table_name
        """,
    )
    if df.empty:
        return None

    names = df["table_name"].tolist()
    for n in names:
        if "depart" in n.lower():
            return f"{schema}.{n}"
    return f"{schema}.{names[0]}"


def get_columns(con: duckdb.DuckDBPyConnection, full_name: str) -> set[str]:
    if "." not in full_name:
        return set()
    schema, table = full_name.split(".", 1)
    df = safe_df(
        con,
        f"""
        SELECT lower(column_name) AS col
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
          AND table_name = '{table}'
        """,
    )
    return set(df["col"].tolist()) if not df.empty else set()


def human_age(ts) -> str:
    if ts is None:
        return "unknown"
    try:
        dt = pd.to_datetime(ts).to_pydatetime()
        diff = datetime.now() - dt.replace(tzinfo=None)
        sec = int(diff.total_seconds())
        if sec < 60:
            return f"{sec}s ago"
        if sec < 3600:
            return f"{sec // 60}m ago"
        return f"{sec // 3600}h ago"
    except Exception:
        return "unknown"


# =============================================================================
# Data fetchers
# =============================================================================
@st.cache_data(ttl=30)
def get_live_statistics():
    """Get real-time statistics"""
    con = get_db_connection()
    if not con:
        return None

    raw_table = find_raw_table(con, RAW_SCHEMA)
    if not raw_table:
        return {"__error__": f"No tables found in schema `{RAW_SCHEMA}`. Run DLT/Dagster ingestion first."}

    cols = get_columns(con, raw_table)
    if not cols:
        return {"__error__": f"Could not read columns for `{raw_table}`"}

    ts_candidates = ["ingestion_timestamp_utc", "ingestion_timestamp", "ingestion_ts", "timestamp", "created_at"]
    site_candidates = ["site_id", "station_id"]
    line_candidates = ["line", "line_number", "linenumber"]
    delay_candidates = ["delay_minutes", "delay", "delay_mins", "avg_delay_minutes"]
    dev_candidates = ["has_deviation", "deviation", "is_deviation"]

    def pick(cands: list[str]) -> str | None:
        for c in cands:
            if c in cols:
                return c
        return None

    ts_col = pick(ts_candidates)
    site_col = pick(site_candidates)
    line_col = pick(line_candidates)
    delay_col = pick(delay_candidates)
    dev_col = pick(dev_candidates)

    if ts_col is None:
        return {"__error__": f"No timestamp column found in `{raw_table}`. Columns: {sorted(list(cols))[:40]}..."}

    ts_expr = f"try_cast({ts_col} as timestamptz)"
    delay_expr = f"coalesce(try_cast({delay_col} as double), 0)" if delay_col else "0.0"
    site_expr = f"try_cast({site_col} as bigint)" if site_col else "NULL"
    line_expr = f"cast({line_col} as varchar)" if line_col else "NULL"
    dev_expr = f"coalesce(try_cast({dev_col} as boolean), false)" if dev_col else "false"

    q = f"""
    WITH base AS (
      SELECT
        {ts_expr} AS ingestion_ts,
        {site_expr} AS station_key,
        {line_expr} AS line_key,
        {delay_expr} AS delay_mins,
        {dev_expr} AS has_dev
      FROM {raw_table}
    )
    SELECT
      COUNT(*) AS total_departures,
      COUNT(DISTINCT station_key) AS active_stations,
      COUNT(DISTINCT line_key) AS active_lines,
      AVG(delay_mins) AS avg_delay,
      MAX(delay_mins) AS max_delay,
      SUM(CASE WHEN delay_mins > 5 THEN 1 ELSE 0 END) AS significant_delays,
      SUM(CASE WHEN has_dev THEN 1 ELSE 0 END) AS active_disruptions,
      MAX(ingestion_ts) AS last_update
    FROM base
    WHERE ingestion_ts IS NOT NULL
      AND ingestion_ts >= current_timestamp - INTERVAL '15 minutes'
    """

    try:
        row = con.execute(q).fetchone()
        if not row:
            return {"__error__": "Live stats query returned no rows"}

        return {
            "raw_table": raw_table,
            "total_departures": int(row[0] or 0),
            "active_stations": int(row[1] or 0),
            "active_lines": int(row[2] or 0),
            "avg_delay": float(row[3] or 0),
            "max_delay": float(row[4] or 0),
            "significant_delays": int(row[5] or 0),
            "active_disruptions": int(row[6] or 0),
            "last_update": row[7],
        }
    except Exception as e:
        return {"__error__": f"{e}\n\nRaw table used: {raw_table}\nColumns: {sorted(list(cols))[:40]}..."}


@st.cache_data(ttl=60)
def get_hourly_trends(hours: int = 24) -> pd.DataFrame:
    con = get_db_connection()
    if not con or not table_exists(con, FACT_HOURLY):
        return pd.DataFrame()

    q = f"""
    SELECT
      hour,
      total_departures,
      avg_delay_minutes,
      delayed_departures,
      delay_percentage
    FROM {FACT_HOURLY}
    WHERE hour >= current_timestamp - INTERVAL '{hours} hours'
    ORDER BY hour
    """
    return safe_df(con, q)


@st.cache_data(ttl=120)
def get_congestion_last_24h() -> pd.DataFrame:
    con = get_db_connection()
    if not con or not table_exists(con, FACT_CONGESTION):
        return pd.DataFrame()

    q = f"""
    SELECT
      hour,
      station_name,
      congestion_score,
      congestion_level,
      traffic_status,
      avg_delay
    FROM {FACT_CONGESTION}
    WHERE hour >= current_timestamp - INTERVAL '24 hours'
    ORDER BY hour
    """
    return safe_df(con, q)


@st.cache_data(ttl=300)
def get_predictions() -> pd.DataFrame:
    """Get 7-day predictions"""
    con = get_db_connection()

    if con and PREDICTIONS_TABLE and table_exists(con, PREDICTIONS_TABLE):
        df = safe_df(con, f"SELECT * FROM {PREDICTIONS_TABLE}")
        if not df.empty:
            return df

    if PREDICTIONS_CSV_FALLBACK.exists():
        try:
            return pd.read_csv(PREDICTIONS_CSV_FALLBACK)
        except Exception:
            return pd.DataFrame()

    return pd.DataFrame()


# =============================================================================
# Main UI
# =============================================================================
def main():
    # Header
    c1, c2, c3 = st.columns([2.2, 1, 1])
    with c1:
        st.title("🚇 Stockholm Traffic Analytics")
        st.markdown("**Real-time monitoring with AI-powered 7-day predictions**")
        #st.caption(f"DB: `{DUCKDB_DATABASE}`")

    with c2:
        auto_refresh = st.checkbox("🔄 Auto-refresh", value=True)
        refresh_seconds = st.selectbox("Interval", [15, 30, 60, 120], index=1)

    with c3:
        if st.button("🔃 Refresh now"):
            st.cache_data.clear()
            st.rerun()

    # Safe auto-refresh
    if auto_refresh:
        last = st.session_state.get("last_refresh_ts")
        now = time.time()
        if last is None or (now - last) >= refresh_seconds:
            st.session_state["last_refresh_ts"] = now
            st.cache_data.clear()
            st.rerun()

    con = get_db_connection()
    if con is None:
        st.error("⚠️ DuckDB not found or not accessible.")
        st.stop()

    # Live stats
    stats = get_live_statistics()
    if not stats or "__error__" in stats:
        st.error("⚠️ Could not load live stats.")
        if stats and "__error__" in stats:
            st.code(stats["__error__"])
        st.stop()

    st.info(f"📡 Last data update: **{human_age(stats.get('last_update'))}**")
    st.caption(f"Using raw table: `{stats.get('raw_table')}`")

    st.divider()

    # KPI
    st.subheader("📊 Live Metrics (Last 15 minutes)")
    k1, k2, k3, k4 = st.columns(4)

    with k1:
        st.metric("Total Departures", f"{stats['total_departures']:,}", f"{stats['active_stations']} stations")

    with k2:
        st.metric("Average Delay", f"{stats['avg_delay']:.1f} min", f"Max: {stats['max_delay']:.0f} min", delta_color="inverse")

    with k3:
        pct = (stats["significant_delays"] / max(stats["total_departures"], 1)) * 100
        st.metric("Significant Delays", f"{stats['significant_delays']:,}", f"{pct:.1f}%", delta_color="inverse")

    with k4:
        st.metric("Active Lines", f"{stats['active_lines']:,}", f"{stats['active_disruptions']} deviations", delta_color="inverse")

    st.divider()

    tab1, tab2, tab3 = st.tabs(["📈 Trends", "📊 Congestion", "🔮 Predictions (Optional)"])

    with tab1:
        st.subheader("📈 Hourly trends (marts)")
        hours = st.slider("Hours back", min_value=6, max_value=72, value=24, step=6)
        hourly_df = get_hourly_trends(hours)

        if hourly_df.empty:
            st.warning("No hourly mart data found. Ensure dbt built `analytics_analytics_marts.fact_hourly_delays`.")
        else:
            fig1 = go.Figure()
            fig1.add_trace(go.Scatter(x=hourly_df["hour"], y=hourly_df["total_departures"], mode="lines+markers", name="Departures", fill="tozeroy"))
            fig1.update_layout(title=f"Departures per Hour (Last {hours}h)", xaxis_title="Hour", yaxis_title="Departures", height=380)
            st.plotly_chart(fig1, use_container_width=True)

            left, right = st.columns(2)
            with left:
                fig2 = go.Figure()
                fig2.add_trace(go.Scatter(x=hourly_df["hour"], y=hourly_df["avg_delay_minutes"], mode="lines+markers", name="Avg delay (min)"))
                fig2.update_layout(title="Average Delay", xaxis_title="Hour", yaxis_title="Minutes", height=330)
                st.plotly_chart(fig2, use_container_width=True)

            with right:
                fig3 = go.Figure()
                fig3.add_trace(go.Bar(x=hourly_df["hour"], y=hourly_df["delay_percentage"], name="Delay %"))
                fig3.update_layout(title="Delay Percentage", xaxis_title="Hour", yaxis_title="Percent", height=330)
                st.plotly_chart(fig3, use_container_width=True)

    with tab2:
        st.subheader("📊 Congestion (last 24h)")
        cong_df = get_congestion_last_24h()

        if cong_df.empty:
            st.warning("No congestion mart data found. Ensure dbt built `analytics_analytics_marts.fact_congestion_score`.")
        else:
            latest = cong_df.sort_values("hour").groupby("station_name")["congestion_score"].last().sort_values(ascending=False)

            fig = px.bar(x=latest.values, y=latest.index, orientation="h", title="Current Congestion Score by Station", labels={"x": "Congestion Score", "y": "Station"})
            fig.update_layout(height=520)
            st.plotly_chart(fig, use_container_width=True)

            fig2 = px.line(cong_df, x="hour", y="congestion_score", color="station_name", title="Congestion Timeline (24h)")
            fig2.update_layout(height=420)
            st.plotly_chart(fig2, use_container_width=True)

    with tab3:
        st.subheader("🔮 Predictions (Optional ML)")

        with st.expander("🤖 Model status", expanded=True):
            if MODEL_PATH.exists():
                mt = datetime.fromtimestamp(MODEL_PATH.stat().st_mtime)
                age = datetime.now() - mt
                st.success(f"✅ Model found: trained {age.days} day(s) ago")
                st.caption(f"Last modified: {mt.strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                st.warning("⚠️ No model file found yet.")
                st.caption(f"Expected: {MODEL_PATH}")

        pred_df = get_predictions()
        if pred_df.empty:
            st.warning("No predictions available. Set `PREDICTIONS_TABLE` or ensure `predictions_sample.csv` exists.")
        else:
            st.success(f"✅ Loaded predictions: {len(pred_df):,} rows")
            st.dataframe(pred_df.head(200), use_container_width=True)

    with st.sidebar:
        st.header("⚙️ Info")
        st.caption(f"Repo root: {REPO_ROOT}")
        st.divider()

        st.markdown("**DuckDB**")
        st.code(DUCKDB_DATABASE, language="text")

        st.markdown("**Schemas**")
        st.markdown(f"- Raw: `{RAW_SCHEMA}`")
        st.markdown(f"- Marts: `{MART_SCHEMA}`")

        st.divider()
        st.markdown("**Monitored sites**")
        for k, v in STOCKHOLM_STATIONS.items():
            st.markdown(f"- {k}: {v}")


if __name__ == "__main__":
    main()
