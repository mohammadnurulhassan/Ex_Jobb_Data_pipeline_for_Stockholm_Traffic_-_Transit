import duckdb
from pathlib import Path
import pandas as pd
import streamlit as st

# --- Connect to DuckDB warehouse ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "warehouse" / "trafiklab_realtime.duckdb"

@st.cache_resource
def get_connection():
    return duckdb.connect(str(DB_PATH))

@st.cache_data
def load_delay_data():
    con = get_connection()
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
    return df

# --- Streamlit app ---
st.set_page_config(page_title="Stockholm Traffic – Realtime Analytics", layout="wide")

st.title("Stockholm Traffic & Transit – Delay Overview")

df = load_delay_data()

if df.empty:
    st.warning("No data found in analytics.fct_departure_delays yet. Run dlt + dbt first.")
    st.stop()

# Filters
col1, col2, col3 = st.columns(3)
with col1:
    modes = df["route_transport_mode"].dropna().unique().tolist()
    selected_modes = st.multiselect("Transport mode", modes, default=modes)
with col2:
    lines = df["route_designation"].dropna().unique().tolist()
    selected_lines = st.multiselect("Lines", lines[:10], default=lines[:5] if len(lines) >= 5 else lines)
with col3:
    max_delay = int(df["delay_seconds"].fillna(0).max())
    delay_threshold = st.slider("Delay threshold (seconds)", 0, max_delay, 60)

# Apply filters
filtered = df.copy()
if selected_modes:
    filtered = filtered[filtered["route_transport_mode"].isin(selected_modes)]
if selected_lines:
    filtered = filtered[filtered["route_designation"].isin(selected_lines)]

# KPIs
total_dep = len(filtered)
delayed_dep = filtered["is_delayed"].fillna(0).sum()
avg_delay = filtered["delay_seconds"].mean()

k1, k2, k3 = st.columns(3)
k1.metric("Total departures", f"{total_dep}")
k2.metric("Delayed departures (>60s)", f"{int(delayed_dep)}")
k3.metric("Average delay (sec)", f"{avg_delay:.1f}" if avg_delay else "0.0")

# Charts
st.subheader("Delays by hour of day")
hourly = (
    filtered
    .groupby("hour_of_day")
    .agg(avg_delay=("delay_seconds", "mean"), count=("delay_seconds", "count"))
    .reset_index()
)

st.bar_chart(hourly.set_index("hour_of_day")["avg_delay"])

st.subheader("Delays by line")
line_delay = (
    filtered
    .groupby("route_designation")
    .agg(avg_delay=("delay_seconds", "mean"), count=("delay_seconds", "count"))
    .reset_index()
    .sort_values("avg_delay", ascending=False)
    .head(20)
)

st.bar_chart(line_delay.set_index("route_designation")["avg_delay"])
