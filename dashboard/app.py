import duckdb
from pathlib import Path

import pandas as pd
import streamlit as st

# --- Optional: ML model support ---
try:
    import joblib
    HAS_JOBLIB = True
except ImportError:
    HAS_JOBLIB = False

# --- Connect to DuckDB warehouse ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "warehouse" / "trafiklab_realtime.duckdb"


@st.cache_resource
def get_connection():
    return duckdb.connect(str(DB_PATH))


@st.cache_data
def load_delay_data():
    con = get_connection()
    df = con.execute(
        """
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
        """
    ).fetchdf()
    return df


# --- Streamlit app ---
st.set_page_config(page_title="Stockholm Traffic – Realtime Analytics", layout="wide")

st.title("Stockholm Traffic & Transit – Delay Overview")

df = load_delay_data()

if df.empty:
    st.warning(
        "No data found in analytics.fct_departure_delays yet. "
        "Run the dlt pipeline and dbt models first."
    )
    st.stop()

# ==========================
# Filters
# ==========================
col1, col2, col3 = st.columns(3)
with col1:
    modes = df["route_transport_mode"].dropna().unique().tolist()
    selected_modes = st.multiselect("Transport mode", modes, default=modes)

with col2:
    lines = df["route_designation"].dropna().unique().tolist()
    default_lines = lines[:5] if len(lines) >= 5 else lines
    selected_lines = st.multiselect("Lines", lines[:10], default=default_lines)

with col3:
    max_delay = int(df["delay_seconds"].fillna(0).max())
    delay_threshold = st.slider("Delay threshold (seconds)", 0, max_delay, 60)

# Apply filters
filtered = df.copy()

if selected_modes:
    filtered = filtered[filtered["route_transport_mode"].isin(selected_modes)]
if selected_lines:
    filtered = filtered[filtered["route_designation"].isin(selected_lines)]

# ==========================
# KPIs
# ==========================
total_dep = len(filtered)

# delayed based on threshold
delayed_mask = filtered["delay_seconds"].fillna(0) > delay_threshold
delayed_dep = delayed_mask.sum()
avg_delay = filtered["delay_seconds"].mean()

k1, k2, k3 = st.columns(3)
k1.metric("Total departures", f"{total_dep}")
k2.metric(f"Delayed departures (> {delay_threshold}s)", f"{int(delayed_dep)}")
k3.metric(
    "Average delay (sec)",
    f"{avg_delay:.1f}" if avg_delay is not None else "0.0",
)

# ==========================
# Charts
# ==========================
st.subheader("Delays by hour of day")
hourly = (
    filtered.groupby("hour_of_day")
    .agg(avg_delay=("delay_seconds", "mean"), count=("delay_seconds", "count"))
    .reset_index()
)
if not hourly.empty:
    st.bar_chart(hourly.set_index("hour_of_day")["avg_delay"])
else:
    st.info("No data available for the selected filters (hourly view).")

st.subheader("Delays by line")
line_delay = (
    filtered.groupby("route_designation")
    .agg(avg_delay=("delay_seconds", "mean"), count=("delay_seconds", "count"))
    .reset_index()
    .sort_values("avg_delay", ascending=False)
    .head(20)
)
if not line_delay.empty:
    st.bar_chart(line_delay.set_index("route_designation")["avg_delay"])
else:
    st.info("No data available for the selected filters (line view).")

# ==========================
# Simple delay prediction demo
# ==========================
# Load model + feature list
st.subheader("Simple delay prediction (demo)")
model_path = PROJECT_ROOT / "models" / "delay_model.pkl"
features_path = PROJECT_ROOT / "models" / "delay_model_features.joblib"

if not HAS_JOBLIB:
    st.info("Prediction model not available (joblib not installed in this environment).")
elif not model_path.exists() or not features_path.exists():
    st.info("Model or feature list not found. Run scripts/train_delay_model.py first.")
else:
    model = joblib.load(model_path)
    feature_cols = joblib.load(features_path)

    col_a, col_b, col_c = st.columns(3)
    with col_a:
        pred_hour = st.slider("Hour of day", 0, 23, 8)
    with col_b:
        pred_day = st.selectbox("Day of week (0=Sun)", list(range(7)), index=1)
    with col_c:
        sample_line = df["route_designation"].dropna().unique().tolist()
        pred_line = st.selectbox("Line for prediction", sample_line[:20] if sample_line else [""])

    # Build raw input
    row = {
        "hour_of_day": pred_hour,
        "day_of_week": pred_day,
    }

    # One-hot column name exactly like training produced
    if pred_line and pred_line != "":
        row[f"route_designation_{pred_line}"] = 1

    # Create dataframe and align to training features
    X_pred = pd.DataFrame([row])

    # IMPORTANT: make columns exactly match training (missing -> 0, extra -> drop, correct order)
    X_pred = X_pred.reindex(columns=feature_cols, fill_value=0)

    try:
        pred_delay = model.predict(X_pred)[0]
        st.write(f"**Predicted delay:** {pred_delay:.1f} seconds (approx.)")
    except Exception as e:
        st.error(f"Prediction error: {e}")



