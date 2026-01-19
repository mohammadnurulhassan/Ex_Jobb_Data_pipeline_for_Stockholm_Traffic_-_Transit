from pathlib import Path
import pandas as pd
import plotly.express as px

from taipy.gui import Gui, notify

from queries import (
    load_base_data,
    compute_kpis,
    delays_by_hour,
    delays_by_line,
)

# Optional ML deps
try:
    import joblib
    HAS_JOBLIB = True
except ImportError:
    HAS_JOBLIB = False

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "warehouse" / "trafiklab_realtime.duckdb"

MODEL_PATH = PROJECT_ROOT / "models" / "delay_model.pkl"
FEATURES_PATH = PROJECT_ROOT / "models" / "delay_model_features.joblib"

# ----------------------------
# Load data once at startup
# ----------------------------
df_all = load_base_data()

# Filters
delay_threshold = 60

modes = sorted(df_all["route_transport_mode"].dropna().unique().tolist()) if not df_all.empty else []
selected_modes = modes[:]  # default all

lines = sorted(df_all["route_designation"].dropna().unique().tolist()) if not df_all.empty else []
selected_lines = lines[:5] if len(lines) >= 5 else lines[:]  # default some

# Outputs
kpi_total = 0
kpi_delayed = 0
kpi_avg_delay = 0.0

df_filtered = pd.DataFrame()
fig_hourly = None
fig_line = None

# ML state
ml_ready = False
ml_status = ""
pred_hour = 8
pred_day = 1
pred_line = lines[0] if lines else ""
pred_delay = None

_model = None
_feature_cols = None


def _load_model():
    global ml_ready, ml_status, _model, _feature_cols

    if not HAS_JOBLIB:
        ml_ready = False
        ml_status = "joblib is not installed in this environment."
        return

    if not MODEL_PATH.exists():
        ml_ready = False
        ml_status = "Model file not found. Run scripts/train_delay_model.py"
        return

    if not FEATURES_PATH.exists():
        ml_ready = False
        ml_status = "Feature list not found. Re-run scripts/train_delay_model.py"
        return

    try:
        _model = joblib.load(MODEL_PATH)
        _feature_cols = joblib.load(FEATURES_PATH)
        ml_ready = True
        ml_status = "ML model loaded successfully."
    except Exception as e:
        ml_ready = False
        ml_status = f"Failed to load model: {e}"


def apply_filters():
    global df_filtered, kpi_total, kpi_delayed, kpi_avg_delay, fig_hourly, fig_line

    if df_all.empty:
        df_filtered = pd.DataFrame()
        kpi_total, kpi_delayed, kpi_avg_delay = 0, 0, 0.0
        fig_hourly = None
        fig_line = None
        return

    df = df_all.copy()

    if selected_modes:
        df = df[df["route_transport_mode"].isin(selected_modes)]
    if selected_lines:
        df = df[df["route_designation"].isin(selected_lines)]

    df_filtered = df

    kpis = compute_kpis(df_filtered, delay_threshold)
    kpi_total = kpis["total"]
    kpi_delayed = kpis["delayed"]
    kpi_avg_delay = kpis["avg_delay"]

    # Charts
    df_hourly = delays_by_hour(df_filtered)
    df_line = delays_by_line(df_filtered)

    fig_hourly = px.bar(df_hourly, x="hour_of_day", y="avg_delay", title="Average delay by hour")
    fig_line = px.bar(df_line, x="route_designation", y="avg_delay", title="Average delay by line (Top 20)")


def predict_delay(state=None):
    global pred_delay

    if not ml_ready:
        pred_delay = None
        if state is not None:
            notify(state, "warning", f"ML not ready: {ml_status}")
        return

    # Build input row
    row = {
        "hour_of_day": int(pred_hour),
        "day_of_week": int(pred_day),
    }

    # Add one-hot for selected line (only that one = 1)
    if pred_line:
        row[f"route_designation_{pred_line}"] = 1

    X_pred = pd.DataFrame([row])

    # Critical: align to training features (names + order)
    X_pred = X_pred.reindex(columns=_feature_cols, fill_value=0)

    try:
        pred_delay = float(_model.predict(X_pred)[0])
        if state is not None:
            notify(state, "success", f"Predicted delay: {pred_delay:.1f} seconds")
    except Exception as e:
        pred_delay = None
        if state is not None:
            notify(state, "error", f"Prediction failed: {e}")


def on_change(state, var_name, var_value):
    apply_filters()


def refresh_data(state):
    global df_all, modes, lines, selected_modes, selected_lines, pred_line

    df_all = load_base_data()

    modes = sorted(df_all["route_transport_mode"].dropna().unique().tolist()) if not df_all.empty else []
    lines = sorted(df_all["route_designation"].dropna().unique().tolist()) if not df_all.empty else []

    selected_modes = modes[:]
    selected_lines = lines[:5] if len(lines) >= 5 else lines[:]
    pred_line = lines[0] if lines else ""

    apply_filters()
    notify(state, "success", "Data refreshed from DuckDB.")


# Initialize
apply_filters()
_load_model()

# ----------------------------
# UI
# ----------------------------
page = """
# Stockholm Traffic & Transit – Delay Overview (Taipy)

**Warehouse:** `{DB_PATH}`

<|Refresh data|button|on_action=refresh_data|>

---

## Filters

<|{selected_modes}|selector|lov={modes}|multiple=True|dropdown=True|label=Transport mode|on_change=on_change|>

<|{selected_lines}|selector|lov={lines}|multiple=True|dropdown=True|label=Lines|on_change=on_change|>

<|{delay_threshold}|slider|min=0|max=900|step=30|label=Delay threshold (seconds)|on_change=on_change|>

---

## KPIs

<|layout|columns=3|
<|Total departures|text|>
<|{kpi_total}|text|class_name=h2|>
|
<|Delayed departures|text|>
<|{kpi_delayed}|text|class_name=h2|>
|
<|Average delay (sec)|text|>
<|{kpi_avg_delay:.1f}|text|class_name=h2|>
|>

---

## Charts

<|{fig_hourly}|plotly|height=420px|>

<|{fig_line}|plotly|height=520px|>

---

## ML: Predict delay (demo)

**ML status:** {ml_status}

<|layout|columns=3|
<|{pred_hour}|slider|min=0|max=23|step=1|label=Hour of day|>
|
<|{pred_day}|selector|lov={[0,1,2,3,4,5,6]}|dropdown=True|label=Day of week (0=Sun)|>
|
<|{pred_line}|selector|lov={lines}|dropdown=True|label=Line|>
|>

<|Predict delay|button|on_action=predict_delay|>

<|Predicted delay (sec): {pred_delay}|text|>

---

## Data preview (filtered)

<|{df_filtered}|table|page_size=15|height=420px|>
"""

Gui(page).run(title="Stockholm Traffic – Taipy Dashboard", use_reloader=True)



