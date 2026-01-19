from pathlib import Path
import pandas as pd
import duckdb

from taipy.gui import Gui, notify

# Optional ML deps
try:
    import joblib
    HAS_JOBLIB = True
except ImportError:
    HAS_JOBLIB = False


# ----------------------------
# Paths
# ----------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "warehouse" / "trafiklab_realtime.duckdb"

MODEL_PATH = PROJECT_ROOT / "models" / "delay_model.pkl"
FEATURES_PATH = PROJECT_ROOT / "models" / "delay_model_features.joblib"


# ----------------------------
# Data loading helpers
# ----------------------------
def load_base_data() -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH))
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
    con.close()
    return df


def compute_kpis(df: pd.DataFrame, threshold: int) -> dict:
    if df.empty:
        return {"total": 0, "delayed": 0, "avg_delay": 0.0}

    total = len(df)
    delayed = int((df["delay_seconds"].fillna(0) > threshold).sum())
    avg_delay = float(df["delay_seconds"].mean()) if df["delay_seconds"].notna().any() else 0.0
    return {"total": total, "delayed": delayed, "avg_delay": avg_delay}


def delays_by_hour(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["hour_of_day", "avg_delay", "count"])
    return (
        df.groupby("hour_of_day", as_index=False)
          .agg(avg_delay=("delay_seconds", "mean"), count=("delay_seconds", "count"))
          .sort_values("hour_of_day")
    )


def delays_by_line(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["route_designation", "avg_delay", "count"])
    return (
        df.groupby("route_designation", as_index=False)
          .agg(avg_delay=("delay_seconds", "mean"), count=("delay_seconds", "count"))
          .sort_values("avg_delay", ascending=False)
          .head(20)
    )


# ----------------------------
# App state
# ----------------------------
df_all = load_base_data()

delay_threshold = 60

modes = sorted(df_all["route_transport_mode"].dropna().unique().tolist()) if not df_all.empty else []
selected_modes = modes[:]

lines = sorted(df_all["route_designation"].dropna().unique().tolist()) if not df_all.empty else []
selected_lines = lines[:5] if len(lines) >= 5 else lines[:]

df_filtered = pd.DataFrame()
df_hourly = pd.DataFrame()
df_line = pd.DataFrame()

kpi_total = 0
kpi_delayed = 0
kpi_avg_delay = 0.0

# ML state
ml_ready = False
ml_status = ""
pred_hour = 8
pred_day = 1
pred_line = lines[0] if lines else ""
pred_delay = None

_model = None
_feature_cols = None


def load_model():
    global ml_ready, ml_status, _model, _feature_cols

    if not HAS_JOBLIB:
        ml_ready = False
        ml_status = "joblib not installed. Install: uv pip install joblib scikit-learn"
        return

    if not MODEL_PATH.exists():
        ml_ready = False
        ml_status = "Model not found. Run: python scripts/train_delay_model.py"
        return

    if not FEATURES_PATH.exists():
        ml_ready = False
        ml_status = "Feature list not found. Re-run: python scripts/train_delay_model.py"
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
    global df_filtered, df_hourly, df_line, kpi_total, kpi_delayed, kpi_avg_delay

    if df_all.empty:
        df_filtered = pd.DataFrame()
        df_hourly = pd.DataFrame()
        df_line = pd.DataFrame()
        kpi_total, kpi_delayed, kpi_avg_delay = 0, 0, 0.0
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

    df_hourly = delays_by_hour(df_filtered)
    df_line = delays_by_line(df_filtered)


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


def predict_delay(state):
    global pred_delay

    if not ml_ready:
        pred_delay = None
        notify(state, "warning", f"ML not ready: {ml_status}")
        return

    row = {
        "hour_of_day": int(pred_hour),
        "day_of_week": int(pred_day),
    }
    if pred_line:
        row[f"route_designation_{pred_line}"] = 1

    X_pred = pd.DataFrame([row])
    X_pred = X_pred.reindex(columns=_feature_cols, fill_value=0)

    try:
        pred_delay = float(_model.predict(X_pred)[0])
        notify(state, "success", f"Predicted delay: {pred_delay:.1f} seconds")
    except Exception as e:
        pred_delay = None
        notify(state, "error", f"Prediction failed: {e}")


# Initialize
apply_filters()
load_model()

page = """
# Stockholm Traffic & Transit – Delay Overview (Taipy)

**DuckDB:** `{DB_PATH}`

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
<|Delayed departures (> threshold)|text|>
<|{kpi_delayed}|text|class_name=h2|>
|
<|Average delay (sec)|text|>
<|{kpi_avg_delay:.1f}|text|class_name=h2|>
|>

---

## Chart: Average delay by hour

<|{df_hourly}|chart|type=bar|x=hour_of_day|y=avg_delay|height=380px|>

---

## Chart: Average delay by line (Top 20)

<|{df_line}|chart|type=bar|x=route_designation|y=avg_delay|height=520px|>

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
