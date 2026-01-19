import duckdb
from pathlib import Path
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
import joblib

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "warehouse" / "trafiklab_realtime.duckdb"

MODEL_DIR = PROJECT_ROOT / "models"
MODEL_DIR.mkdir(exist_ok=True)

MODEL_PATH = MODEL_DIR / "delay_model.pkl"
FEATURES_PATH = MODEL_DIR / "delay_model_features.joblib"

con = duckdb.connect(str(DB_PATH))

df = con.execute("""
    SELECT
        delay_seconds,
        hour_of_day,
        day_of_week,
        route_designation
    FROM analytics.fct_departure_delays
    WHERE delay_seconds IS NOT NULL
""").fetchdf()

if df.empty:
    raise RuntimeError("No data available in analytics.fct_departure_delays. Run dbt models first.")

# One-hot encode route_designation
df = pd.get_dummies(df, columns=["route_designation"], dummy_na=True)

y = df["delay_seconds"]
X = df.drop(columns=["delay_seconds"])

# Save exact feature list used during training
feature_cols = list(X.columns)

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

model = RandomForestRegressor(
    n_estimators=200,
    random_state=42,
    n_jobs=-1
)
model.fit(X_train, y_train)

y_pred = model.predict(X_test)
mae = mean_absolute_error(y_test, y_pred)

joblib.dump(model, MODEL_PATH)
joblib.dump(feature_cols, FEATURES_PATH)

print(f"MAE (seconds): {mae:.2f}")
print("Model saved to:", MODEL_PATH)
print("Feature list saved to:", FEATURES_PATH)
print("Num features:", len(feature_cols))





