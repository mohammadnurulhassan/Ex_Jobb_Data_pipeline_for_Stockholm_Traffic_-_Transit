"""
FILE: ml_models/congestion_predictor.py
7-Day Congestion Prediction Model (Dagster-safe, DuckDB-safe)


"""

from __future__ import annotations

import json
import sys
import warnings
warnings.filterwarnings("ignore")

# ── ensure project root is on sys.path regardless of working directory ────────
# This file lives at  <root>/ml_models/congestion_predictor.py
# config.py lives at  <root>/config.py
# Without this, running `python ml_models/congestion_predictor.py` from
# inside ml_models/ fails with ModuleNotFoundError: No module named 'config'.
_ROOT = __import__("pathlib").Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
# ─────────────────────────────────────────────────────────────────────────────

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, Tuple

import duckdb
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import TimeSeriesSplit, cross_val_score
from sklearn.preprocessing import StandardScaler

from config import DUCKDB_DATABASE, STOCKHOLM_STATIONS


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


# ── Schema candidates (tried in order) ────────────────────────────────────────
_FACT_SCHEMA_CANDIDATES = [
    
    "analytics_analytics_marts",
    "main_analytics_marts",
    "analytics_marts",
]


def _resolve_fact_table(db_path: str, table_name: str) -> str:
    for schema in _FACT_SCHEMA_CANDIDATES:
        try:
            with duckdb.connect(db_path, read_only=True) as con:
                exists = con.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = ? AND table_name = ? LIMIT 1",
                    [schema, table_name],
                ).fetchone()
            if exists:
                return f"{schema}.{table_name}"
        except Exception:
            continue
    return f"main_analytics_marts.{table_name}"


@dataclass
class ModelArtifacts:
    model: RandomForestRegressor
    scaler: StandardScaler
    features: list[str]
    trained_at: str
    # [U1] store training metrics so the dashboard can read them from pkl
    metrics: dict = field(default_factory=dict)


class CongestionPredictor:
    """ML model for predicting congestion 7 days ahead."""

    def __init__(
        self,
        db_path: str | Path = DUCKDB_DATABASE,
        model_dir: Optional[str | Path] = None,
        schema: str = "main_analytics_marts",
        fact_table: str = "fact_congestion_score",
    ):
        self.db_path = str(db_path)
        self.schema = schema
        self.fact_table = fact_table

        self.model: Optional[RandomForestRegressor] = None
        self.scaler = StandardScaler()
        self.feature_columns: list[str] = []
        self._metrics: dict = {}

        root = _project_root()
        self.model_path = Path(model_dir) if model_dir else (root / "ml_models" / "saved_models")
        self.model_path.mkdir(parents=True, exist_ok=True)

    def _fact_table_name(self) -> str:
        """Auto-detect which schema holds fact_congestion_score."""
        return _resolve_fact_table(self.db_path, self.fact_table)

    # ──────────────────────────────────────────────────────────────────────────
    # Data extraction
    # ──────────────────────────────────────────────────────────────────────────
    def extract_features(self, min_days: int = 30) -> pd.DataFrame:
        table = self._fact_table_name()

        query = f"""
        SELECT
            hour,
            station_id,
            station_name,
            EXTRACT(HOUR    FROM hour) AS hour_of_day,
            EXTRACT(DOW     FROM hour) AS day_of_week,
            EXTRACT(DAY     FROM hour) AS day_of_month,
            EXTRACT(MONTH   FROM hour) AS month,
            EXTRACT(WEEK    FROM hour) AS week_of_year,
            EXTRACT(QUARTER FROM hour) AS quarter,
            departure_count,
            active_lines,
            avg_delay,
            delay_variance,
            max_delay,
            delayed_vehicles,
            metro_count,
            bus_count,
            train_count,
            tram_count,
            disruption_count,
            is_morning_rush,
            is_evening_rush,
            is_weekend,
            congestion_score
        FROM {table}
        WHERE hour >= CURRENT_TIMESTAMP - INTERVAL '{min_days} days'
          AND congestion_score IS NOT NULL
        ORDER BY hour
        """

        with duckdb.connect(self.db_path, read_only=True) as con:
            df = con.execute(query).df()

        if len(df) < 10:
            raise ValueError(
                f"Insufficient data: only {len(df)} records. Need at least 10 to train the model."
            )
        return df

    # ──────────────────────────────────────────────────────────────────────────
    # Feature engineering
    # ──────────────────────────────────────────────────────────────────────────
    def engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        df["hour_sin"]   = np.sin(2 * np.pi * df["hour_of_day"] / 24)
        df["hour_cos"]   = np.cos(2 * np.pi * df["hour_of_day"] / 24)
        df["day_sin"]    = np.sin(2 * np.pi * df["day_of_week"] / 7)
        df["day_cos"]    = np.cos(2 * np.pi * df["day_of_week"] / 7)
        df["month_sin"]  = np.sin(2 * np.pi * df["month"] / 12)
        df["month_cos"]  = np.cos(2 * np.pi * df["month"] / 12)

        df["is_any_rush"]       = (df["is_morning_rush"] | df["is_evening_rush"]).astype(int)
        df["delay_per_vehicle"] = df["avg_delay"] / (df["departure_count"] + 1)
        df["vehicles_per_line"] = df["departure_count"] / (df["active_lines"] + 1)
        df["metro_ratio"]       = df["metro_count"] / (df["departure_count"] + 1)
        df["bus_ratio"]         = df["bus_count"]   / (df["departure_count"] + 1)

        for col in ["congestion_score", "avg_delay", "departure_count"]:
            df[f"{col}_lag1"]  = df.groupby("station_id")[col].shift(1)
            df[f"{col}_lag2"]  = df.groupby("station_id")[col].shift(2)
            df[f"{col}_lag24"] = df.groupby("station_id")[col].shift(24)

        for col in ["congestion_score", "avg_delay"]:
            df[f"{col}_rolling_mean_6h"] = df.groupby("station_id")[col].transform(
                lambda x: x.rolling(6, min_periods=1).mean()
            )
            df[f"{col}_rolling_std_6h"] = df.groupby("station_id")[col].transform(
                lambda x: x.rolling(6, min_periods=1).std()
            )

        df["is_month_end"]   = (df["day_of_month"] >= 28).astype(int)
        df["is_month_start"] = (df["day_of_month"] <= 3).astype(int)
        return df

    def prepare_training_data(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.Series]:
        base_features = [
            "hour_of_day", "day_of_week", "day_of_month", "month",
            "week_of_year", "quarter",
            "hour_sin", "hour_cos", "day_sin", "day_cos", "month_sin", "month_cos",
            "station_id",
            "departure_count", "active_lines", "avg_delay", "delay_variance",
            "max_delay", "delayed_vehicles",
            "metro_count", "bus_count", "train_count", "tram_count",
            "metro_ratio", "bus_ratio",
            "disruption_count",
            "is_morning_rush", "is_evening_rush", "is_weekend", "is_any_rush",
            "is_month_end", "is_month_start",
            "delay_per_vehicle", "vehicles_per_line",
        ]
        lag_features = [
            "congestion_score_lag1", "congestion_score_lag2", "congestion_score_lag24",
            "avg_delay_lag1", "avg_delay_lag2", "avg_delay_lag24",
            "departure_count_lag1", "departure_count_lag2", "departure_count_lag24",
        ]
        rolling_features = [
            "congestion_score_rolling_mean_6h", "congestion_score_rolling_std_6h",
            "avg_delay_rolling_mean_6h", "avg_delay_rolling_std_6h",
        ]

        counts     = df.groupby("station_id").size()
        lag24_ok   = bool((counts >= 25).any())
        if not lag24_ok:
            lag_features = [c for c in lag_features if not c.endswith("_lag24")]
            print("⚠️  Low history: dropping *_lag24 features.")

        self.feature_columns = base_features + lag_features + rolling_features
        df_clean = df.dropna(
            subset=self.feature_columns + ["congestion_score"]
        ).copy()

        if df_clean.empty:
            lag_features2 = [
                c for c in lag_features
                if not c.endswith("_lag2") and not c.endswith("_lag24")
            ]
            self.feature_columns = base_features + lag_features2 + rolling_features
            print("⚠️  Still 0 rows: dropping *_lag2 features too.")
            df_clean = df.dropna(
                subset=self.feature_columns + ["congestion_score"]
            ).copy()

        if df_clean.empty:
            self.feature_columns = base_features + rolling_features
            print("⚠️  Still 0 rows: dropping ALL lag features.")
            df_clean = df.dropna(
                subset=self.feature_columns + ["congestion_score"]
            ).copy()

        if df_clean.empty:
            raise ValueError(
                "0 training rows after feature engineering. "
                "Collect more data (run DLT + dbt) and retry."
            )

        return df_clean[self.feature_columns], df_clean["congestion_score"]

    # ──────────────────────────────────────────────────────────────────────────
    # Training
    # ──────────────────────────────────────────────────────────────────────────
    def train_model(self, min_days: int = 30) -> Dict[str, Any]:
        print("\n" + "=" * 70)
        print("🤖 TRAINING CONGESTION PREDICTION MODEL")
        print("=" * 70 + "\n")

        print("📊 Extracting features from database...")
        df = self.extract_features(min_days)
        print(f"   ✓ Loaded {len(df):,} records")

        print("🔧 Engineering features...")
        df_eng = self.engineer_features(df)

        print("📦 Preparing training data...")
        X, y = self.prepare_training_data(df_eng)
        print(f"   ✓ Training samples: {len(X):,}")
        if len(X) < 10:
            raise ValueError(f"Too few training rows: {len(X)}.")
        print(f"   ✓ Features: {len(self.feature_columns)}")

        split_idx      = int(len(X) * 0.8)
        X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
        y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

        X_train_sc = self.scaler.fit_transform(X_train)
        X_test_sc  = self.scaler.transform(X_test)

        print("\n🌲 Training Random Forest Regressor...")
        self.model = RandomForestRegressor(
            n_estimators=250, max_depth=20, min_samples_split=5,
            min_samples_leaf=2, max_features="sqrt", random_state=42, n_jobs=-1,
        )
        self.model.fit(X_train_sc, y_train)

        y_pred_train = self.model.predict(X_train_sc)
        y_pred_test  = self.model.predict(X_test_sc)

        train_mae  = float(mean_absolute_error(y_train, y_pred_train))
        train_rmse = float(np.sqrt(mean_squared_error(y_train, y_pred_train)))
        train_r2   = float(r2_score(y_train, y_pred_train))
        test_mae   = float(mean_absolute_error(y_test, y_pred_test))
        test_rmse  = float(np.sqrt(mean_squared_error(y_test, y_pred_test)))
        test_r2    = float(r2_score(y_test, y_pred_test))

        tscv      = TimeSeriesSplit(n_splits=5)
        cv_scores = cross_val_score(
            self.model, X_train_sc, y_train,
            cv=tscv, scoring="neg_mean_absolute_error", n_jobs=-1,
        )
        cv_mae = float(-cv_scores.mean())
        cv_std = float(cv_scores.std() * 2)

        fi_df = pd.DataFrame(
            {"feature": self.feature_columns,
             "importance": self.model.feature_importances_}
        ).sort_values("importance", ascending=False)
        fi_df.to_csv(self.model_path / "feature_importance.csv", index=False)

        # [U1] store metrics in the instance so save_model() can persist them
        # Derive a human-readable "accuracy" as (1 - normalised MAE) * 100
        y_range = max(float(y.max() - y.min()), 1.0)
        accuracy_pct = round(max(0.0, min(100.0, (1.0 - test_mae / y_range) * 100)), 1)

        self._metrics = {
            "train_mae":     round(train_mae,  2),
            "train_rmse":    round(train_rmse, 2),
            "train_r2":      round(train_r2,   4),
            "test_mae":      round(test_mae,   2),
            "test_rmse":     round(test_rmse,  2),
            "test_r2":       round(test_r2,    4),
            "cv_mae":        round(cv_mae,     2),
            "cv_std":        round(cv_std,     2),
            "n_features":    len(self.feature_columns),
            "n_training":    int(len(X_train)),
            "n_test":        int(len(X_test)),
            "accuracy_pct":  accuracy_pct,
            "trained_at":    datetime.now().isoformat(),
        }

        print("\n" + "=" * 70)
        print("📈 MODEL PERFORMANCE")
        print("=" * 70)
        print(f"\n  Train  MAE={train_mae:.2f}  RMSE={train_rmse:.2f}  R²={train_r2:.4f}")
        print(f"  Test   MAE={test_mae:.2f}  RMSE={test_rmse:.2f}  R²={test_r2:.4f}")
        print(f"  CV MAE={cv_mae:.2f}  (+/- {cv_std:.2f})")
        print(f"\n✅ MODEL TRAINING COMPLETE  accuracy≈{accuracy_pct:.1f}%")

        return {**self._metrics, "feature_importance": fi_df}

    # ──────────────────────────────────────────────────────────────────────────
    # Persistence
    # ──────────────────────────────────────────────────────────────────────────
    def save_model(self) -> Path:
        if self.model is None:
            raise ValueError("No model to save. Train first.")

        artifacts = ModelArtifacts(
            model=self.model,
            scaler=self.scaler,
            features=self.feature_columns,
            trained_at=self._metrics.get("trained_at", datetime.now().isoformat()),
            metrics=self._metrics,
        )
        pkl_path = self.model_path / "congestion_predictor.pkl"
        joblib.dump(artifacts, pkl_path)
        print(f"\n💾 Model pkl saved  → {pkl_path}")

        # [U2] also save a lightweight JSON so the dashboard never needs sklearn
        json_path = self.model_path / "model_metrics.json"
        with open(json_path, "w") as f:
            json.dump(self._metrics, f, indent=2)
        print(f"💾 Metrics JSON saved → {json_path}")

        return pkl_path

    def load_model(self) -> ModelArtifacts:
        pkl_path = self.model_path / "congestion_predictor.pkl"
        if not pkl_path.exists():
            raise FileNotFoundError(f"Model not found: {pkl_path}")

        artifacts: ModelArtifacts = joblib.load(pkl_path)
        self.model          = artifacts.model
        self.scaler         = artifacts.scaler
        self.feature_columns = artifacts.features
        # [U3] load stored metrics back into the instance
        self._metrics = getattr(artifacts, "metrics", {})

        print(f"✅ Model loaded from: {pkl_path}")
        print(f"   Trained at: {artifacts.trained_at}")
        return artifacts

    def load_metrics_json(self) -> dict:
        """
        [U3] Load metrics from the lightweight JSON (no sklearn needed).
        Returns empty dict if the file doesn't exist yet.
        """
        json_path = self.model_path / "model_metrics.json"
        if not json_path.exists():
            return {}
        try:
            with open(json_path) as f:
                return json.load(f)
        except Exception:
            return {}

    # ──────────────────────────────────────────────────────────────────────────
    # Forecasting
    # ──────────────────────────────────────────────────────────────────────────
    def predict_next_7_days(
        self, station_id: Optional[int] = None
    ) -> pd.DataFrame:
        if self.model is None:
            raise ValueError("Model not trained. Call train_model() or load_model() first.")

        # [U4] auto-detect the correct schema for fact_congestion_score
        table = self._fact_table_name()

        latest_query = f"""
        SELECT
            station_id, station_name, congestion_score, avg_delay, departure_count, hour
        FROM {table}
        WHERE hour = (SELECT MAX(hour) FROM {table})
        """
        if station_id is not None:
            latest_query += f" AND station_id = {int(station_id)}"

        with duckdb.connect(self.db_path, read_only=True) as con:
            latest_data = con.execute(latest_query).df()

        if latest_data.empty:
            raise ValueError("No recent data available for predictions.")

        start_date  = datetime.now()
        predictions: list[dict[str, Any]] = []
        stations    = [station_id] if station_id else list(STOCKHOLM_STATIONS.keys())

        for sid in stations:
            station_name  = STOCKHOLM_STATIONS.get(sid, f"Station {sid}")
            station_latest = latest_data[latest_data["station_id"] == sid]
            if station_latest.empty:
                continue

            latest_cong  = float(station_latest["congestion_score"].iloc[0])
            latest_delay = float(station_latest["avg_delay"].iloc[0])
            latest_deps  = float(station_latest["departure_count"].iloc[0])

            for day in range(7):
                for hr in range(24):
                    future_dt = start_date + timedelta(days=day, hours=hr)
                    features  = self._create_prediction_features(
                        future_dt, sid, latest_cong, latest_delay, latest_deps
                    )
                    X        = pd.DataFrame([features])[self.feature_columns]
                    X_scaled = self.scaler.transform(X)
                    pred     = float(self.model.predict(X_scaled)[0])
                    pred     = max(0.0, min(100.0, pred))

                    predictions.append({
                        "timestamp":           future_dt,
                        "date":                future_dt.date(),
                        "hour":                hr,
                        "day_of_week":         future_dt.weekday(),
                        "day_name":            future_dt.strftime("%A"),
                        "station_id":          sid,
                        "station_name":        station_name,
                        "predicted_congestion": round(pred, 1),
                        "congestion_level":    self._get_congestion_level(pred),
                    })
                    latest_cong = pred   # autoregressive: feed prediction back

        return pd.DataFrame(predictions)

    def save_predictions(self, predictions_df: pd.DataFrame) -> None:
        if predictions_df is None or predictions_df.empty:
            print("⚠️  No predictions to save.")
            return

        df = predictions_df.copy()
        df["generated_at"] = datetime.now()

        # Keep only the columns the table expects
        keep = [
            "timestamp", "date", "hour", "day_of_week",
            "station_id", "station_name",
            "predicted_congestion", "congestion_level", "generated_at",
        ]
        df = df[[c for c in keep if c in df.columns]]

        target_schema = _FACT_SCHEMA_CANDIDATES[0]   # main_analytics_marts

        with duckdb.connect(self.db_path) as con:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema};")
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS {target_schema}.congestion_predictions (
                    timestamp             TIMESTAMP,
                    date                  DATE,
                    hour                  INTEGER,
                    day_of_week           INTEGER,
                    station_id            INTEGER,
                    station_name          VARCHAR,
                    predicted_congestion  DOUBLE,
                    congestion_level      VARCHAR,
                    generated_at          TIMESTAMP,
                    PRIMARY KEY (timestamp, station_id)
                )
            """)
            con.register("pred_df", df)
            try:
                con.execute(f"""
                    INSERT OR REPLACE INTO {target_schema}.congestion_predictions
                    SELECT * FROM pred_df
                """)
            finally:
                con.unregister("pred_df")

        print(f"💾 Predictions saved: {len(df):,} rows → {target_schema}.congestion_predictions")

    # ──────────────────────────────────────────────────────────────────────────
    # Internal helpers
    # ──────────────────────────────────────────────────────────────────────────
    def _create_prediction_features(
        self,
        future_dt: datetime,
        station_id: int,
        latest_congestion: float,
        latest_delay: float,
        latest_departures: float,
    ) -> Dict[str, Any]:
        hour_of_day     = future_dt.hour
        day_of_week     = future_dt.weekday()
        is_morning_rush = 1 if 7  <= hour_of_day <= 9  else 0
        is_evening_rush = 1 if 16 <= hour_of_day <= 18 else 0
        is_weekend      = 1 if day_of_week >= 5 else 0

        if is_morning_rush or is_evening_rush:
            est_deps  = latest_departures * 1.5
            est_delay = latest_delay      * 1.3
        elif is_weekend:
            est_deps  = latest_departures * 0.6
            est_delay = latest_delay      * 0.7
        else:
            est_deps  = latest_departures
            est_delay = latest_delay

        return {
            "hour_of_day":   hour_of_day,
            "day_of_week":   day_of_week,
            "day_of_month":  future_dt.day,
            "month":         future_dt.month,
            "week_of_year":  future_dt.isocalendar()[1],
            "quarter":       (future_dt.month - 1) // 3 + 1,
            "hour_sin":      np.sin(2 * np.pi * hour_of_day / 24),
            "hour_cos":      np.cos(2 * np.pi * hour_of_day / 24),
            "day_sin":       np.sin(2 * np.pi * day_of_week / 7),
            "day_cos":       np.cos(2 * np.pi * day_of_week / 7),
            "month_sin":     np.sin(2 * np.pi * future_dt.month / 12),
            "month_cos":     np.cos(2 * np.pi * future_dt.month / 12),
            "station_id":         station_id,
            "departure_count":    est_deps,
            "active_lines":       8,
            "avg_delay":          est_delay,
            "delay_variance":     2.0,
            "max_delay":          est_delay * 2,
            "delayed_vehicles":   5,
            "metro_count":        est_deps * 0.40,
            "bus_count":          est_deps * 0.40,
            "train_count":        est_deps * 0.15,
            "tram_count":         est_deps * 0.05,
            "metro_ratio":        0.4,
            "bus_ratio":          0.4,
            "disruption_count":   1,
            "is_morning_rush":    is_morning_rush,
            "is_evening_rush":    is_evening_rush,
            "is_weekend":         is_weekend,
            "is_any_rush":        int(is_morning_rush or is_evening_rush),
            "is_month_end":       1 if future_dt.day >= 28 else 0,
            "is_month_start":     1 if future_dt.day <= 3 else 0,
            "delay_per_vehicle":  est_delay / (est_deps + 1),
            "vehicles_per_line":  est_deps / 8,
            # Autoregressive lags — all seed from latest known value
            "congestion_score_lag1":  latest_congestion,
            "congestion_score_lag2":  latest_congestion,
            "congestion_score_lag24": latest_congestion,
            "avg_delay_lag1":         latest_delay,
            "avg_delay_lag2":         latest_delay,
            "avg_delay_lag24":        latest_delay,
            "departure_count_lag1":   latest_departures,
            "departure_count_lag2":   latest_departures,
            "departure_count_lag24":  latest_departures,
            "congestion_score_rolling_mean_6h": latest_congestion,
            "congestion_score_rolling_std_6h":  5.0,
            "avg_delay_rolling_mean_6h":        latest_delay,
            "avg_delay_rolling_std_6h":         1.5,
        }

    @staticmethod
    def _get_congestion_level(score: float) -> str:
        if score < 25:  return "Low"
        if score < 50:  return "Moderate"
        if score < 75:  return "High"
        return "Critical"


# ── Public API used by Dagster ─────────────────────────────────────────────────

def train_and_save_model() -> tuple[CongestionPredictor, dict[str, Any], Path]:
    predictor = CongestionPredictor()
    metrics   = predictor.train_model(min_days=60)  # use last 60 days for more training data
    model_path = predictor.save_model()
    return predictor, metrics, model_path


def generate_forecast() -> pd.DataFrame:
    predictor = CongestionPredictor()
    predictor.load_model()
    predictions = predictor.predict_next_7_days()
    predictor.save_predictions(predictions)
    return predictions


# ── CLI entrypoint ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    cmd = sys.argv[1] if len(sys.argv) > 1 else "both"

    if cmd == "train":
        _, metrics, path = train_and_save_model()
        print("✅ Model saved →", path)
        print(f"   Test MAE:  {metrics['test_mae']}")
        print(f"   Test R²:   {metrics['test_r2']}")
        print(f"   Accuracy:  {metrics['accuracy_pct']}%")

    elif cmd == "predict":
        df = generate_forecast()
        print("✅ Forecast rows:", len(df))
        print(df[["station_name", "timestamp", "predicted_congestion", "congestion_level"]].head(10))

    else:
        _, metrics, path = train_and_save_model()
        df = generate_forecast()
        print("✅ Done.")
        print(f"   Forecast rows: {len(df)}")
        print(f"   Model path:    {path}")
        print(f"   Accuracy:      {metrics['accuracy_pct']}%")