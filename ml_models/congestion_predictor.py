"""
FILE: ml_models/congestion_predictor.py
7-Day Congestion Prediction Model (Dagster-safe, DuckDB-safe)
"""

from __future__ import annotations

import warnings
warnings.filterwarnings("ignore")

from dataclasses import dataclass
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
    """Assumes this file lives in <root>/ml_models/congestion_predictor.py"""
    return Path(__file__).resolve().parents[1]


@dataclass
class ModelArtifacts:
    model: RandomForestRegressor
    scaler: StandardScaler
    features: list[str]
    trained_at: str


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

        root = _project_root()
        self.model_path = Path(model_dir) if model_dir else (root / "ml_models" / "saved_models")
        self.model_path.mkdir(parents=True, exist_ok=True)

    # -----------------------------
    # Data extraction
    # -----------------------------
    def extract_features(self, min_days: int = 30) -> pd.DataFrame:
        table = f"{self.schema}.{self.fact_table}"

        query = f"""
        SELECT 
            hour,
            station_id,
            station_name,

            EXTRACT(HOUR FROM hour) as hour_of_day,
            EXTRACT(DOW FROM hour) as day_of_week,
            EXTRACT(DAY FROM hour) as day_of_month,
            EXTRACT(MONTH FROM hour) as month,
            EXTRACT(WEEK FROM hour) as week_of_year,
            EXTRACT(QUARTER FROM hour) as quarter,

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

        if len(df) < 30:
            raise ValueError(f"Insufficient data: only {len(df)} records. Need at least 100.")

        return df

    # -----------------------------
    # Feature engineering
    # -----------------------------
    def engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        df["hour_sin"] = np.sin(2 * np.pi * df["hour_of_day"] / 24)
        df["hour_cos"] = np.cos(2 * np.pi * df["hour_of_day"] / 24)
        df["day_sin"] = np.sin(2 * np.pi * df["day_of_week"] / 7)
        df["day_cos"] = np.cos(2 * np.pi * df["day_of_week"] / 7)
        df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
        df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)

        df["is_any_rush"] = (df["is_morning_rush"] | df["is_evening_rush"]).astype(int)

        df["delay_per_vehicle"] = df["avg_delay"] / (df["departure_count"] + 1)
        df["vehicles_per_line"] = df["departure_count"] / (df["active_lines"] + 1)
        df["metro_ratio"] = df["metro_count"] / (df["departure_count"] + 1)
        df["bus_ratio"] = df["bus_count"] / (df["departure_count"] + 1)

        for col in ["congestion_score", "avg_delay", "departure_count"]:
            df[f"{col}_lag1"] = df.groupby("station_id")[col].shift(1)
            df[f"{col}_lag2"] = df.groupby("station_id")[col].shift(2)
            df[f"{col}_lag24"] = df.groupby("station_id")[col].shift(24)

        for col in ["congestion_score", "avg_delay"]:
            df[f"{col}_rolling_mean_6h"] = df.groupby("station_id")[col].transform(
                lambda x: x.rolling(6, min_periods=1).mean()
            )
            df[f"{col}_rolling_std_6h"] = df.groupby("station_id")[col].transform(
                lambda x: x.rolling(6, min_periods=1).std()
            )

        df["is_month_end"] = (df["day_of_month"] >= 28).astype(int)
        df["is_month_start"] = (df["day_of_month"] <= 3).astype(int)

        return df

    def prepare_training_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Prepare features + target.
        Works even when you have limited history (drops lag24 automatically if needed).
        """
        # -----------------------------
        # Base features (always available)
        # -----------------------------
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

        # -----------------------------
        # Lag + rolling features
        # -----------------------------
        lag_features = [
            "congestion_score_lag1", "congestion_score_lag2", "congestion_score_lag24",
            "avg_delay_lag1", "avg_delay_lag2", "avg_delay_lag24",
            "departure_count_lag1", "departure_count_lag2", "departure_count_lag24",
        ]

        rolling_features = [
            "congestion_score_rolling_mean_6h", "congestion_score_rolling_std_6h",
            "avg_delay_rolling_mean_6h", "avg_delay_rolling_std_6h",
        ]

        # -----------------------------
        # Decide if lag24 is possible
        # Need at least 25 hourly rows for a station to populate lag24.
        # -----------------------------
        counts = df.groupby("station_id").size()
        lag24_ok = bool((counts >= 25).any())

        if not lag24_ok:
            lag_features = [c for c in lag_features if not c.endswith("_lag24")]
            print("⚠️ Low history: dropping *_lag24 features (need >=25 hourly rows per station).")

        self.feature_columns = base_features + lag_features + rolling_features
        # Drop NaNs created by lagging/rolling
        df_clean = df.dropna(subset=self.feature_columns + ["congestion_score"]).copy()
        # If still empty, drop lag2 as well (very small dataset)
        if df_clean.empty:
            lag_features2 = [c for c in lag_features if not c.endswith("_lag2") and not c.endswith("_lag24")]
            self.feature_columns = base_features + lag_features2 + rolling_features
            print("⚠️ Still 0 rows after dropna: also dropping *_lag2 features.")
            df_clean = df.dropna(subset=self.feature_columns + ["congestion_score"]).copy()
        # If still empty, drop all lag features (last resort)
        if df_clean.empty:
            self.feature_columns = base_features + rolling_features
            print("⚠️ Still 0 rows after dropna: dropping ALL lag features (training with base+rolling only).")
            df_clean = df.dropna(subset=self.feature_columns + ["congestion_score"]).copy()

        if df_clean.empty:
            max_rows_station = int(counts.max()) if len(counts) else 0
            raise ValueError(
            "0 training rows after feature engineering.\n"
            f"- total rows fetched: {len(df)}\n"
            f"- max rows per station: {max_rows_station}\n"
            "Collect more data (run DLT over time + dbt run), then retry."
            )

        X = df_clean[self.feature_columns]
        y = df_clean["congestion_score"]
        return X, y


    # -----------------------------
    # Training
    # -----------------------------
    def train_model(self, min_days: int = 30) -> Dict[str, Any]:
        print("\n" + "=" * 70)
        print("🤖 TRAINING CONGESTION PREDICTION MODEL")
        print("=" * 70 + "\n")

        print("📊 Extracting features from database...")
        df = self.extract_features(min_days)
        print(f"   ✓ Loaded {len(df):,} records")

        print("🔧 Engineering features...")
        df_engineered = self.engineer_features(df)

        print("📦 Preparing training data...")
        X, y = self.prepare_training_data(df_engineered)
        print(f"   ✓ Training samples: {len(X):,}")
        if len(X) < 10:
            raise ValueError(f"Too few training rows after cleaning: {len(X)}. Collect more data and try again.")

        print(f"   ✓ Features: {len(self.feature_columns)}")

        tscv = TimeSeriesSplit(n_splits=5)

        split_idx = int(len(X) * 0.8)
        X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
        y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)

        print("\n🌲 Training Random Forest Regressor...")
        self.model = RandomForestRegressor(
            n_estimators=250,
            max_depth=20,
            min_samples_split=5,
            min_samples_leaf=2,
            max_features="sqrt",
            random_state=42,
            n_jobs=-1,
        )
        self.model.fit(X_train_scaled, y_train)

        y_pred_train = self.model.predict(X_train_scaled)
        y_pred_test = self.model.predict(X_test_scaled)

        train_mae = mean_absolute_error(y_train, y_pred_train)
        train_rmse = float(np.sqrt(mean_squared_error(y_train, y_pred_train)))
        train_r2 = r2_score(y_train, y_pred_train)

        test_mae = mean_absolute_error(y_test, y_pred_test)
        test_rmse = float(np.sqrt(mean_squared_error(y_test, y_pred_test)))
        test_r2 = r2_score(y_test, y_pred_test)

        cv_scores = cross_val_score(
            self.model,
            X_train_scaled,
            y_train,
            cv=tscv,
            scoring="neg_mean_absolute_error",
            n_jobs=-1,
        )
        cv_mae = float(-cv_scores.mean())

        feature_importance = pd.DataFrame(
            {"feature": self.feature_columns, "importance": self.model.feature_importances_}
        ).sort_values("importance", ascending=False)
        feature_importance.to_csv(self.model_path / "feature_importance.csv", index=False)

        print("\n" + "=" * 70)
        print("📈 MODEL PERFORMANCE")
        print("=" * 70)
        print(f"\n📊 Train MAE: {train_mae:.2f} | RMSE: {train_rmse:.2f} | R²: {train_r2:.4f}")
        print(f"📊 Test  MAE: {test_mae:.2f} | RMSE: {test_rmse:.2f} | R²: {test_r2:.4f}")
        print(f"\n🔄 CV MAE: {cv_mae:.2f} (+/- {cv_scores.std() * 2:.2f})")

        print("\n✅ MODEL TRAINING COMPLETE")

        return {
            "train_mae": float(train_mae),
            "train_r2": float(train_r2),
            "test_mae": float(test_mae),
            "test_rmse": float(test_rmse),
            "test_r2": float(test_r2),
            "cv_mae": float(cv_mae),
            "feature_importance": feature_importance,
        }

    # -----------------------------
    # Persistence
    # -----------------------------
    def save_model(self) -> Path:
        if self.model is None:
            raise ValueError("No model to save. Train the model first.")

        artifacts = ModelArtifacts(
            model=self.model,
            scaler=self.scaler,
            features=self.feature_columns,
            trained_at=datetime.now().isoformat(),
        )
        filepath = self.model_path / "congestion_predictor.pkl"
        joblib.dump(artifacts, filepath)
        print(f"\n💾 Model saved to: {filepath}")
        return filepath

    def load_model(self) -> ModelArtifacts:
        filepath = self.model_path / "congestion_predictor.pkl"
        if not filepath.exists():
            raise FileNotFoundError(f"Model not found at {filepath}")

        artifacts: ModelArtifacts = joblib.load(filepath)
        self.model = artifacts.model
        self.scaler = artifacts.scaler
        self.feature_columns = artifacts.features
        print(f"✅ Model loaded from: {filepath}")
        print(f"   Trained at: {artifacts.trained_at}")
        return artifacts

    # -----------------------------
    # Forecasting
    # -----------------------------
    def predict_next_7_days(self, station_id: Optional[int] = None) -> pd.DataFrame:
        if self.model is None:
            raise ValueError("Model not trained. Call train_model() or load_model() first.")

        table = f"{self.schema}.{self.fact_table}"

        latest_query = f"""
        SELECT 
            station_id,
            station_name,
            congestion_score,
            avg_delay,
            departure_count,
            hour
        FROM {table}
        WHERE hour = (SELECT MAX(hour) FROM {table})
        """
        if station_id is not None:
            latest_query += f" AND station_id = {int(station_id)}"

        with duckdb.connect(self.db_path, read_only=True) as con:
            latest_data = con.execute(latest_query).df()

        if latest_data.empty:
            raise ValueError("No recent data available for predictions")

        start_date = datetime.now()
        predictions: list[dict[str, Any]] = []

        stations_to_predict = [station_id] if station_id is not None else list(STOCKHOLM_STATIONS.keys())

        for sid in stations_to_predict:
            station_name = STOCKHOLM_STATIONS.get(sid, f"Station {sid}")
            station_latest = latest_data[latest_data["station_id"] == sid]
            if station_latest.empty:
                continue

            latest_congestion = float(station_latest["congestion_score"].iloc[0])
            latest_delay = float(station_latest["avg_delay"].iloc[0])
            latest_departures = float(station_latest["departure_count"].iloc[0])

            for day in range(7):
                for hr in range(24):
                    future_dt = start_date + timedelta(days=day, hours=hr)
                    features = self._create_prediction_features(
                        future_dt, sid, latest_congestion, latest_delay, latest_departures
                    )

                    X = pd.DataFrame([features])[self.feature_columns]
                    X_scaled = self.scaler.transform(X)
                    pred_score = float(self.model.predict(X_scaled)[0])
                    pred_score = max(0.0, min(100.0, pred_score))

                    predictions.append(
                        {
                            "timestamp": future_dt,
                            "date": future_dt.date(),
                            "hour": hr,
                            "day_of_week": future_dt.weekday(),
                            "station_id": sid,
                            "station_name": station_name,
                            "predicted_congestion": round(pred_score, 1),
                            "congestion_level": self._get_congestion_level(pred_score),
                        }
                    )
                    latest_congestion = pred_score

        return pd.DataFrame(predictions)

    def save_predictions(self, predictions_df: pd.DataFrame) -> None:
        """Save predictions to DuckDB (DuckDB-safe via dataframe registration)."""
        if predictions_df is None or predictions_df.empty:
            print("⚠️ No predictions to save.")
            return

        expected_cols = [
            "timestamp",
            "date",
            "hour",
            "day_of_week",
            "station_id",
            "station_name",
            "predicted_congestion",
            "congestion_level",
        ]
        missing = [c for c in expected_cols if c not in predictions_df.columns]
        if missing:
            raise ValueError(f"Predictions dataframe missing columns: {missing}")

        df = predictions_df.copy()
        df["generated_at"] = datetime.now()

        df = df[
            [
                "timestamp",
                "date",
                "hour",
                "day_of_week",
                "station_id",
                "station_name",
                "predicted_congestion",
                "congestion_level",
                "generated_at",
            ]
        ]

        target_schema = "main_analytics_marts"

        with duckdb.connect(self.db_path) as con:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema};")
            con.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {target_schema}.congestion_predictions (
                    timestamp TIMESTAMP,
                    date DATE,
                    hour INTEGER,
                    day_of_week INTEGER,
                    station_id INTEGER,
                    station_name VARCHAR,
                    predicted_congestion DOUBLE,
                    congestion_level VARCHAR,
                    generated_at TIMESTAMP,
                    PRIMARY KEY (timestamp, station_id)
                )
                """
            )

            con.register("pred_df", df)
            try:
                con.execute(
                    f"""
                    INSERT OR REPLACE INTO {target_schema}.congestion_predictions
                    SELECT * FROM pred_df
                    """
                )
            finally:
                con.unregister("pred_df")

        print(f"💾 Predictions saved: {len(df):,} rows -> {target_schema}.congestion_predictions")

    # -----------------------------
    # Helpers
    # -----------------------------
    def _create_prediction_features(
        self,
        future_dt: datetime,
        station_id: int,
        latest_congestion: float,
        latest_delay: float,
        latest_departures: float,
    ) -> Dict[str, Any]:
        hour_of_day = future_dt.hour
        day_of_week = future_dt.weekday()

        is_morning_rush = 1 if 7 <= hour_of_day <= 9 else 0
        is_evening_rush = 1 if 16 <= hour_of_day <= 18 else 0
        is_weekend = 1 if day_of_week >= 5 else 0

        if is_morning_rush or is_evening_rush:
            est_departures = latest_departures * 1.5
            est_delay = latest_delay * 1.3
        elif is_weekend:
            est_departures = latest_departures * 0.6
            est_delay = latest_delay * 0.7
        else:
            est_departures = latest_departures
            est_delay = latest_delay

        return {
            "hour_of_day": hour_of_day,
            "day_of_week": day_of_week,
            "day_of_month": future_dt.day,
            "month": future_dt.month,
            "week_of_year": future_dt.isocalendar()[1],
            "quarter": (future_dt.month - 1) // 3 + 1,
            "hour_sin": np.sin(2 * np.pi * hour_of_day / 24),
            "hour_cos": np.cos(2 * np.pi * hour_of_day / 24),
            "day_sin": np.sin(2 * np.pi * day_of_week / 7),
            "day_cos": np.cos(2 * np.pi * day_of_week / 7),
            "month_sin": np.sin(2 * np.pi * future_dt.month / 12),
            "month_cos": np.cos(2 * np.pi * future_dt.month / 12),
            "station_id": station_id,
            "departure_count": est_departures,
            "active_lines": 8,
            "avg_delay": est_delay,
            "delay_variance": 2.0,
            "max_delay": est_delay * 2,
            "delayed_vehicles": 5,
            "metro_count": est_departures * 0.4,
            "bus_count": est_departures * 0.4,
            "train_count": est_departures * 0.15,
            "tram_count": est_departures * 0.05,
            "metro_ratio": 0.4,
            "bus_ratio": 0.4,
            "disruption_count": 1,
            "is_morning_rush": is_morning_rush,
            "is_evening_rush": is_evening_rush,
            "is_weekend": is_weekend,
            "is_any_rush": int(is_morning_rush or is_evening_rush),
            "is_month_end": 1 if future_dt.day >= 28 else 0,
            "is_month_start": 1 if future_dt.day <= 3 else 0,
            "delay_per_vehicle": est_delay / (est_departures + 1),
            "vehicles_per_line": est_departures / 8,
            "congestion_score_lag1": latest_congestion,
            "congestion_score_lag2": latest_congestion,
            "congestion_score_lag24": latest_congestion,
            "avg_delay_lag1": latest_delay,
            "avg_delay_lag2": latest_delay,
            "avg_delay_lag24": latest_delay,
            "departure_count_lag1": latest_departures,
            "departure_count_lag2": latest_departures,
            "departure_count_lag24": latest_departures,
            "congestion_score_rolling_mean_6h": latest_congestion,
            "congestion_score_rolling_std_6h": 5.0,
            "avg_delay_rolling_mean_6h": latest_delay,
            "avg_delay_rolling_std_6h": 1.5,
        }

    def _get_congestion_level(self, score: float) -> str:
        if score < 25:
            return "Low"
        elif score < 50:
            return "Moderate"
        elif score < 75:
            return "High"
        return "Critical"


def train_and_save_model() -> tuple[CongestionPredictor, dict[str, Any], Path]:
    predictor = CongestionPredictor()
    metrics = predictor.train_model(min_days=30)
    model_path = predictor.save_model()
    return predictor, metrics, model_path


def generate_forecast() -> pd.DataFrame:
    predictor = CongestionPredictor()
    predictor.load_model()
    predictions = predictor.predict_next_7_days()
    predictor.save_predictions(predictions)
    return predictions


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "train":
        predictor, metrics, model_path = train_and_save_model()
        print("✅ Model saved to:", model_path)
        print("Test MAE:", metrics["test_mae"])

    elif len(sys.argv) > 1 and sys.argv[1] == "predict":
        df = generate_forecast()
        print("✅ Forecast rows:", len(df))

    else:
        predictor, metrics, model_path = train_and_save_model()
        df = generate_forecast()
        print("✅ Done. Forecast rows:", len(df))
