"""
FILE: dagster_app.py
Dagster Orchestration for Stockholm Traffic (DLT -> dbt -> analytics -> optional ML)

Run (inside venv):
  source .venv/Scripts/activate
  pip install -U dagster dagster-webserver dagster-duckdb
  dagster dev -f dagster_app.py

Optional ML:
  set ENABLE_ML=1   (PowerShell)  OR  export ENABLE_ML=1 (Git Bash)
"""

import os
import subprocess
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import sys


from dagster import (
    asset,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    AssetSelection,
    sensor,
    RunRequest,
    SensorEvaluationContext,
    MetadataValue,
    Output,
    SkipReason,
)
from dagster_duckdb import DuckDBResource

# -----------------------------------------------------------------------------
# Project paths / config
# -----------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent  # repo root (same folder as dagster_app.py)
WAREHOUSE_DB = PROJECT_ROOT / "warehouse" / "stockholm_traffic.duckdb"

# dbt project folder (you run dbt inside trafiklab_exjobb/)
DBT_PROJECT_DIR = PROJECT_ROOT / "trafiklab_exjobb"
DBT_PROFILES_DIR = Path.home() / ".dbt"

# DuckDB schema names produced by your dbt setup
STG_SCHEMA = "analytics_analytics_staging"
MART_SCHEMA = "analytics_analytics_marts"

RAW_SCHEMA = "raw_traffic"
RAW_TABLE = f"{RAW_SCHEMA}.realtime_departures"

ENABLE_ML = os.getenv("ENABLE_ML", "0") == "1"


# -----------------------------------------------------------------------------
# Imports that depend on your project structure
# -----------------------------------------------------------------------------
from dlt_pipeline.data_ingestion import run_dlt_pipeline

# Optional ML imports (do not break Dagster load if missing)
ML_AVAILABLE = False
train_and_save_model = None
generate_forecast = None
if ENABLE_ML:
    try:
        from ml_models.congestion_predictor import train_and_save_model, generate_forecast

        ML_AVAILABLE = True
    except Exception as e:
        # Dagster should still load; ML assets will be disabled.
        ML_AVAILABLE = False


# =============================================================================
# ASSETS
# =============================================================================

@asset(group_name="ingestion", description="Fetch real-time traffic data via DLT into DuckDB")
def raw_traffic_data(context) -> Output[dict]:
    context.log.info("🚀 Starting DLT ingestion...")

    run_dlt_pipeline()  # writes into WAREHOUSE_DB

    conn = duckdb.connect(str(WAREHOUSE_DB))

    total_records = conn.execute(
        f"SELECT COUNT(*) FROM {RAW_TABLE}"
    ).fetchone()[0]

    # last hour (best effort casting)
    recent_records = conn.execute(f"""
        SELECT COUNT(*)
        FROM {RAW_TABLE}
        WHERE try_cast(ingestion_timestamp_utc as timestamptz)
              >= current_timestamp - interval '1 hour'
    """).fetchone()[0]

    sites_count = conn.execute(f"""
        SELECT COUNT(DISTINCT site_id)
        FROM {RAW_TABLE}
        WHERE try_cast(ingestion_timestamp_utc as timestamptz)
              >= current_timestamp - interval '1 hour'
    """).fetchone()[0]

    conn.close()

    context.log.info(f"✅ Ingestion OK: {recent_records} records (last 1h), total={total_records}")

    return Output(
        value={
            "duckdb_path": str(WAREHOUSE_DB),
            "total_records": int(total_records),
            "recent_records_1h": int(recent_records),
            "sites_count_1h": int(sites_count),
        },
        metadata={
            "duckdb_path": MetadataValue.path(str(WAREHOUSE_DB)),
            "total_records": MetadataValue.int(int(total_records)),
            "recent_records_1h": MetadataValue.int(int(recent_records)),
            "sites_count_1h": MetadataValue.int(int(sites_count)),
            "ingestion_timestamp": MetadataValue.text(datetime.now().isoformat()),
        },
    )


@asset(
    group_name="transformation",
    deps=[raw_traffic_data],
    description="Run dbt build to produce staging + marts",
)
def transformed_traffic_data(context) -> Output[dict]:
    context.log.info("🔄 Running dbt build...")

    cmd = [
        "dbt", "build",
        "--no-partial-parse",
        "--project-dir", str(DBT_PROJECT_DIR),
        "--profiles-dir", str(DBT_PROFILES_DIR),
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        context.log.error("❌ dbt failed")
        context.log.error(result.stdout[-4000:] if result.stdout else "")
        context.log.error(result.stderr[-4000:] if result.stderr else "")
        raise RuntimeError("dbt build failed")

    # Keep logs readable in UI
    context.log.info(result.stdout[-4000:])

    conn = duckdb.connect(str(WAREHOUSE_DB))

    stg_count = conn.execute(
        f"SELECT COUNT(*) FROM {STG_SCHEMA}.stg_departures"
    ).fetchone()[0]

    hourly_count = conn.execute(
        f"SELECT COUNT(*) FROM {MART_SCHEMA}.fact_hourly_delays"
    ).fetchone()[0]

    station_count = conn.execute(
        f"SELECT COUNT(*) FROM {MART_SCHEMA}.fact_station_performance"
    ).fetchone()[0]

    congestion_count = conn.execute(
        f"SELECT COUNT(*) FROM {MART_SCHEMA}.fact_congestion_score"
    ).fetchone()[0]

    conn.close()

    context.log.info("✅ dbt build OK")

    return Output(
        value={
            "stg_departures_rows": int(stg_count),
            "fact_hourly_delays_rows": int(hourly_count),
            "fact_station_performance_rows": int(station_count),
            "fact_congestion_score_rows": int(congestion_count),
        },
        metadata={
            "stg_departures_rows": MetadataValue.int(int(stg_count)),
            "fact_hourly_delays_rows": MetadataValue.int(int(hourly_count)),
            "fact_station_performance_rows": MetadataValue.int(int(station_count)),
            "fact_congestion_score_rows": MetadataValue.int(int(congestion_count)),
            "transformation_timestamp": MetadataValue.text(datetime.now().isoformat()),
        },
    )


@asset(
    group_name="analytics",
    deps=[transformed_traffic_data],
    description="Compute 24h congestion stats from marts",
)
def congestion_analytics(context) -> Output[dict]:
    context.log.info("📊 Computing congestion analytics (last 24h)...")

    conn = duckdb.connect(str(WAREHOUSE_DB))

    stats_query = f"""
    SELECT
        avg(congestion_score) as avg_score,
        max(congestion_score) as max_score,
        min(congestion_score) as min_score,
        count(*) as total_records,
        sum(case when congestion_level = 'Critical' then 1 else 0 end) as critical_hours,
        sum(case when congestion_level = 'High' then 1 else 0 end) as high_hours
    FROM {MART_SCHEMA}.fact_congestion_score
    WHERE hour >= current_timestamp - interval '24 hours'
    """
    row = conn.execute(stats_query).fetchone()
    avg_score, max_score, min_score, total_records, critical_hours, high_hours = row

    worst_stations_query = f"""
    SELECT
        station_name,
        avg(congestion_score) as avg_score
    FROM {MART_SCHEMA}.fact_congestion_score
    WHERE hour >= current_timestamp - interval '24 hours'
    GROUP BY station_name
    ORDER BY avg_score DESC
    LIMIT 5
    """
    worst_stations_df = conn.execute(worst_stations_query).fetchdf()

    conn.close()

    context.log.info(
        f"✅ Analytics OK: avg={avg_score if avg_score is not None else 'NA'}, "
        f"max={max_score if max_score is not None else 'NA'}, critical_hours={critical_hours or 0}"
    )

    return Output(
        value={
            "avg_congestion_24h": float(avg_score) if avg_score is not None else None,
            "max_congestion_24h": float(max_score) if max_score is not None else None,
            "min_congestion_24h": float(min_score) if min_score is not None else None,
            "total_records_24h": int(total_records) if total_records is not None else 0,
            "critical_hours_24h": int(critical_hours) if critical_hours is not None else 0,
            "high_hours_24h": int(high_hours) if high_hours is not None else 0,
            "worst_stations": worst_stations_df.to_dict("records"),
        },
        metadata={
            "avg_congestion_24h": MetadataValue.float(float(avg_score) if avg_score is not None else 0.0),
            "max_congestion_24h": MetadataValue.float(float(max_score) if max_score is not None else 0.0),
            "critical_hours_24h": MetadataValue.int(int(critical_hours) if critical_hours is not None else 0),
            "high_hours_24h": MetadataValue.int(int(high_hours) if high_hours is not None else 0),
            "analysis_timestamp": MetadataValue.text(datetime.now().isoformat()),
        },
    )


# =============================================================================
# OPTIONAL ML ASSETS
# =============================================================================

_ml_assets = []
_ml_jobs = []
_ml_schedules = []
_ml_sensors = []

if ENABLE_ML and ML_AVAILABLE:

    @asset(group_name="ml", deps=[congestion_analytics], description="Train congestion prediction model (weekly)")
    def ml_model_training(context) -> Output[dict]:
        context.log.info("🤖 Training ML model...")

        predictor, metrics, model_path = train_and_save_model()

        context.log.info(f"✅ Model trained. Test MAE={metrics.get('test_mae')}, path={model_path}")

        return Output(
            value=metrics,
            metadata={
                "test_mae": MetadataValue.float(float(metrics.get("test_mae", 0.0))),
                "test_r2": MetadataValue.float(float(metrics.get("test_r2", 0.0))),
                "cv_mae": MetadataValue.float(float(metrics.get("cv_mae", 0.0))),
                "training_timestamp": MetadataValue.text(datetime.now().isoformat()),
                "model_path": MetadataValue.path(str(model_path)),
            },
        )


    @asset(group_name="ml", deps=[ml_model_training], description="Generate 7-day congestion predictions (daily)")
    def congestion_predictions(context) -> Output[pd.DataFrame]:
        context.log.info("🔮 Generating forecast...")

        predictions_df = generate_forecast()

        avg_pred = float(predictions_df["predicted_congestion"].mean())
        max_pred = float(predictions_df["predicted_congestion"].max())
        critical_hours = int((predictions_df["congestion_level"] == "Critical").sum())

        sample_csv = PROJECT_ROOT / "predictions_sample.csv"
        predictions_df.head(200).to_csv(sample_csv, index=False)

        context.log.info(f"✅ Forecast OK: rows={len(predictions_df)}, avg={avg_pred:.1f}, max={max_pred:.1f}")

        return Output(
            value=predictions_df,
            metadata={
                "total_predictions": MetadataValue.int(int(len(predictions_df))),
                "avg_predicted_congestion": MetadataValue.float(avg_pred),
                "max_predicted_congestion": MetadataValue.float(max_pred),
                "critical_hours_predicted": MetadataValue.int(int(critical_hours)),
                "prediction_timestamp": MetadataValue.text(datetime.now().isoformat()),
                "sample_predictions": MetadataValue.path(str(sample_csv)),
            },
        )

    _ml_assets.extend([ml_model_training, congestion_predictions])

    model_training_job = define_asset_job(
        name="model_training",
        selection=AssetSelection.assets(ml_model_training).upstream(),
        description="Train ML model (weekly)",
    )

    prediction_job = define_asset_job(
        name="prediction_generation",
        selection=AssetSelection.assets(congestion_predictions),
        description="Generate 7-day forecasts (daily)",
    )

    _ml_jobs.extend([model_training_job, prediction_job])

    model_training_schedule = ScheduleDefinition(
        job=model_training_job,
        cron_schedule="0 2 * * 0",  # Sunday 02:00
        name="weekly_model_training",
        description="Retrain ML model every Sunday at 2 AM",
    )

    prediction_schedule = ScheduleDefinition(
        job=prediction_job,
        cron_schedule="0 1 * * *",  # Daily 01:00
        name="daily_prediction_generation",
        description="Generate 7-day forecast daily at 1 AM",
    )

    _ml_schedules.extend([model_training_schedule, prediction_schedule])

    @sensor(
        job=prediction_job,
        name="new_model_sensor",
        description="Trigger prediction generation when a new model is trained",
    )
    def new_model_trained_sensor(context: SensorEvaluationContext):
        model_path = PROJECT_ROOT / "ml_models" / "saved_models" / "congestion_predictor.pkl"
        if not model_path.exists():
            yield SkipReason("Model file not found yet.")
            return

        last_modified = datetime.fromtimestamp(model_path.stat().st_mtime)
        cursor = context.cursor or ""

        cursor_dt = datetime.min
        if cursor:
            try:
                cursor_dt = datetime.fromisoformat(cursor)
            except Exception:
                cursor_dt = datetime.min

        if last_modified > cursor_dt:
            yield RunRequest(
                run_key=f"model_updated_{last_modified.isoformat()}",
                run_config={},
            )
            context.update_cursor(last_modified.isoformat())
        else:
            yield SkipReason("No new model file detected.")

    _ml_sensors.append(new_model_trained_sensor)


# =============================================================================
# JOBS (base pipeline)
# =============================================================================

ingestion_and_transformation_job = define_asset_job(
    name="ingestion_and_transformation",
    selection=AssetSelection.groups("ingestion", "transformation", "analytics"),
    description="Ingest + dbt build + analytics (every 5 minutes)",
)

ingestion_schedule = ScheduleDefinition(
    job=ingestion_and_transformation_job,
    cron_schedule="*/5 * * * *",
    name="ingestion_schedule",
    description="Fetch + transform + analytics every 5 minutes",
)


# =============================================================================
# RESOURCES
# =============================================================================
duckdb_resource = DuckDBResource(database=str(WAREHOUSE_DB))


# =============================================================================
# DEFINITIONS
# =============================================================================
defs = Definitions(
    assets=[
        raw_traffic_data,
        transformed_traffic_data,
        congestion_analytics,
        *_ml_assets,
    ],
    jobs=[
        ingestion_and_transformation_job,
        *_ml_jobs,
    ],
    schedules=[
        ingestion_schedule,
        *_ml_schedules,
    ],
    sensors=[
        *_ml_sensors,
    ],
    resources={
        "duckdb": duckdb_resource,
    },
)


if __name__ == "__main__":
    print("✅ Dagster definitions loaded.")
    print(f"PROJECT_ROOT: {PROJECT_ROOT}")
    print(f"WAREHOUSE_DB: {WAREHOUSE_DB}")
    print(f"DBT_PROJECT_DIR: {DBT_PROJECT_DIR}")
    print(f"DBT_PROFILES_DIR: {DBT_PROFILES_DIR}")
    print(f"ENABLE_ML: {ENABLE_ML} | ML_AVAILABLE: {ML_AVAILABLE}")
