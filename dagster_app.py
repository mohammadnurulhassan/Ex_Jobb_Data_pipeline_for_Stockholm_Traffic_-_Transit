"""
FILE: dagster_app.py
Dagster Orchestration for Stockholm Traffic (DLT -> dbt -> analytics -> ML)

Run:
  dagster dev -f dagster_app.py

Notes (based on your current project state):
- DLT writes to DuckDB: warehouse/stockholm_traffic.duckdb
- Raw table: raw_traffic.realtime_departures
  - uses columns: site_id, ingestion_timestamp_utc, ...
- dbt builds:
  - analytics_analytics_staging.stg_departures (view)
  - analytics_analytics_marts.fact_* (tables)
  (Because your profile schema is "analytics" and dbt schema config adds suffixes)
"""

from __future__ import annotations

import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

import duckdb
import pandas as pd
from dagster import (
    asset,
    AssetExecutionContext,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    AssetSelection,
    sensor,
    RunRequest,
    SensorEvaluationContext,
    MetadataValue,
    Output,
)
from dagster_duckdb import DuckDBResource

# ✅ Update these imports to match YOUR project structure
# DLT ingestion (you currently run: python -m dlt_pipeline.data_ingestion)
from dlt_pipeline.data_ingestion import run_dlt_pipeline

# If you still want ML parts, keep these. Otherwise comment out the ML assets below.
from ml_models.congestion_predictor import (
    train_and_save_model,
    generate_forecast,
)

# -----------------------------------------------------------------------------
# Project paths / config
# -----------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # adjust if needed
WAREHOUSE_DB = PROJECT_ROOT / "warehouse" / "stockholm_traffic.duckdb"

# dbt project folder (you run dbt inside trafiklab_exjobb/)
DBT_PROJECT_DIR = PROJECT_ROOT / "trafiklab_exjobb"

# If your profiles.yml is in C:\Users\<you>\.dbt (default), you don’t need --profiles-dir.
# But if you want to be explicit, set this:
DBT_PROFILES_DIR = Path.home() / ".dbt"

# DuckDB schema names produced by your dbt setup (confirmed by your dbt output)
STG_SCHEMA = "analytics_analytics_staging"
MART_SCHEMA = "analytics_analytics_marts"

RAW_SCHEMA = "raw_traffic"
RAW_TABLE = f"{RAW_SCHEMA}.realtime_departures"


# =============================================================================
# ASSETS
# =============================================================================

@asset(group_name="ingestion", description="Fetch real-time traffic data via DLT into DuckDB")
def raw_traffic_data(context: AssetExecutionContext) -> Output[dict]:
    context.log.info("🚀 Starting DLT ingestion...")

    try:
        run_dlt_pipeline()  # writes into WAREHOUSE_DB

        conn = duckdb.connect(str(WAREHOUSE_DB))

        total_records = conn.execute(
            f"SELECT COUNT(*) FROM {RAW_TABLE}"
        ).fetchone()[0]

        # Raw uses ingestion_timestamp_utc (VARCHAR in raw); dlt pipeline loads UTC ISO strings.
        # We'll TRY_CAST to timestamptz for the last hour count.
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
                "total_records": total_records,
                "recent_records_1h": recent_records,
                "sites_count_1h": sites_count,
            },
            metadata={
                "duckdb_path": MetadataValue.path(str(WAREHOUSE_DB)),
                "total_records": MetadataValue.int(total_records),
                "recent_records_1h": MetadataValue.int(recent_records),
                "sites_count_1h": MetadataValue.int(sites_count),
                "ingestion_timestamp": MetadataValue.text(datetime.now().isoformat()),
            },
        )

    except Exception as e:
        context.log.error(f"❌ Ingestion failed: {e}")
        raise


@asset(group_name="transformation", deps=[raw_traffic_data], description="Run dbt build to produce staging + marts")
def transformed_traffic_data(context: AssetExecutionContext) -> Output[dict]:
    context.log.info("🔄 Running dbt build...")

    try:
        # Use dbt build (since you now use build in CLI)
        cmd = [
            "dbt", "build",
            "--no-partial-parse",
            "--project-dir", str(DBT_PROJECT_DIR),
            "--profiles-dir", str(DBT_PROFILES_DIR),
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
        )

        # Log a shorter chunk to keep Dagster UI readable
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
                "stg_departures_rows": stg_count,
                "fact_hourly_delays_rows": hourly_count,
                "fact_station_performance_rows": station_count,
                "fact_congestion_score_rows": congestion_count,
            },
            metadata={
                "stg_departures_rows": MetadataValue.int(stg_count),
                "fact_hourly_delays_rows": MetadataValue.int(hourly_count),
                "fact_station_performance_rows": MetadataValue.int(station_count),
                "fact_congestion_score_rows": MetadataValue.int(congestion_count),
                "transformation_timestamp": MetadataValue.text(datetime.now().isoformat()),
            },
        )

    except subprocess.CalledProcessError as e:
        context.log.error("❌ dbt failed")
        context.log.error(e.stdout[-4000:] if e.stdout else "")
        context.log.error(e.stderr[-4000:] if e.stderr else "")
        raise
    except Exception as e:
        context.log.error(f"❌ Transformation failed: {e}")
        raise


@asset(group_name="analytics", deps=[transformed_traffic_data], description="Compute 24h congestion stats from marts")
def congestion_analytics(context: AssetExecutionContext) -> Output[dict]:
    context.log.info("📊 Computing congestion analytics (last 24h)...")

    try:
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

        avg_score, max_score, min_score, total_records, critical_hours, high_hours = conn.execute(stats_query).fetchone()

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

        context.log.info(f"✅ Analytics OK: avg={avg_score:.1f}, max={max_score:.0f}, critical_hours={critical_hours}")

        return Output(
            value={
                "avg_congestion_24h": float(avg_score) if avg_score is not None else None,
                "max_congestion_24h": float(max_score) if max_score is not None else None,
                "critical_hours_24h": int(critical_hours) if critical_hours is not None else 0,
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

    except Exception as e:
        context.log.error(f"❌ Analytics failed: {e}")
        raise


# =============================================================================
# ML ASSETS (optional – keep if you have these modules working)
# =============================================================================

@asset(group_name="ml", deps=[congestion_analytics], description="Train congestion prediction model (weekly)")
def ml_model_training(context: AssetExecutionContext) -> Output[dict]:
    context.log.info("🤖 Training ML model...")

    try:
        predictor, metrics = train_and_save_model()
        context.log.info(f"✅ Model trained. Test MAE={metrics.get('test_mae')}")
        return Output(
            value=metrics,
            metadata={
                "test_mae": MetadataValue.float(float(metrics.get("test_mae", 0.0))),
                "test_r2": MetadataValue.float(float(metrics.get("test_r2", 0.0))),
                "cv_mae": MetadataValue.float(float(metrics.get("cv_mae", 0.0))),
                "training_timestamp": MetadataValue.text(datetime.now().isoformat()),
                "model_path": MetadataValue.path("ml_models/saved_models/congestion_predictor.pkl"),
            },
        )
    except Exception as e:
        context.log.error(f"❌ Model training failed: {e}")
        raise


@asset(group_name="ml", deps=[ml_model_training], description="Generate 7-day congestion predictions (daily)")
def congestion_predictions(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    context.log.info("🔮 Generating forecast...")

    try:
        predictions_df = generate_forecast()

        avg_pred = float(predictions_df["predicted_congestion"].mean())
        max_pred = float(predictions_df["predicted_congestion"].max())
        critical_hours = int((predictions_df["congestion_level"] == "Critical").sum())

        sample_csv = str(PROJECT_ROOT / "predictions_sample.csv")
        predictions_df.head(200).to_csv(sample_csv, index=False)

        context.log.info(f"✅ Forecast OK: rows={len(predictions_df)}, avg={avg_pred:.1f}, max={max_pred:.1f}")

        return Output(
            value=predictions_df,
            metadata={
                "total_predictions": MetadataValue.int(len(predictions_df)),
                "avg_predicted_congestion": MetadataValue.float(avg_pred),
                "max_predicted_congestion": MetadataValue.float(max_pred),
                "critical_hours_predicted": MetadataValue.int(critical_hours),
                "stations_predicted": MetadataValue.int(int(predictions_df["station_id"].nunique()) if "station_id" in predictions_df.columns else 0),
                "prediction_timestamp": MetadataValue.text(datetime.now().isoformat()),
                "sample_predictions": MetadataValue.path(sample_csv),
            },
        )

    except Exception as e:
        context.log.error(f"❌ Forecast failed: {e}")
        raise


# =============================================================================
# JOBS
# =============================================================================

ingestion_and_transformation_job = define_asset_job(
    name="ingestion_and_transformation",
    selection=AssetSelection.groups("ingestion", "transformation", "analytics"),
    description="Ingest + dbt build + analytics (every 5 minutes)",
)

model_training_job = define_asset_job(
    name="model_training",
    selection=AssetSelection.groups("ml").upstream(),
    description="Train ML model (weekly)",
)

prediction_job = define_asset_job(
    name="prediction_generation",
    selection=AssetSelection.assets(congestion_predictions),
    description="Generate 7-day forecasts (daily)",
)


# =============================================================================
# SCHEDULES
# =============================================================================

ingestion_schedule = ScheduleDefinition(
    job=ingestion_and_transformation_job,
    cron_schedule="*/5 * * * *",
    name="ingestion_schedule",
    description="Fetch + transform + analytics every 5 minutes",
)

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


# =============================================================================
# SENSOR (trigger predictions when model file changes)
# =============================================================================

@sensor(
    job=prediction_job,
    name="new_model_sensor",
    description="Trigger prediction generation when a new model is trained",
)
def new_model_trained_sensor(context: SensorEvaluationContext):
    model_path = PROJECT_ROOT / "ml_models" / "saved_models" / "congestion_predictor.pkl"
    if not model_path.exists():
        return

    last_modified = datetime.fromtimestamp(model_path.stat().st_mtime)
    cursor = context.cursor or datetime.min.isoformat()

    if isinstance(cursor, str):
        cursor_dt = datetime.fromisoformat(cursor) if cursor else datetime.min
    else:
        cursor_dt = cursor

    if last_modified > cursor_dt:
        context.log.info("🔔 New model detected -> triggering predictions")

        yield RunRequest(
            run_key=f"model_updated_{last_modified.isoformat()}",
            run_config={},
        )

        context.update_cursor(last_modified.isoformat())


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
        ml_model_training,
        congestion_predictions,
    ],
    jobs=[
        ingestion_and_transformation_job,
        model_training_job,
        prediction_job,
    ],
    schedules=[
        ingestion_schedule,
        model_training_schedule,
        prediction_schedule,
    ],
    sensors=[
        new_model_trained_sensor,
    ],
    resources={
        "duckdb": duckdb_resource,
    },
)


# =============================================================================
# CLI helper
# =============================================================================

def print_pipeline_status():
    print("\n" + "=" * 70)
    print("🚇 STOCKHOLM TRAFFIC ANALYTICS - DAGSTER PIPELINE STATUS")
    print("=" * 70)

    print("\n📦 Assets:")
    print("   1) raw_traffic_data           - DLT ingestion -> DuckDB")
    print("   2) transformed_traffic_data   - dbt build (staging + marts)")
    print("   3) congestion_analytics       - 24h congestion summary")
    print("   4) ml_model_training          - train model (weekly)")
    print("   5) congestion_predictions     - forecast (daily)")

    print("\n⚙️  Jobs:")
    print("   • ingestion_and_transformation  - Every 5 minutes")
    print("   • model_training                - Weekly (Sunday 2 AM)")
    print("   • prediction_generation         - Daily (1 AM)")

    print("\n📅 Schedules:")
    print("   • ingestion_schedule            - */5 * * * *")
    print("   • weekly_model_training         - 0 2 * * 0")
    print("   • daily_prediction_generation   - 0 1 * * *")

    print("\n🔔 Sensor:")
    print("   • new_model_trained_sensor      - Triggers predictions when model file updates")

    print("\nPaths:")
    print(f"   DuckDB: {WAREHOUSE_DB}")
    print(f"   dbt project: {DBT_PROJECT_DIR}")
    print(f"   dbt profiles: {DBT_PROFILES_DIR}")

    print("\n" + "=" * 70)
    print("Run: dagster dev -f dagster_app.py")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    print_pipeline_status()
