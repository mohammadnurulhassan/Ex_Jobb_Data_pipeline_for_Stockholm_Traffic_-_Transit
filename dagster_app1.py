"""
FILE: dagster_app1.py
Dagster Orchestration for Stockholm Traffic (DLT -> dbt -> analytics -> optional ML)

FIXES APPLIED:
  [F1] Added missing _is_duckdb_locked() helper (was called but never defined → NameError).
  [F2] Removed duplicate run_dlt_pipeline() call that always ran after the retry loop.
  [F3] All DuckDB connections now use try/finally: conn.close() so the file lock is
       released immediately — even on error. This stops Streamlit from starving DLT.
  [F4] Retry window widened: 7 attempts × 5 s back-off (max ~35 s wait) instead of
       5 attempts × 3 s, giving Streamlit more time to release the handle between tries.

Run (inside venv):
  source .venv/Scripts/activate          # Windows
  dagster dev -f dagster_app1.py
  
Optional ML:
  set ENABLE_ML=1   (PowerShell)  OR  export ENABLE_ML=1 (Git Bash / Linux)
"""

from __future__ import annotations

import os
import subprocess
import time
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
from dagster import (
    AssetSelection,
    Definitions,
    MetadataValue,
    Output,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext,
    SkipReason,
    asset,
    define_asset_job,
    sensor,
)
from dagster_duckdb import DuckDBResource

# =============================================================================
# Project paths / config
# =============================================================================
PROJECT_ROOT   = Path(__file__).resolve().parent
WAREHOUSE_DB   = PROJECT_ROOT / "warehouse" / "stockholm_traffic.duckdb"
DBT_PROJECT_DIR = PROJECT_ROOT / "trafiklab_exjobb"
DBT_PROFILES_DIR = Path.home() / ".dbt"

STG_SCHEMA  = "analytics_analytics_staging"
MART_SCHEMA = "analytics_analytics_marts"
RAW_SCHEMA  = "raw_traffic"
RAW_TABLE   = f"{RAW_SCHEMA}.realtime_departures"

ENABLE_ML = os.getenv("ENABLE_ML", "0") == "1"

# =============================================================================
# Project imports
# =============================================================================
from dlt_pipeline.data_ingestion import run_dlt_pipeline  # noqa: E402

ML_AVAILABLE         = False
train_and_save_model = None
generate_forecast    = None

if ENABLE_ML:
    try:
        from ml_models.congestion_predictor import (  # noqa: E402
            generate_forecast,
            train_and_save_model,
        )
        ML_AVAILABLE = True
    except Exception:
        ML_AVAILABLE = False


# =============================================================================
# Helpers
# =============================================================================

def _run_cmd(context, cmd: list[str]) -> None:
    """Run a subprocess and raise on failure, streaming trimmed output to Dagster UI."""
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        context.log.error("❌ Command failed: " + " ".join(cmd))
        if result.stdout:
            context.log.error(result.stdout[-4_000:])
        if result.stderr:
            context.log.error(result.stderr[-4_000:])
        raise RuntimeError("Subprocess command failed — see logs above.")
    if result.stdout:
        context.log.info(result.stdout[-4_000:])


# ─────────────────────────────────────────────────────────────────────────────
# FIX 1 ▸ _is_duckdb_locked was called in the retry loop but NEVER DEFINED.
#          This caused an immediate NameError on the first lock exception,
#          making all 5 retry attempts crash before even sleeping once.
# ─────────────────────────────────────────────────────────────────────────────
def _is_duckdb_locked(exc: Exception) -> bool:
    """Return True when exc indicates a DuckDB exclusive-lock conflict."""
    msg = str(exc).lower()
    return any(
        kw in msg
        for kw in (
            "database is locked",
            "conflicting lock",
            "cannot access the file because it is being used",
            "being used by another process",
            "lock",
        )
    )


def _con(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """
    Open a fresh DuckDB connection.

    FIX 3 ▸ Always open a NEW connection per query and close it in a
             try/finally block at the call site.  Never cache connections
             across queries — that keeps the file handle open permanently
             and blocks DLT's write access (the root cause of the lock error).
    """
    return duckdb.connect(str(WAREHOUSE_DB), read_only=read_only)


def _scalar(conn: duckdb.DuckDBPyConnection, sql: str) -> object:
    """Execute a scalar query and return the single value."""
    return conn.execute(sql).fetchone()[0]


def _max_hour(conn: duckdb.DuckDBPyConnection, table: str) -> pd.Timestamp | None:
    try:
        return _scalar(conn, f"SELECT max(hour) FROM {table}")
    except Exception:
        return None


# =============================================================================
# ASSETS
# =============================================================================

@asset(
    group_name="ingestion",
    description="Fetch real-time SL departure data via DLT and load into DuckDB.",
)
def raw_traffic_data(context) -> Output[dict]:
    context.log.info("🚀 Starting DLT ingestion...")

    # ─────────────────────────────────────────────────────────────────────────
    # FIX 4 ▸ Wider retry window: 7 attempts × 5 s back-off (max ~35 s total).
    #          Old setting (5 × 3 s = 15 s max) was shorter than a single
    #          Streamlit auto-refresh cycle, so the lock was never released
    #          in time and every retry failed.
    # FIX 2 ▸ Removed the unconditional second run_dlt_pipeline() call that
    #          existed after this loop in the original code.  The loop already
    #          breaks on success — calling it again caused double ingestion and
    #          a lock error on the second call (which had no retry logic).
    # ─────────────────────────────────────────────────────────────────────────
    max_attempts = 7
    for attempt in range(1, max_attempts + 1):
        try:
            run_dlt_pipeline()
            context.log.info("✅ DLT pipeline completed successfully.")
            break                          # ← success; do NOT call again after loop
        except Exception as exc:
            if _is_duckdb_locked(exc) and attempt < max_attempts:
                wait = attempt * 5         # 5, 10, 15, 20, 25, 30 s
                context.log.warning(
                    f"🔒 DuckDB locked (attempt {attempt}/{max_attempts}). "
                    f"Waiting {wait}s before retry…"
                )
                time.sleep(wait)
                continue
            if _is_duckdb_locked(exc):
                raise SkipReason(
                    "DuckDB file is still locked after 7 attempts (Streamlit or dbt is "
                    "holding it open). The next scheduled run will retry automatically. "
                    "To fix immediately: close Streamlit and re-run the schedule."
                )
            raise   # non-lock exception — propagate as a real failure

    # ─────────────────────────────────────────────────────────────────────────
    # FIX 3 ▸ Short-lived read-only connection wrapped in try/finally so the
    #          file handle is released the moment we finish reading stats.
    # ─────────────────────────────────────────────────────────────────────────
    conn = _con(read_only=True)
    try:
        total_records = _scalar(conn, f"SELECT COUNT(*) FROM {RAW_TABLE}")

        recent_records = _scalar(conn, f"""
            SELECT COUNT(*)
            FROM   {RAW_TABLE}
            WHERE  try_cast(ingestion_timestamp_utc AS TIMESTAMPTZ)
                   >= current_timestamp - INTERVAL '1 hour'
        """)

        sites_count = _scalar(conn, f"""
            SELECT COUNT(DISTINCT site_id)
            FROM   {RAW_TABLE}
            WHERE  try_cast(ingestion_timestamp_utc AS TIMESTAMPTZ)
                   >= current_timestamp - INTERVAL '1 hour'
        """)

        last_ingestion = _scalar(conn, f"""
            SELECT max(try_cast(ingestion_timestamp_utc AS TIMESTAMPTZ))
            FROM   {RAW_TABLE}
        """)
    finally:
        conn.close()   # ← always release the file lock

    context.log.info(
        f"📦 Ingestion stats: recent_1h={recent_records}, total={total_records}, "
        f"sites_1h={sites_count}, last_ts={last_ingestion}"
    )

    return Output(
        value={
            "duckdb_path":        str(WAREHOUSE_DB),
            "total_records":      int(total_records),
            "recent_records_1h":  int(recent_records),
            "sites_count_1h":     int(sites_count),
            "last_ingestion_ts":  str(last_ingestion) if last_ingestion else None,
        },
        metadata={
            "duckdb_path":        MetadataValue.path(str(WAREHOUSE_DB)),
            "total_records":      MetadataValue.int(int(total_records)),
            "recent_records_1h":  MetadataValue.int(int(recent_records)),
            "sites_count_1h":     MetadataValue.int(int(sites_count)),
            "last_ingestion_ts":  MetadataValue.text(str(last_ingestion) if last_ingestion else "None"),
            "ingestion_timestamp": MetadataValue.text(datetime.now().isoformat()),
        },
    )


@asset(
    group_name="transformation",
    deps=[raw_traffic_data],
    description="Run dbt build to materialise staging views and mart tables.",
)
def transformed_traffic_data(context) -> Output[dict]:
    context.log.info("🔄 Running dbt build…")

    _run_cmd(context, [
        "dbt", "build",
        "--no-partial-parse",
        "--project-dir",  str(DBT_PROJECT_DIR),
        "--profiles-dir", str(DBT_PROFILES_DIR),
    ])

    # FIX 3 ▸ Short-lived connection; closed in finally block.
    conn = _con(read_only=True)
    try:
        stg_count       = _scalar(conn, f"SELECT COUNT(*) FROM {STG_SCHEMA}.stg_departures")
        hourly_count    = _scalar(conn, f"SELECT COUNT(*) FROM {MART_SCHEMA}.fact_hourly_delays")
        station_count   = _scalar(conn, f"SELECT COUNT(*) FROM {MART_SCHEMA}.fact_station_performance")
        congestion_count = _scalar(conn, f"SELECT COUNT(*) FROM {MART_SCHEMA}.fact_congestion_score")
        max_hourly      = _scalar(conn, f"SELECT max(hour) FROM {MART_SCHEMA}.fact_hourly_delays")
        max_cong        = _scalar(conn, f"SELECT max(hour) FROM {MART_SCHEMA}.fact_congestion_score")
    finally:
        conn.close()

    context.log.info(
        f"✅ dbt build OK — stg={stg_count}, hourly={hourly_count}, "
        f"station={station_count}, congestion={congestion_count}"
    )

    return Output(
        value={
            "stg_departures_rows":      int(stg_count),
            "fact_hourly_delays_rows":  int(hourly_count),
            "fact_station_perf_rows":   int(station_count),
            "fact_congestion_rows":     int(congestion_count),
            "max_hour_hourly":          str(max_hourly) if max_hourly else None,
            "max_hour_congestion":      str(max_cong)   if max_cong   else None,
        },
        metadata={
            "stg_departures_rows":      MetadataValue.int(int(stg_count)),
            "fact_hourly_delays_rows":  MetadataValue.int(int(hourly_count)),
            "fact_station_perf_rows":   MetadataValue.int(int(station_count)),
            "fact_congestion_rows":     MetadataValue.int(int(congestion_count)),
            "max_hour_hourly":          MetadataValue.text(str(max_hourly) if max_hourly else "None"),
            "max_hour_congestion":      MetadataValue.text(str(max_cong)   if max_cong   else "None"),
            "transformation_timestamp": MetadataValue.text(datetime.now().isoformat()),
        },
    )


@asset(
    group_name="analytics",
    deps=[transformed_traffic_data],
    description="Compute 24-hour congestion statistics anchored to the latest available hour.",
)
def congestion_analytics(context) -> Output[dict]:
    context.log.info("📊 Computing congestion analytics (last 24 h of available data)…")

    table = f"{MART_SCHEMA}.fact_congestion_score"

    # FIX 3 ▸ Short-lived connection; closed in finally block.
    conn = _con(read_only=True)
    try:
        max_hour = _max_hour(conn, table)
        if not max_hour:
            raise RuntimeError(
                f"No data found in {table}. Did dbt build succeed? "
                "Check transformed_traffic_data logs."
            )

        stats_row = conn.execute(f"""
            WITH mx AS (SELECT max(hour) AS mh FROM {table})
            SELECT
                avg(congestion_score)                                          AS avg_score,
                max(congestion_score)                                          AS max_score,
                min(congestion_score)                                          AS min_score,
                count(*)                                                       AS total_records,
                sum(CASE WHEN congestion_level = 'Critical' THEN 1 ELSE 0 END) AS critical_hours,
                sum(CASE WHEN congestion_level = 'High'     THEN 1 ELSE 0 END) AS high_hours
            FROM {table}
            WHERE hour >= (SELECT mh FROM mx) - INTERVAL '24 hours'
        """).fetchone()

        worst_df = conn.execute(f"""
            WITH mx AS (SELECT max(hour) AS mh FROM {table})
            SELECT
                station_name,
                avg(congestion_score) AS avg_score
            FROM {table}
            WHERE hour >= (SELECT mh FROM mx) - INTERVAL '24 hours'
            GROUP BY station_name
            ORDER BY avg_score DESC
            LIMIT 5
        """).fetchdf()
    finally:
        conn.close()

    avg_score, max_score, min_score, total_records, critical_hours, high_hours = stats_row

    context.log.info(
        f"✅ Analytics OK: latest_hour={max_hour}, avg={avg_score}, "
        f"max={max_score}, critical_h={critical_hours}"
    )

    return Output(
        value={
            "latest_hour":        str(max_hour),
            "avg_congestion_24h": float(avg_score)     if avg_score     is not None else None,
            "max_congestion_24h": float(max_score)     if max_score     is not None else None,
            "min_congestion_24h": float(min_score)     if min_score     is not None else None,
            "total_records_24h":  int(total_records)   if total_records is not None else 0,
            "critical_hours_24h": int(critical_hours)  if critical_hours is not None else 0,
            "high_hours_24h":     int(high_hours)      if high_hours    is not None else 0,
            "worst_stations":     worst_df.to_dict("records"),
        },
        metadata={
            "latest_hour":        MetadataValue.text(str(max_hour)),
            "avg_congestion_24h": MetadataValue.float(float(avg_score)    if avg_score    is not None else 0.0),
            "max_congestion_24h": MetadataValue.float(float(max_score)    if max_score    is not None else 0.0),
            "critical_hours_24h": MetadataValue.int(int(critical_hours)   if critical_hours is not None else 0),
            "high_hours_24h":     MetadataValue.int(int(high_hours)       if high_hours   is not None else 0),
            "analysis_timestamp": MetadataValue.text(datetime.now().isoformat()),
        },
    )


# =============================================================================
# OPTIONAL ML ASSETS
# =============================================================================

_ml_assets:    list = []
_ml_jobs:      list = []
_ml_schedules: list = []
_ml_sensors:   list = []

if ENABLE_ML and ML_AVAILABLE:

    @asset(
        group_name="ml",
        deps=[congestion_analytics],
        description="Train the congestion prediction model (runs weekly).",
    )
    def ml_model_training(context) -> Output[dict]:
        context.log.info("🤖 Training ML model…")
        _predictor, metrics, model_path = train_and_save_model()
        context.log.info(
            f"✅ Model trained — MAE={metrics.get('test_mae')}, R²={metrics.get('test_r2')}, "
            f"path={model_path}"
        )
        return Output(
            value=metrics,
            metadata={
                "test_mae":           MetadataValue.float(float(metrics.get("test_mae", 0.0))),
                "test_r2":            MetadataValue.float(float(metrics.get("test_r2",  0.0))),
                "cv_mae":             MetadataValue.float(float(metrics.get("cv_mae",   0.0))),
                "training_timestamp": MetadataValue.text(datetime.now().isoformat()),
                "model_path":         MetadataValue.path(str(model_path)),
            },
        )

    @asset(
        group_name="ml",
        deps=[ml_model_training],
        description="Generate 7-day congestion forecast (runs daily).",
    )
    def congestion_predictions(context) -> Output[pd.DataFrame]:
        context.log.info("🔮 Generating 7-day forecast…")
        predictions_df = generate_forecast()

        avg_pred      = float(predictions_df["predicted_congestion"].mean())
        max_pred      = float(predictions_df["predicted_congestion"].max())
        critical_hours = int((predictions_df["congestion_level"] == "Critical").sum())

        sample_csv = PROJECT_ROOT / "predictions_sample.csv"
        predictions_df.to_csv(sample_csv, index=False)

        context.log.info(
            f"✅ Forecast OK — rows={len(predictions_df)}, avg={avg_pred:.1f}, "
            f"max={max_pred:.1f}, critical_h={critical_hours}"
        )
        return Output(
            value=predictions_df,
            metadata={
                "total_predictions":         MetadataValue.int(len(predictions_df)),
                "avg_predicted_congestion":  MetadataValue.float(avg_pred),
                "max_predicted_congestion":  MetadataValue.float(max_pred),
                "critical_hours_predicted":  MetadataValue.int(critical_hours),
                "prediction_timestamp":      MetadataValue.text(datetime.now().isoformat()),
                "sample_predictions":        MetadataValue.path(str(sample_csv)),
            },
        )

    _ml_assets.extend([ml_model_training, congestion_predictions])

    model_training_job = define_asset_job(
        name="model_training",
        selection=AssetSelection.assets(ml_model_training).upstream(),
        description="Retrain ML model (weekly).",
    )
    prediction_job = define_asset_job(
        name="prediction_generation",
        selection=AssetSelection.assets(congestion_predictions),
        description="Generate 7-day forecast (daily).",
    )
    _ml_jobs.extend([model_training_job, prediction_job])

    _ml_schedules.extend([
        ScheduleDefinition(
            job=model_training_job,
            cron_schedule="0 2 * * 0",   # Sunday 02:00
            name="weekly_model_training",
            description="Retrain ML model every Sunday at 2 AM.",
        ),
        ScheduleDefinition(
            job=prediction_job,
            cron_schedule="0 1 * * *",   # Daily 01:00
            name="daily_prediction_generation",
            description="Generate 7-day forecast daily at 1 AM.",
        ),
    ])

    @sensor(
        job=prediction_job,
        name="new_model_sensor",
        description="Trigger forecast generation whenever the model file changes.",
    )
    def new_model_trained_sensor(context: SensorEvaluationContext):
        model_path = PROJECT_ROOT / "ml_models" / "saved_models" / "congestion_predictor.pkl"
        if not model_path.exists():
            yield SkipReason("Model file not found yet.")
            return

        last_modified = datetime.fromtimestamp(model_path.stat().st_mtime)
        cursor_dt = datetime.min
        if context.cursor:
            try:
                cursor_dt = datetime.fromisoformat(context.cursor)
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
# JOBS & SCHEDULES (base pipeline)
# =============================================================================

ingestion_and_transformation_job = define_asset_job(
    name="ingestion_and_transformation",
    selection=AssetSelection.groups("ingestion", "transformation", "analytics"),
    description="Full pipeline: DLT ingest → dbt build → congestion analytics (every 5 min).",
)

ingestion_schedule = ScheduleDefinition(
    job=ingestion_and_transformation_job,
    cron_schedule="*/5 * * * *",
    name="ingestion_schedule",
    description="Run full pipeline every 5 minutes.",
)

# =============================================================================
# RESOURCES & DEFINITIONS
# =============================================================================

duckdb_resource = DuckDBResource(database=str(WAREHOUSE_DB))

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

# =============================================================================
# Entrypoint sanity check
# =============================================================================
if __name__ == "__main__":
    print("✅ Dagster definitions loaded successfully.")
    print(f"   PROJECT_ROOT    : {PROJECT_ROOT}")
    print(f"   WAREHOUSE_DB    : {WAREHOUSE_DB}")
    print(f"   DBT_PROJECT_DIR : {DBT_PROJECT_DIR}")
    print(f"   DBT_PROFILES_DIR: {DBT_PROFILES_DIR}")
    print(f"   ENABLE_ML       : {ENABLE_ML}  |  ML_AVAILABLE: {ML_AVAILABLE}")