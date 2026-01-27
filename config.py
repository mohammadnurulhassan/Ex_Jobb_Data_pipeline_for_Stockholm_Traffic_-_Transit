"""
FILE: config.py (project root)
Single source of truth for:
- Env keys
- Paths (DuckDB in ./warehouse)
- Station/site lists
- Default schemas/datasets for DLT + dbt + ML
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root by default
load_dotenv()


# -----------------------------
# Project paths
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parent

WAREHOUSE_DIR = PROJECT_ROOT / "warehouse"
WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

# DuckDB path used by DLT/dbt/ML/Dagster (ONE TRUE DB)
DUCKDB_PATH = WAREHOUSE_DIR / "stockholm_traffic.duckdb"
DUCKDB_DATABASE = str(DUCKDB_PATH)  # backward-compatible name used in your ML code

# Optional: DuckDB destination credentials for DLT (duckdb:///absolute/path)
DUCKDB_CREDENTIALS = f"duckdb:///{DUCKDB_PATH.as_posix()}"


# -----------------------------
# Environment keys
# -----------------------------
# Keep both names because you previously used both
TRAFIKLAB_API_KEY = os.getenv("TRAFIKLAB_API_KEY")  # legacy
REALTIME_API_KEY = os.getenv("REALTIME_API_KEY")    # current

# If you need legacy SL endpoint (you used before and got 404):
TRAFIKLAB_API_URL = "https://api.sl.se/api2/realtimedeparturesV4.json"

# New SL Transport Integration API (your current DLT ingestion)
SL_TRANSPORT_BASE_URL = "https://transport.integration.sl.se/v1"


# -----------------------------
# Sites / Stations
# -----------------------------
# Your list is site_id based (used by SL Transport departures endpoint)
STOCKHOLM_SITES = {
    9001: "T-Centralen",
    9192: "Slussen",
    9204: "Odenplan",
    9302: "Fridhemsplan",
    9303: "Kungsträdgården",
    9506: "Södermalm",
    1080: "Gullmarsplan",
    9190: "Gamla Stan",
    9191: "Medborgarplatsen",
    1051: "Hötorget",
}

# Keep the old name used elsewhere in your project
STOCKHOLM_STATIONS = STOCKHOLM_SITES


# -----------------------------
# DLT settings
# -----------------------------
DLT_PIPELINE_NAME = "stockholm_traffic"
DLT_DESTINATION = "duckdb"
DLT_DATASET_NAME = "raw_traffic"  # schema in DuckDB


# -----------------------------
# dbt settings (adjust to your project)
# -----------------------------
# Your dbt models will likely read from raw_traffic.* and write to analytics.*
DBT_TARGET_SCHEMA = "analytics"
DBT_RAW_SCHEMA = DLT_DATASET_NAME


# -----------------------------
# Collection/runtime settings
# -----------------------------
COLLECTION_INTERVAL_MINUTES = int(os.getenv("COLLECTION_INTERVAL_MINUTES", "5"))
API_TIMEOUT_SECONDS = int(os.getenv("API_TIMEOUT_SECONDS", "20"))
RATE_LIMIT_DELAY_SECONDS = float(os.getenv("RATE_LIMIT_DELAY_SECONDS", "0.3"))
