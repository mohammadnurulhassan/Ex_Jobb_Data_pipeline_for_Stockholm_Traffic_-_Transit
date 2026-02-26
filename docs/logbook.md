# Project Logbook – Real-Time Data Pipeline for Stockholm Traffic & Transit Analytics

**Student:** Mohammad Nurul Hassan  
**Program:** Data Engineer  
**Start date:** 2025-11-27  
**End date:** 2026-02-26  
**Repository:** [Ex_Jobb_Data_pipeline_for_Stockholm_Traffic_-_Transit](https://github.com/mohammadnurulhassan/Ex_Jobb_Data_pipeline_for_Stockholm_Traffic_-_Transit)  
**Live Dashboard:** http://stockholm-dashboard.swedencentral.azurecontainer.io:8501  
**Dagster UI:** http://stockholm-dagster.swedencentral.azurecontainer.io:3000

---

## 2025-11-27 – Project Kickoff

**Time spent:** ~2–3 hours

**Summary:**
Today I officially started working on my exjobb project:
"Real-Time Data Pipeline for Stockholm Traffic & Transit Analytics".

Even though I submitted the project plan earlier, this is my 2025-11-27 start date.
I feel it is a bit late to start, but I am confident that I can complete the project within the scheduled time and submit both the technical work and the report.

### Tasks completed

- ✅ **Created GitHub repository**
  - Created a new private repo for the project on GitHub.
  - Added a short README with project title and one-sentence description.

- ✅ **Created base folder structure (locally and in repo)**

```text
smart-traffic-stockholm/
  docs/
    logbook.md
  dlt_pipeline/
  dbt_project/
  warehouse/
  dashboard/
  notebooks/
  data_raw/
```

- ✅ **Set up API keys**
  - Registered on Trafiklab and confirmed access to SL real-time departure data.
  - Generated API keys for SL Transport and SL Real-time Information 4.
  - Created `.env` file to store keys securely.
  - Added `.env` to `.gitignore`.

---

## 2025-12 – Infrastructure & Ingestion Setup

**Time spent:** ~15–20 hours

**Summary:**
Focused on building the foundational data ingestion pipeline. Chose DLT (Data Load Tool) as the ingestion framework and DuckDB as the embedded analytical database. Set up the project structure properly and began pulling real data from the Trafiklab API.

### Tasks completed

- ✅ **Selected tech stack**
  - DLT for incremental API ingestion into DuckDB
  - DuckDB as the embedded analytical warehouse
  - dbt-duckdb for SQL transformations
  - Dagster for orchestration
  - Streamlit for the dashboard
  - scikit-learn for ML predictions

- ✅ **Built DLT ingestion pipeline** (`dlt_pipeline/data_ingestion.py`)
  - Connects to Trafiklab SL Realtime API
  - Fetches live departure data for 9 key Stockholm stations
  - Loads incrementally into DuckDB `raw_traffic.realtime_departures`
  - Handles API pagination and rate limits

- ✅ **Defined 9 monitored Stockholm stations** (`config.py`)
  - T-Centralen, Slussen, Gamla Stan, Södermalm, Medborgarplatsen
  - Gullmarsplan, Fridhemsplan, Odenplan, Kungsträdgården

- ✅ **Set up DuckDB warehouse**
  - Local file: `warehouse/stockholm_traffic.duckdb`
  - Schema structure: `raw_traffic` → `analytics_analytics_staging` → `analytics_analytics_marts`

- ✅ **Verified data flow**
  - Confirmed API returns real departure data
  - Confirmed DLT loads data incrementally (no duplicates)
  - First 549 rows of real departure data collected

---

## 2026-01 – dbt Transformations & Dagster Orchestration

**Time spent:** ~20–25 hours

**Summary:**
Built the transformation layer using dbt to convert raw API data into analytics-ready tables. Set up Dagster to orchestrate the full pipeline automatically every 5 minutes.

### Tasks completed

- ✅ **Built dbt project** (`trafiklab_exjobb/`)
  - `stg_departures.sql` — staging model cleaning raw data
  - `fact_hourly_delays.sql` — hourly delay aggregations per station
  - `fact_station_performance.sql` — on-time %, disruption rates per station
  - `fact_congestion_score.sql` — composite congestion score 0–100 per station per hour

- ✅ **Configured dbt profiles** (`trafiklab_exjobb/profiles.yml`)
  - `dev` target for local Windows development
  - `docker` target for containerised deployment (Linux paths)

- ✅ **Built Dagster orchestration** (`dagster_app1.py`)
  - **Asset 1:** `raw_traffic_data` — DLT ingestion from SL API
  - **Asset 2:** `transformed_traffic_data` — dbt build (staging + marts)
  - **Asset 3:** `congestion_analytics` — 24-hour congestion statistics
  - **Schedule:** `ingestion_schedule` — full pipeline every 5 minutes
  - **DuckDB lock retry logic** — 7 attempts with exponential backoff (prevents crashes when Streamlit holds the file lock)

- ✅ **Fixed critical bugs in Dagster pipeline**
  - Missing `_is_duckdb_locked()` helper function (caused NameError)
  - Removed duplicate `run_dlt_pipeline()` call (caused double ingestion + lock)
  - All DuckDB connections wrapped in `try/finally` to release file handles immediately

- ✅ **Verified full pipeline run**
  - DLT ingestion → dbt build → congestion analytics completing successfully
  - Data flowing into all mart tables

---

## 2026-01 (late) – Machine Learning Module

**Time spent:** ~15–20 hours

**Summary:**
Designed and implemented the ML module for predicting congestion 7 days ahead using a Random Forest Regressor. Integrated it into the Dagster pipeline so the model retrains automatically as new data accumulates.

### Tasks completed

- ✅ **Built ML model** (`ml_models/congestion_predictor.py`)
  - Algorithm: Random Forest Regressor (scikit-learn)
  - Target variable: congestion score (0–100)
  - 44 engineered features including time, station, lag, rolling, and calendar features
  - Cross-validation with 5 folds
  - Saves `congestion_predictor.pkl`, `model_metrics.json`, `feature_importance.csv`

- ✅ **Added ML assets to Dagster**
  - `ml_model_training` asset — trains and saves model
  - `congestion_predictions` asset — generates 7-day forecast using saved model
  - `data_volume_sensor` — triggers retraining when `fact_congestion_score` grows by ≥50 rows
  - `daily_model_training` schedule — retrains at 2 AM every day

- ✅ **ML model metrics (first training run)**
  - Training samples: 159 records
  - Features: 44 (dropped `*_lag24` features due to limited history)
  - Model saved to `ml_models/saved_models/congestion_predictor.pkl`

- ✅ **Created `predictions_sample.csv`**
  - Pre-generated fallback CSV so dashboard shows AI predictions even before Dagster has run
  - Updated with real Stockholm station data

---

## 2026-02 (early) – Streamlit Dashboard

**Time spent:** ~25–30 hours

**Summary:**
Built a full production-quality Streamlit dashboard with three tabs: Live Stream, Analysis, and AI Predictions. Designed with custom CSS for a professional appearance suitable for thesis presentation.

### Tasks completed

- ✅ **Dashboard architecture** (`dashboard/app.py`)
  - Auto-reconnects to DuckDB using dynamic table resolver (`resolve_tables()`)
  - DuckDB retry logic (5 attempts, exponential backoff) to handle pipeline lock conflicts
  - Cached queries (TTL 20–120s) to minimise database load
  - Stockholm local time display (Europe/Stockholm timezone)

- ✅ **Hero header**
  - 🟢 LIVE badge + last update timestamp in Stockholm local time
  - Clean design, no clutter

- ✅ **Live Stream tab — KPI cards (top row)**
  - **AVERAGE DELAY** — with dynamic time window label (last 60 min / last 3 hours / all data)
  - **CONGESTION LEVEL** — network-wide severity (Low / Medium / High / Critical)
  - **🚌 DISRUPTION RATE** — % of active lines with live deviations
  - **🚨 WORST STATION NOW** — station with highest average delay in last 30 min

- ✅ **Live Stream tab — insight cards (bottom row)**
  - **🏥 NETWORK HEALTH** — composite score: 40% on-time + 35% station coverage + 25% deviation-free
  - **📈 DELAY TREND** — improving / stable / worsening compared to 5 min ago

- ✅ **Live Stream tab — charts**
  - Peak Hour Heatmap (station × hour congestion matrix) with slim colorbar
  - Live Departure Feed table (most recent departures with delay status)

- ✅ **Analysis tab**
  - 3 network KPI cards: Network On-Time %, Severe Delay Rate, 95th Percentile
  - Delay Band Breakdown — 100% stacked bar chart per station (6 severity bands)
  - Station Deep-Dive — dropdown to select station, shows 4 metrics + donut chart

- ✅ **AI Predictions tab**
  - 7-day congestion forecast chart per station
  - ML accuracy card (MAE, R², accuracy %, training timestamp, feature count)
  - Feature importance horizontal bar chart
  - Prediction confidence display

- ✅ **Tab order:** 🔴 Live Stream | 📋 Analysis | 🤖 AI Predictions

- ✅ **Fixed multiple dashboard bugs**
  - `UnboundLocalError` — removed local `from config import STOCKHOLM_STATIONS` inside functions
  - `NameError` — restored accidentally deleted `get_predictions()` function
  - Network Health showing 0% — fixed DuckDB boolean comparison (`is_delayed = 0` not `FALSE`)
  - Time display 1 hour behind — added `TZ=Europe/Stockholm` environment variable

---

## 2026-02 (mid) – Docker Containerisation

**Time spent:** ~8–10 hours

**Summary:**
Containerised the entire project into two Docker services so the pipeline and dashboard can run consistently on any machine and be deployed to Azure.

### Tasks completed

- ✅ **Created `Dockerfile.dashboard`**
  - Base: `python:3.11-slim`
  - Installs all requirements, copies project files
  - Starts Streamlit on port 8501

- ✅ **Created `Dockerfile.dagster`**
  - Base: `python:3.11-slim`
  - Copies `trafiklab_exjobb/profiles.yml` to `/root/.dbt/profiles.yml` so dbt works inside container
  - Starts Dagster on port 3000

- ✅ **Created `docker-compose.yml`**
  - Two services: `stockholm-dashboard` and `stockholm-dagster`
  - Explicit `image:` names (lowercase) to avoid Docker naming errors from long folder name
  - Shared volumes: `./warehouse` and `./ml_models/saved_models`
  - Environment variables: `ENABLE_ML=1`, `DBT_TARGET=docker`, `TZ=Europe/Stockholm`

- ✅ **Fixed Docker issues encountered**
  - `invalid reference format` — added explicit `image:` with lowercase names
  - `dbt build failed` — profiles.yml not inside Docker image, fixed by copying into `trafiklab_exjobb/`
  - `environment must be a mapping` — changed from list format (`- KEY=value`) to mapping format (`KEY: value`)
  - `The volume mount path cannot contain ':'` — used `//app/warehouse` (double slash) in Git Bash
  - `InvalidOsType` — added `--os-type Linux` flag

- ✅ **Verified full Docker pipeline**
  - Both containers building and running successfully
  - DLT ingestion working inside container
  - dbt transformations completing successfully
  - ML model training running inside container
  - Dashboard accessible at `http://localhost:8501`
  - Dagster UI accessible at `http://localhost:3000`

---

## 2026-02-25/26 – Azure Cloud Deployment

**Time spent:** ~6–8 hours

**Summary:**
Deployed the full containerised application to Microsoft Azure using Azure Container Instances and Azure File Share for persistent DuckDB storage. The project is now publicly accessible from the internet.

### Azure resources created

| Resource | Name | Purpose |
|---|---|---|
| Resource Group | `stockholm-traffic-live-rg` | Container for all resources |
| Container Registry | `stockholmtraffilivecacr` | Docker image storage |
| Storage Account | `stockholmtraffilivecsa` | DuckDB persistent storage |
| File Share | `duckdb-share` | Shared volume between containers |
| Container Instance | `streamlit-dashboard` | Public dashboard (1 CPU, 2GB) |
| Container Instance | `dagster-pipeline` | Pipeline orchestration (2 CPU, 4GB) |

### Tasks completed

- ✅ **Created Azure Container Registry (ACR)**
  - Built and pushed both Docker images to ACR

- ✅ **Created Azure Storage Account + File Share**
  - Uploaded `stockholm_traffic.duckdb` to Azure File Share
  - Uploaded ML model files (`congestion_predictor.pkl`, `model_metrics.json`)

- ✅ **Deployed both containers to Azure Container Instances**
  - Both containers mount the same Azure File Share at `/app/warehouse`
  - Public IP addresses assigned to both containers
  - `TZ=Europe/Stockholm` set so timestamps display correctly

- ✅ **Fixed Azure deployment issues**
  - `^ line continuation` doesn't work in Git Bash — switched to `\`
  - `--registry-password` flag was missing in first attempt
  - `--os-type Linux` required to avoid `InvalidOsType` error
  - `//app/warehouse` (double slash) required in Git Bash to prevent Windows path conversion
  - `--ip-address Public` required to expose port to internet (without it, container runs but is unreachable)

- ✅ **Verified deployment**
  - Both containers show state: `"Running"`
  - Health check: `curl http://stockholm-dashboard.swedencentral.azurecontainer.io:8501/_stcore/health` returns `ok`
  - Dashboard publicly accessible in browser
  - Dagster UI accessible and running schedules + sensors

### Live URLs
- **Dashboard:** http://stockholm-dashboard.swedencentral.azurecontainer.io:8501
- **Dagster:** http://stockholm-dagster.swedencentral.azurecontainer.io:3000

---

## 2026-02-26 – Final Cleanup & Documentation

**Time spent:** ~3–4 hours

**Summary:**
Cleaned up the repository, removed temporary files, wrote comprehensive README and updated the project logbook.

### Tasks completed

- ✅ **Cleaned Dagster temp folders**
  - Removed `.tmp_dagster_home_*` folders from project root
  - Created proper `.dagster/dagster.yaml` config
  - Added `DAGSTER_HOME=.dagster` to `.env`

- ✅ **Removed debug/diagnostic scripts from `scripts/` folder**
  - `check_db.py` (row counter) — no longer needed
  - `explore_schema.py` (schema explorer) — no longer needed

- ✅ **Removed old `dagster_app.py`**
  - Only `dagster_app1.py` is the active pipeline file

- ✅ **Updated `.gitignore`**
  - Added `warehouse/` (DuckDB binary should not be in git)
  - Added `ml_models/saved_models/*.pkl`
  - Added `.tmp_dagster_home_*/`
  - Added `.dagster/storage/`, `.dagster/history/`, `.dagster/logs/`

- ✅ **Wrote comprehensive README.md**
  - Architecture diagram
  - Full feature documentation for all 3 dashboard tabs
  - Tech stack with links
  - Project folder structure
  - Getting started guide (local + Docker)
  - Pipeline details, ML documentation
  - Docker and Azure deployment instructions
  - Troubleshooting guide

- ✅ **Rotated exposed credentials**
  - Regenerated ACR password after it appeared in terminal output
  - Regenerated Azure Storage Account key

---

## Final Project Summary

### What was built

A complete end-to-end data engineering system for Stockholm public transit:

```
SL Trafiklab API (real-time departures)
        ↓ every 5 minutes
DLT ingestion pipeline
        ↓
DuckDB warehouse (Azure File Share)
        ↓
dbt transformations
  → stg_departures
  → fact_hourly_delays
  → fact_station_performance
  → fact_congestion_score
        ↓
Congestion analytics (24h window)
        ↓
Random Forest ML model → 7-day forecast
        ↓
Streamlit dashboard (3 tabs, live public URL)

ORCHESTRATION: Dagster (5-min schedule + data volume sensor + daily ML retrain)
DEPLOYMENT:    Azure Container Instances (2 containers, always-on)
```

### Key technical achievements

| Achievement | Detail |
|---|---|
| Real-time pipeline | Fetches SL departure data every 5 minutes, 24/7 |
| Data volume | 137,650+ total records ingested as of deployment |
| ML model | Random Forest with 44 features, trained on real Stockholm data |
| Dashboard | 3-tab Streamlit app with live KPIs, heatmap, delay analysis, AI predictions |
| Containerisation | Full Docker setup with 2 services, shared persistent volume |
| Cloud deployment | Live on Azure, publicly accessible from anywhere |
| Resilience | DuckDB lock retry logic, fallback CSV, cached queries |
| Timezone handling | Correct Stockholm local time (UTC+1/+2 DST aware) |

### Technologies used

| Layer | Technology |
|---|---|
| Ingestion | DLT (Data Load Tool) |
| Storage | DuckDB |
| Transformation | dbt-duckdb |
| Orchestration | Dagster |
| Machine Learning | scikit-learn (Random Forest) |
| Dashboard | Streamlit + Plotly |
| Containerisation | Docker + Docker Compose |
| Cloud | Azure Container Instances + Azure File Share |
| Language | Python 3.11 |

### Total estimated hours

| Phase | Hours |
|---|---|
| Project setup & API integration | 2–3 |
| DLT ingestion pipeline | 15–20 |
| dbt transformations | 10–12 |
| Dagster orchestration | 10–12 |
| Machine learning module | 15–20 |
| Streamlit dashboard | 25–30 |
| Docker containerisation | 8–10 |
| Azure deployment | 6–8 |
| Debugging & fixes | 10–15 |
| Documentation & cleanup | 6–8 |
| **Total** | **~140-150 hours** |