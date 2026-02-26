
# 📡 Stockholm Traffic Analytics
### AI-Powered Real-Time Transit Intelligence Platform

[![Python](https://img.shields.io/badge/Python-3.11-blue)](https://python.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.32+-red)](https://streamlit.io)
[![Dagster](https://img.shields.io/badge/Dagster-1.6+-purple)](https://dagster.io)
[![dbt](https://img.shields.io/badge/dbt-1.7+-orange)](https://getdbt.com)
[![DuckDB](https://img.shields.io/badge/DuckDB-0.10+-yellow)](https://duckdb.org)
[![Docker](https://img.shields.io/badge/Docker-Containerised-blue)](https://docker.com)
[![Azure](https://img.shields.io/badge/Azure-Deployed-0078D4)](https://azure.microsoft.com)

> ** Data Engineer ** — Examensarbete,STI, Sweden  
> A production-grade data pipeline that ingests real-time Stockholm public transit data, transforms it with dbt, trains a machine learning model for congestion prediction, and serves live analytics through a Streamlit dashboard — all orchestrated by Dagster and deployed to Azure.

---

## 🌐 Live Demo

| Service | URL |
|---|---|
| 📊 Live Dashboard | http://stockholm-dashboard.swedencentral.azurecontainer.io:8501 |
| ⚙️ Dagster Pipeline UI | http://stockholm-dagster.swedencentral.azurecontainer.io:3000 |

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [Pipeline Details](#pipeline-details)
- [Machine Learning](#machine-learning)
- [Dashboard](#dashboard)
- [Docker Deployment](#docker-deployment)
- [Azure Deployment](#azure-deployment)
- [API Keys](#api-keys)

---

## Overview

Stockholm's public transit network (operated by SL — Storstockholms Lokaltrafik) handles millions of journeys daily. This project builds a complete **end-to-end data engineering pipeline** that:

1. **Ingests** real-time departure data from the Trafiklab SL API every 5 minutes
2. **Transforms** raw data into analytics-ready tables using dbt
3. **Analyses** congestion patterns across Stockholm's major transit stations
4. **Predicts** future congestion using a trained Random Forest model
5. **Visualises** everything on a live Streamlit dashboard

The system monitors **9 key Stockholm stations** including T-Centralen, Slussen, Södermalm, Gamla Stan, Medborgarplatsen, Gullmarsplan, Fridhemsplan, Odenplan, and Kungsträdgården.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                                │
│         SL Trafiklab API (Real-time departures)                 │
└────────────────────────┬────────────────────────────────────────┘
                         │ every 5 minutes
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                  INGESTION LAYER                                 │
│              DLT (Data Load Tool)                               │
│         Incremental loading → DuckDB                            │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│               TRANSFORMATION LAYER                              │
│                   dbt (dbt-duckdb)                              │
│  raw → stg_departures → fact_hourly_delays                      │
│                      → fact_station_performance                 │
│                      → fact_congestion_score                    │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                  ANALYTICS & ML LAYER                           │
│   Congestion Analytics   │   Random Forest Regressor            │
│   24h statistics         │   7-day congestion forecast          │
│   Station rankings       │   Trained daily at 2 AM             │
│   Delay distribution     │   44 engineered features             │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                 PRESENTATION LAYER                              │
│              Streamlit Dashboard                                │
│   Live Stream │ Analysis │ AI Predictions                       │
└─────────────────────────────────────────────────────────────────┘

ORCHESTRATION: Dagster (schedules + sensors + asset lineage)
STORAGE:       DuckDB on Azure File Share
DEPLOYMENT:    Azure Container Instances (2 containers)
```

---

## Features

### 🔴 Live Stream Tab
- **Average Delay** — with dynamic time window (last 60 min / 3 hours / all data)
- **Congestion Level** — network-wide severity indicator
- **Disruption Rate** — % of active lines with live deviations
- **Worst Station Now** — station with highest delay in last 30 min
- **Network Health Score** — composite 0–100 score (on-time rate + station coverage + deviation-free)
- **Delay Trend** — improving / stable / worsening vs 5 min ago
- **Peak Hour Heatmap** — congestion by station and hour of day
- **Live Departure Feed** — real-time table of recent departures

### 📋 Analysis Tab
- **Network KPI Cards** — on-time %, severe delay rate, 95th percentile delay
- **Delay Band Breakdown** — stacked 100% bar chart per station (6 severity bands)
- **Station Deep-Dive** — select any station for detailed delay distribution donut chart

### 🤖 AI Predictions Tab
- **7-Day Congestion Forecast** — predicted congestion per station per hour
- **ML Model Accuracy Card** — MAE, R², accuracy %, training timestamp
- **Feature Importance Chart** — top predictors ranked by importance
- **Prediction Confidence** — forecast uncertainty bands

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Ingestion | [DLT](https://dlthub.com) | Incremental API → DuckDB loading |
| Storage | [DuckDB](https://duckdb.org) | Embedded analytical database |
| Transformation | [dbt-duckdb](https://github.com/duckdb/dbt-duckdb) | SQL transformations + data models |
| Orchestration | [Dagster](https://dagster.io) | Pipeline scheduling, sensors, asset lineage |
| ML | [scikit-learn](https://scikit-learn.org) | Random Forest congestion predictor |
| Dashboard | [Streamlit](https://streamlit.io) | Interactive web dashboard |
| Visualisation | [Plotly](https://plotly.com) | Interactive charts |
| Containerisation | [Docker](https://docker.com) | Reproducible deployment |
| Cloud | [Azure Container Instances](https://azure.microsoft.com/en-us/products/container-instances) | Serverless container hosting |
| File Storage | [Azure File Share](https://azure.microsoft.com/en-us/products/storage/files) | Shared persistent DuckDB volume |

---

## Project Structure

```
Ex_Jobb_Data_pipeline_for_Stockholm_Traffic_-_Transit/
│
├── dashboard/
│   └── app.py                    # Streamlit dashboard (main UI)
│
├── dlt_pipeline/
│   └── data_ingestion.py         # DLT pipeline — SL API → DuckDB
│
├── trafiklab_exjobb/             # dbt project
│   ├── models/
│   │   ├── staging/
│   │   │   └── stg_departures.sql
│   │   └── marts/
│   │       ├── fact_hourly_delays.sql
│   │       ├── fact_station_performance.sql
│   │       └── fact_congestion_score.sql
│   ├── dbt_project.yml
│   └── profiles.yml              # dbt connection profiles (dev + docker)
│
├── ml_models/
│   ├── congestion_predictor.py   # Random Forest model training + forecast
│   └── saved_models/
│       ├── congestion_predictor.pkl
│       ├── model_metrics.json
│       └── feature_importance.csv
│
├── warehouse/
│   └── stockholm_traffic.duckdb  # DuckDB database file
│
├── config.py                     # Project configuration + station list
├── dagster_app1.py               # Dagster orchestration definitions
├── requirements.txt              # Python dependencies
├── Dockerfile.dashboard          # Dashboard container image
├── Dockerfile.dagster            # Dagster container image
├── docker-compose.yml            # Local development compose file
├── predictions_sample.csv        # ML forecast fallback (pre-generated)
└── .env                          # API keys (not committed to git)
```

---

## Getting Started

### Prerequisites
- Python 3.11+
- Docker Desktop
- Git

### Local Setup

**1 — Clone the repository**
```bash
git clone https://github.com/YOUR_USERNAME/Ex_Jobb_Data_pipeline_for_Stockholm_Traffic_-_Transit.git
cd Ex_Jobb_Data_pipeline_for_Stockholm_Traffic_-_Transit
```

**2 — Create virtual environment**
```bash
python -m venv .venv
source .venv/Scripts/activate   # Windows Git Bash

```

**3 — Install dependencies**
```bash
pip install -r requirements.txt
```

**4 — Set up environment variables**
```bash
cp .env
# Edit .env and add your API keys (see API Keys section)
```

**5 — Run the pipeline manually (first time)**
```bash
# Optional: enable ML training
export ENABLE_ML=1

# Start Dagster
dagster dev -f dagster_app1.py
```

**6 — Start the dashboard**
```bash
streamlit run dashboard/app.py
```

Open http://localhost:8501

---

### Docker Setup (Recommended)

```bash
# Build and start both services
docker compose up --build

# Dashboard → http://localhost:8501
# Dagster UI → http://localhost:3000
```

---

## Configuration

### `.env` file
```env
TRAFIKLAB_API_KEY=your_trafiklab_api_key
REALTIME_API_KEY=your_realtime_api_key
ENABLE_ML=1
```

### `config.py` — Monitored Stations
```python
STOCKHOLM_STATIONS = [
    {"name": "T-Centralen",       "site_id": "9001"},
    {"name": "Slussen",           "site_id": "9192"},
    {"name": "Gamla Stan",        "site_id": "9117"},
    {"name": "Södermalm",         "site_id": "9189"},
    {"name": "Medborgarplatsen",  "site_id": "9273"},
    {"name": "Gullmarsplan",      "site_id": "9120"},
    {"name": "Fridhemsplan",      "site_id": "9306"},
    {"name": "Odenplan",          "site_id": "9117"},
    {"name": "Kungsträdgården",   "site_id": "9225"},
]
```

### `trafiklab_exjobb/profiles.yml` — dbt Targets
```yaml
stockholm_traffic:
  target: dev
  outputs:
    dev:          # Local development (Windows path)
      type: duckdb
      path: "C:/path/to/warehouse/stockholm_traffic.duckdb"
      schema: analytics
      threads: 4
    docker:       # Docker container (Linux path)
      type: duckdb
      path: "/app/warehouse/stockholm_traffic.duckdb"
      schema: analytics
      threads: 4
```

---

## Pipeline Details

### Dagster Assets

```
raw_traffic_data          [ingestion]
      ↓
transformed_traffic_data  [transformation]  ← dbt build
      ↓
congestion_analytics      [analytics]       ← 24h statistics
      ↓
congestion_predictions    [ml]              ← 7-day forecast
ml_model_training         [ml]              ← retrain model
```

### Schedules
| Schedule | Cron | Action |
|---|---|---|
| `ingestion_schedule` | `*/5 * * * *` | Full pipeline every 5 minutes |
| `daily_model_training` | `0 2 * * *` | Retrain ML model at 2 AM |

### Sensors
| Sensor | Trigger | Action |
|---|---|---|
| `data_volume_sensor` | +50 new rows in `fact_congestion_score` | Trigger ML retraining |


---

## Machine Learning

### Model
- **Algorithm:** Random Forest Regressor (scikit-learn)
- **Target:** Congestion score (0–100)
- **Training data:** Historical departures from `fact_congestion_score`
- **Retraining:** Daily at 2 AM + triggered by data volume sensor

### Features (44 total)
- Time features: `hour_of_day`, `day_of_week`, `is_weekend`, `is_rush_hour`
- Station features: `station_id`, `line_count`, `avg_delay_station`
- Lag features: `congestion_lag1h`, `congestion_lag2h`, `congestion_lag6h`
- Rolling features: `rolling_mean_3h`, `rolling_std_3h`, `rolling_max_6h`
- Calendar: `month`, `week_of_year`, `is_holiday`

### Performance Metrics
Stored in `ml_models/saved_models/model_metrics.json`:
```json
{
  "test_mae": 2.3,
  "test_r2": 0.87,
  "accuracy_pct": 91.2,
  "n_features": 44,
  "trained_at": "2026-02-25T02:00:00"
}
```

### 95th Percentile (P95)
The P95 delay metric is SL's official performance contract metric:
> *"95% of passengers at this station experience a delay less than X minutes."*

This metric is displayed in the Analysis tab and catches severe outliers that average delay hides.

---

## Dashboard

### URL Structure
- `http://localhost:8501` — local
- `http://stockholm-dashboard.swedencentral.azurecontainer.io:8501` — Azure

### Tabs
| Tab | Content |
|---|---|
| 🔴 Live Stream | Real-time KPIs, heatmap, departure feed |
| 📋 Analysis | Delay distribution breakdown, station deep-dive |
| 🤖 AI Predictions | 7-day forecast, model accuracy, feature importance |

### Key Components
- **Hero Header** — LIVE badge + Stockholm local timestamp
- **KPI Cards** — Average Delay, Congestion Level, Disruption Rate, Worst Station
- **Network Health** — Composite score: 40% on-time + 35% station coverage + 25% deviation-free
- **Delay Trend** — Comparing current vs 5 minutes ago
- **Peak Hour Heatmap** — Station × Hour congestion matrix

---

## Docker Deployment

### Local
```bash
# Start
docker compose up --build

# Stop
docker compose down
```
---

## Azure Deployment

### Resources
| Resource | Name | Purpose |
|---|---|---|
| Resource Group | `stockholm-traffic-live-rg` | Container for all resources |
| Container Registry | `stockholmtraffilivecacr` | Docker image storage |
| Storage Account | `stockholmtraffilivecsa` | DuckDB persistent storage |
| File Share | `duckdb-share` | Shared volume between containers |
| Container Instance | `streamlit-dashboard` | Dashboard hosting |
| Container Instance | `dagster-pipeline` | Pipeline orchestration |

### Deploy from scratch
```bash
# 1 — Create resources
az group create --name stockholm-traffic-live-rg --location swedencentral
az acr create --resource-group stockholm-traffic-live-rg --name stockholmtraffilivecacr --sku Basic --admin-enabled true
az storage account create --name stockholmtraffilivecsa --resource-group stockholm-traffic-live-rg --location swedencentral --sku Standard_LRS
az storage share create --name duckdb-share --account-name stockholmtraffilivecsa --account-key YOUR_KEY

# 2 — Push images
az acr login --name stockholmtraffilivecacr
docker tag stockholm-dashboard:latest stockholmtraffilivecacr.azurecr.io/dashboard:latest
docker tag stockholm-dagster:latest stockholmtraffilivecacr.azurecr.io/dagster:latest
docker push stockholmtraffilivecacr.azurecr.io/dashboard:latest
docker push stockholmtraffilivecacr.azurecr.io/dagster:latest

# 3 — Deploy containers
az container create \
  --resource-group stockholm-traffic-live-rg \
  --name streamlit-dashboard \
  --image stockholmtraffilivecacr.azurecr.io/dashboard:latest \
  --os-type Linux --ip-address Public \
  --cpu 1 --memory 2 --ports 8501 \
  --dns-name-label stockholm-dashboard \
  --environment-variables DBT_TARGET=docker TZ=Europe/Stockholm \
  --azure-file-volume-account-name stockholmtraffilivecsa \
  --azure-file-volume-account-key YOUR_KEY \
  --azure-file-volume-share-name duckdb-share \
  --azure-file-volume-mount-path //app/warehouse \
  --restart-policy Always
```

### Cost Management (Azure for Students)
```bash
# Stop containers when not in use
az container stop --resource-group stockholm-traffic-live-rg --name streamlit-dashboard
az container stop --resource-group stockholm-traffic-live-rg --name dagster-pipeline

# Start before demo/presentation
az container start --resource-group stockholm-traffic-live-rg --name dagster-pipeline
az container start --resource-group stockholm-traffic-live-rg --name streamlit-dashboard
```

---

## API Keys

This project uses the **Trafiklab** API (Swedish public transit open data):

1. Register at https://www.trafiklab.se
2. Create a project and request access to:
   - **SL Transport** (departures + deviations)
   - **SL Real-time Information 4** (real-time data)
3. Add keys to `.env`:
```env
TRAFIKLAB_API_KEY=your_key_here
REALTIME_API_KEY=your_key_here
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `DuckDB locked` | Retry logic built-in — waits up to 35s automatically |
| `dbt build failed` | Check `profiles.yml` exists in `trafiklab_exjobb/` folder |
| `No predictions shown` | `predictions_sample.csv` is the fallback — check it exists |
| `Time 1 hour behind` | Set `TZ=Europe/Stockholm` environment variable |
| `Docker image name invalid` | Add `image:` with lowercase name in `docker-compose.yml` |
| `Azure mount path error` | Use `//app/warehouse` (double slash) in Git Bash |
| `Container times out` | Ensure `--ip-address Public` is set in `az container create` |

---

## License

This project was developed as a thesis (Examensarbete) at **STI — Sweden**.  
Data sourced from [Trafiklab](https://www.trafiklab.se) under their open data license.

---

*Built with ❤️ for Stockholm transit riders*