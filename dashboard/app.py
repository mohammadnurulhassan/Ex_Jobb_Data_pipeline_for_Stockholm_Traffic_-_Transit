"""
FILE: dashboard/app.py
Stockholm Traffic Analytics Dashboard — Streamlit Application

"""

from __future__ import annotations

import contextlib
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ── make project root importable ────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config import DUCKDB_DATABASE, DLT_DATASET_NAME, STOCKHOLM_STATIONS  # noqa: E402

# =============================================================================
# Constants
# =============================================================================
PREDICTIONS_CSV_FALLBACK = REPO_ROOT / "predictions_sample.csv"
MODEL_PATH               = REPO_ROOT / "ml_models" / "saved_models" / "congestion_predictor.pkl"
RAW_SCHEMA               = DLT_DATASET_NAME

# =============================================================================
# Page config
# =============================================================================
st.set_page_config(
    page_title="Stockholm Traffic Analytics",
    page_icon="🚇",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =============================================================================
# CSS theme
# =============================================================================
st.markdown(
    """
<style>
.stApp {
  background: radial-gradient(circle at 10%, #0b1220 0%, #071022 35%, #060a1a 70%, #050816 100%);
}
.block-container { padding-top: 1.2rem; padding-bottom: 2rem; }
#MainMenu { visibility: hidden; }
footer    { visibility: hidden; }
header    { visibility: hidden; }

/* ── hero ── */
.r5-hero {
  border-radius: 26px; padding: 26px;
  color: white;
  background: linear-gradient(90deg, #2563eb 0%, #4f46e5 40%, #7c3aed 100%);
  box-shadow: 0 18px 45px rgba(0,0,0,0.45);
  position: relative; overflow: hidden;
}
.r5-hero::after {
  content: ""; position: absolute; inset: 0;
  background: rgba(0,0,0,0.15); pointer-events: none;
}
.r5-hero-inner { position: relative; z-index: 2; }

/* ── badge ── */
.r5-badge {
  display: inline-flex; align-items: center; gap: 8px;
  background: rgba(255,255,255,0.20); border: 1px solid rgba(255,255,255,0.22);
  padding: 8px 12px; border-radius: 12px;
  font-weight: 800; font-size: 13px; margin-right: 10px;
}

/* ── section card ── */
.r5-white-card {
  background: rgba(255,255,255,0.98); border-radius: 14px; padding: 8px 13px;
  box-shadow: 0 14px 35px rgba(0,0,0,0.35);
}

/* ── KPI card — fixed height so all 4 align ── */
.r5-kpi-card {
  background: rgba(255,255,255,0.98); border-radius: 18px; padding: 16px;
  box-shadow: 0 14px 35px rgba(0,0,0,0.35);
  min-height: 128px; height: 128px;
  display: flex; flex-direction: column; justify-content: space-between;
}
.r5-leftbar   { border-left: 6px solid #2563eb; }
.r5-kpi-title { font-size: 11px; font-weight: 900; letter-spacing: .08em;
                text-transform: uppercase; color: #4b5563; }
.r5-kpi-value { font-size: 36px; font-weight: 950; line-height: 1; }
.r5-kpi-sub   { font-size: 12px; font-weight: 700; color: #6b7280; }

/* ── pill badge ── */
.r5-pill {
  display: inline-block; padding: 4px 10px; border-radius: 999px;
  font-weight: 900; font-size: 12px;
  background: rgba(239,68,68,0.15); color: #ef4444;
  border: 1px solid rgba(239,68,68,0.25); margin-left: 8px;
}

/* ── tabs ── */
div[data-testid="stTabs"] > div {
  background: transparent !important; border-radius: 14px !important;
  padding: 0 !important; border: none !important;
}
div[data-baseweb="tab-list"] {
  background: rgba(255,255,255,0.98) !important; border-radius: 14px !important;
  padding: 8px 10px !important; border: 1px solid rgba(0,0,0,0.08) !important;
}
div[data-baseweb="tab-list"] button[role="tab"] p {
  color: rgba(0,0,0,0.75) !important; font-weight: 800 !important;
}
div[data-baseweb="tab-list"] button[aria-selected="true"] p {
  color: rgba(0,0,0,0.95) !important;
}
div[data-baseweb="tab-border"] { display: none !important; }

/* ── AI hero ── */
.r5-ai-hero {
  border-radius: 20px; padding: 26px; color: white;
  background: linear-gradient(90deg, #7c3aed 0%, #4f46e5 60%, #2563eb 100%);
  box-shadow: 0 14px 35px rgba(0,0,0,0.35);
}

/* ── inline alert ── */
.r5-alert {
  border-radius: 14px; padding: 14px; font-weight: 800; font-size: 14px;
  border: 1px solid rgba(0,0,0,0.10);
  background: #fff7ed; color: #9a3412;
}
.r5-alert code {
  background: rgba(0,0,0,0.06); padding: 2px 6px;
  border-radius: 8px; font-weight: 900;
}

/* ── ML metric card ── */
.r5-ml-metric {
  background: rgba(255,255,255,0.98); border-radius: 14px; padding: 14px 16px;
  box-shadow: 0 8px 24px rgba(0,0,0,0.25);
  display: flex; flex-direction: column; gap: 4px;
  border-left: 5px solid #7c3aed;
}
.r5-ml-metric .r5-ml-label  { font-size: 11px; font-weight: 900; color: #6b7280;
                               letter-spacing:.08em; text-transform:uppercase; }
.r5-ml-metric .r5-ml-val    { font-size: 30px; font-weight: 950; color: #4f46e5; line-height:1; }
.r5-ml-metric .r5-ml-sub    { font-size: 11px; font-weight: 700; color: #9ca3af; }

/* ── model status pill ── */
.r5-model-status {
  display:inline-flex; align-items:center; gap:8px;
  border-radius:999px; padding:6px 14px;
  font-weight:900; font-size:13px;
}
.r5-model-ok   { background:#d1fae5; color:#065f46; border:1px solid #6ee7b7; }
.r5-model-warn { background:#fef3c7; color:#92400e; border:1px solid #fcd34d; }
.r5-model-none { background:#f3f4f6; color:#6b7280; border:1px solid #d1d5db; }

/* ── congestion level colour pills ── */
.r5-level-low      { background:#d1fae5; color:#065f46; }
.r5-level-moderate { background:#fef3c7; color:#92400e; }
.r5-level-high     { background:#fed7aa; color:#9a3412; }
.r5-level-critical { background:#fee2e2; color:#991b1b; }
</style>
""",
    unsafe_allow_html=True,
)

# =============================================================================
# Constants
# =============================================================================
PREDICTIONS_CSV_FALLBACK = REPO_ROOT / "predictions_sample.csv"
MODEL_PATH               = REPO_ROOT / "ml_models" / "saved_models" / "congestion_predictor.pkl"
RAW_SCHEMA               = DLT_DATASET_NAME




def _fmt_local(ts) -> str:
    """
    Format the last-query timestamp as HH:MM:SS.
    DuckDB's current_timestamp may return a tz-aware object; strip the tz so
    strftime shows the local wall-clock time with no offset applied.
    """
    if ts is None:
        return "—"
    try:
        dt = pd.to_datetime(ts).to_pydatetime()
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)   # already local — drop tz tag
        return dt.strftime("%H:%M:%S")
    except Exception:
        return "—"


# =============================================================================
# DuckDB helpers  — FIX 1: short-lived connections, always closed
# =============================================================================

@contextlib.contextmanager
def _db_conn(read_only: bool = True):
    """
    Context-manager: open a fresh DuckDB connection with retry logic.

    DuckDB only allows one writer at a time. When Dagster's pipeline is
    writing (DLT ingest / dbt build), the dashboard gets an IOException.
    Instead of immediately failing, we retry up to 5 times with short
    sleep intervals — the write window is usually <2 seconds.

    Usage:
        with _db_conn() as con:
            df = con.execute("SELECT ...").fetchdf()
    """
    MAX_RETRIES = 5
    DELAYS      = [0.3, 0.6, 1.0, 1.5, 2.0]   # seconds between retries

    con       = None
    last_exc  = None

    for attempt, wait in enumerate(DELAYS[:MAX_RETRIES]):
        try:
            con = duckdb.connect(DUCKDB_DATABASE, read_only=read_only)
            break                               # success — exit retry loop
        except duckdb.IOException as exc:
            last_exc = exc
            if attempt < MAX_RETRIES - 1:
                time.sleep(wait)                # wait then retry
            else:
                # All retries exhausted — show a soft warning, not a hard crash
                st.warning(
                    f"⚠️ DuckDB locked after {MAX_RETRIES} retries "
                    f"(pipeline is writing). Showing cached data. "
                    f"Will retry on next refresh."
                )
        except Exception as exc:
            last_exc = exc
            st.error(f"⚠️ DuckDB connection error: {exc}")
            break

    try:
        yield con       # con is None if all retries failed — callers handle None
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


def safe_df(con, query: str, params: list | None = None) -> pd.DataFrame:
    """Execute query safely; return empty DataFrame on any error."""
    if con is None:
        return pd.DataFrame()
    try:
        return con.execute(query, params or []).fetchdf()
    except Exception:
        return pd.DataFrame()


def safe_scalar(con, query: str, default=None):
    """Execute scalar query safely; return default on any error."""
    if con is None:
        return default
    try:
        row = con.execute(query).fetchone()
        return row[0] if row else default
    except Exception:
        return default


# =============================================================================
# Table resolution
# =============================================================================

def _table_exists(con, full_name: str) -> bool:
    if not full_name or "." not in full_name:
        return False
    schema, name = full_name.split(".", 1)
    try:
        return con.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ? LIMIT 1",
            [schema, name],
        ).fetchone() is not None
    except Exception:
        return False


def _first_existing(con, candidates: list[str]) -> str:
    """Return the first table name from the list that actually exists."""
    for t in candidates:
        if t and _table_exists(con, t):
            return t
    return ""


@st.cache_data(ttl=300)
def resolve_tables() -> dict[str, str]:
    """Resolve actual dbt table names — cached 5 min."""
    with _db_conn() as con:
        if con is None:
            return {}
        return {
            "STG_DEPARTURES": _first_existing(con, [
                "analytics_analytics_staging.stg_departures",
                "analytics_staging.stg_departures",
            ]),
            "FACT_HOURLY": _first_existing(con, [
                "analytics_analytics_marts.fact_hourly_delays",
                "analytics_marts.fact_hourly_delays",
            ]),
            "FACT_CONGESTION": _first_existing(con, [
                "analytics_analytics_marts.fact_congestion_score",
                "analytics_marts.fact_congestion_score",
            ]),
            "FACT_STATION": _first_existing(con, [
                "analytics_analytics_marts.fact_station_performance",
                "analytics_marts.fact_station_performance",
            ]),
            "PREDICTIONS": _first_existing(con, [
                os.getenv("PREDICTIONS_TABLE", "").strip(),
                "main_analytics_marts.congestion_predictions",
                "analytics_analytics_marts.congestion_predictions",
                "analytics_marts.congestion_predictions",
            ]),
        }


# =============================================================================
# Data fetchers
# =============================================================================

def _build_stats_query(stg: str, where_clause: str) -> str:
    """
    Live-statistics SQL.

    FIX 8 (CORRECTED): expected_datetime is stored as Stockholm local time (naive).
    Use it directly — NO AT TIME ZONE cast.  The previous version cast it as UTC
    and compared against UTC current_timestamp, which was wrong because the stored
    values are local, not UTC.

    FIX 3: COUNT(DISTINCT line WHERE has_deviation) — not SUM(has_deviation).
    """
    return f"""
    WITH base AS (
        SELECT
            expected_datetime,                                                    -- already Stockholm local
            station_id,
            line_designation,
            delay_minutes,
            is_delayed,
            has_deviation
        FROM {stg}
        {where_clause}
    )
    SELECT
        COUNT(*)                                                                  AS total_departures,
        COUNT(DISTINCT station_id)                                                AS active_stations,
        COUNT(DISTINCT line_designation)                                          AS active_lines,
        AVG(delay_minutes)                                                        AS avg_delay,
        MAX(delay_minutes)                                                        AS max_delay,
        SUM(CASE WHEN delay_minutes > 5 THEN 1 ELSE 0 END)                       AS significant_delays,
        COUNT(DISTINCT CASE WHEN has_deviation THEN line_designation END)         AS disrupted_lines,
        -- FIX 8 (REAL FIX): expected_datetime is a future scheduled departure time,
        -- so MAX(expected_datetime) always shows ~1 h ahead of now.
        -- Use current_timestamp instead — it records when this query actually ran.
        current_timestamp                                                         AS last_update,
        SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END)                              AS delayed_count
    FROM base
    """


@st.cache_data(ttl=20)
def get_live_statistics() -> dict:
    """
    Top-level KPI stats with cascading time-window fallback.
    Returns __locked__ key (not __error__) when DB is temporarily busy
    so the dashboard shows the last cached result instead of stopping.
    """
    tbls = resolve_tables()
    stg  = tbls.get("STG_DEPARTURES", "")
    if not stg:
        return {"__error__": (
            "stg_departures table not found. "
            "Run: dbt build --project-dir trafiklab_exjobb"
        )}

    # FIX 8 (CORRECTED): expected_datetime is local Stockholm time — compare directly
    # against current_timestamp (also local on the machine running this).
    # No AT TIME ZONE cast needed or correct here.
    windows = [
        "WHERE expected_datetime >= current_timestamp - INTERVAL '60 minutes'",
        "WHERE expected_datetime >= current_timestamp - INTERVAL '180 minutes'",
        "",   # no filter — full table as last resort
    ]

    row = None
    for where in windows:
        with _db_conn() as con:
            if con is None:
                break
            try:
                candidate = con.execute(_build_stats_query(stg, where)).fetchone()
                if candidate and int(candidate[0] or 0) > 0:
                    row = candidate
                    break
            except Exception:
                continue

    if not row:
        return {"__error__": "Live stats query returned no rows — check ingestion pipeline."}

    total           = int(row[0] or 0)
    sig             = int(row[5] or 0)
    sig_pct         = (sig / max(total, 1)) * 100

    disrupted_lines = int(row[6] or 0)
    active_lines    = int(row[2] or 1)
    # FIX 2: rate-based — both terms 0–100 so result is always 0–100
    disruption_rate = (disrupted_lines / max(active_lines, 1)) * 100
    congestion      = min(100.0, max(0.0, sig_pct * 0.7 + disruption_rate * 0.3))
    on_time         = max(0.0, min(100.0, 100.0 - (float(row[8] or 0) / max(total, 1)) * 100))

    # ── Peak Load Index ────────────────────────────────────────────────────
    # A single 0–100 score showing how hard the network is being pushed NOW.
    # Formula:  delay_score   = min(avg_delay / 10 * 100, 100)  → 10 min delay = 100%
    #           sig_score     = sig_pct                          → % trips delayed >5 min
    #           disruption    = disruption_rate                  → % lines disrupted
    # Weighted blend: 40% delay severity + 35% significant delays + 25% disruption
    avg_delay_val  = float(row[3] or 0)
    delay_score    = min(avg_delay_val / 10.0 * 100.0, 100.0)
    peak_load      = round(
        min(100.0, max(0.0,
            delay_score    * 0.40 +
            sig_pct        * 0.35 +
            disruption_rate * 0.25
        )), 1
    )
    peak_label = (
        "Low"      if peak_load < 25 else
        "Moderate" if peak_load < 50 else
        "High"     if peak_load < 75 else
        "Critical"
    )
    peak_color = (
        "#16a34a" if peak_load < 25 else
        "#eab308" if peak_load < 50 else
        "#ea580c" if peak_load < 75 else
        "#dc2626"
    )

    return {
        "total_departures":  total,
        "active_stations":   int(row[1] or 0),
        "active_lines":      active_lines,
        "avg_delay":         float(row[3] or 0),
        "max_delay":         float(row[4] or 0),
        "significant_delays": sig,
        "disrupted_lines":   disrupted_lines,
        "disruption_rate":   disruption_rate,
        "last_update":       row[7],
        "congestion_level":  congestion,
        "on_time_rate":      on_time,
        "peak_load_index":   peak_load,
        "peak_load_label":   peak_label,
        "peak_load_color":   peak_color,
    }


@st.cache_data(ttl=10)
def get_live_stream_minutes(window_minutes: int = 30) -> pd.DataFrame:
    """
    Per-minute chart aggregation.
    FIX 8: UTC-safe WHERE + UTC-normalised ts_min.
    FIX 2: rate-based congestion in SQL.
    FIX 3: distinct disrupted lines per minute.
    """
    tbls = resolve_tables()
    stg  = tbls.get("STG_DEPARTURES", "")
    if not stg:
        return pd.DataFrame()

    q = f"""
    WITH base AS (
        SELECT
            -- FIX 8 (CORRECTED): expected_datetime is local — truncate directly
            date_trunc('minute', expected_datetime)                               AS ts_min,
            line_designation,
            delay_minutes,
            is_delayed,
            has_deviation
        FROM {stg}
        -- FIX 8 (CORRECTED): plain local-time comparison — no AT TIME ZONE needed
        WHERE expected_datetime >= current_timestamp - INTERVAL '{int(window_minutes)} minutes'
    ),
    agg AS (
        SELECT
            ts_min,
            COUNT(*)                                                               AS departures,
            AVG(delay_minutes)                                                     AS avg_delay,
            SUM(CASE WHEN delay_minutes > 5 THEN 1 ELSE 0 END)                    AS sig_delays,
            -- FIX 3: distinct lines with deviation
            COUNT(DISTINCT CASE WHEN has_deviation THEN line_designation END)      AS disrupted_lines,
            COUNT(DISTINCT line_designation)                                       AS total_lines,
            SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END)                           AS delayed
        FROM base
        GROUP BY 1
    )
    SELECT
        ts_min,
        departures,
        avg_delay,
        -- FIX 2: both components 0–100 → result always 0–100
        LEAST(100.0, GREATEST(0.0,
            (100.0 * sig_delays      / GREATEST(departures,  1)) * 0.7 +
            (100.0 * disrupted_lines / GREATEST(total_lines, 1)) * 0.3
        ))                                                                         AS congestion_pct,
        (100.0 - (100.0 * delayed / GREATEST(departures, 1)))                     AS on_time_rate
    FROM agg
    ORDER BY ts_min
    """
    with _db_conn() as con:
        return safe_df(con, q)


@st.cache_data(ttl=20)
def get_live_business_insights() -> dict:
    """
    4 genuinely NEW business insights for the Live tab bottom row.
    None of these are visible in the chart above — each answers a distinct
    operational question a transport manager would actually act on.

      1. worst_station      — which station has the highest avg delay right now
      2. total_delay_mins   — cumulative passenger-minutes lost since midnight (G)
                              + worst geographic hotspot zone (J)
      3. network_health     — composite 0-100 score: on-time rate + station
                              coverage + deviation-free lines (Option E)
      4. delay_trend        — direction of avg delay: improving / worsening /
                              stable, comparing last 5 min vs previous 5 min
    """
    tbls = resolve_tables()
    stg  = tbls.get("STG_DEPARTURES", "")
    if not stg:
        return {}

    result = {}
    with _db_conn() as con:
        if con is None:
            return {}

        # ── 1. Worst station right now (last 30 min) ──────────────────────
        worst = safe_df(con, f"""
            SELECT station_id,
                   AVG(delay_minutes) AS avg_delay,
                   COUNT(*)           AS trips
            FROM {stg}
            WHERE expected_datetime >= current_timestamp - INTERVAL '30 minutes'
            GROUP BY station_id
            HAVING COUNT(*) >= 3
            ORDER BY avg_delay DESC
            LIMIT 1
        """)
        if not worst.empty:
            sid = int(worst.iloc[0]["station_id"])
            from config import STOCKHOLM_STATIONS
            result["worst_station_name"]  = STOCKHOLM_STATIONS.get(sid, f"Station {sid}")
            result["worst_station_delay"] = round(float(worst.iloc[0]["avg_delay"]), 1)
        else:
            result["worst_station_name"]  = "—"
            result["worst_station_delay"] = 0.0

       
        delay_gj = safe_df(con, f"""
            WITH today_delays AS (
                SELECT
                    SUM(GREATEST(delay_minutes, 0))          AS total_delay_mins
                FROM {stg}
                WHERE expected_datetime >= date_trunc('day', current_timestamp)
                  AND delay_minutes > 0
            ),
            zone_delays AS (
                SELECT
                    CASE station_id
                        WHEN 9001 THEN 'T-Centralen'
                        WHEN 9192 THEN 'Slussen'
                        WHEN 9191 THEN 'Medborgarplatsen'
                        WHEN 9190 THEN 'Gamla Stan'
                        WHEN 9204 THEN 'Odenplan'
                        WHEN 9302 THEN 'Fridhemsplan'
                        WHEN 9303 THEN 'Kungsträdgården'
                        WHEN 1080 THEN 'Gullmarsplan'
                        WHEN 1051 THEN 'Hötorget'
                        WHEN 9506 THEN 'Södermalm'
                        ELSE 'Other'
                    END                                       AS zone,
                    AVG(delay_minutes)                        AS zone_avg_delay
                FROM {stg}
                WHERE expected_datetime >= current_timestamp - INTERVAL '30 minutes'
                GROUP BY 1
                HAVING COUNT(*) >= 3
                ORDER BY zone_avg_delay DESC
                LIMIT 1
            )
            SELECT
                t.total_delay_mins,
                z.zone           AS hotspot_zone,
                z.zone_avg_delay AS hotspot_delay
            FROM today_delays t, zone_delays z
        """)
        if not delay_gj.empty:
            row_gj = delay_gj.iloc[0]
            result["total_delay_mins"] = int(float(row_gj["total_delay_mins"] or 0))
            result["hotspot_zone"]     = str(row_gj["hotspot_zone"] or "—")
            result["hotspot_delay"]    = round(float(row_gj["hotspot_delay"] or 0), 1)
        else:
            result["total_delay_mins"] = 0
            result["hotspot_zone"]     = "—"
            result["hotspot_delay"]    = 0.0

        # ── 3. Network Health Score (Option E) ───────────────────────────
        # Composite 0–100 score combining:
        #   • on-time rate      (40%) — are departures running on schedule?
        #   • station coverage  (35%) — how many stations actively reporting?
        #   • deviation-free    (25%) — what % of lines have NO disruption?
        #
        # NOTE: is_delayed and has_deviation are stored as INTEGER (0/1) in
        # DuckDB, NOT as native booleans — use = 0 / = 1 not = FALSE / = TRUE.
        health = safe_df(con, f"""
            WITH window AS (
                SELECT *
                FROM {stg}
                WHERE expected_datetime >= current_timestamp - INTERVAL '30 minutes'
            ),
            metrics AS (
                SELECT
                    -- on-time rate 0-100
                    100.0 * SUM(CASE WHEN is_delayed = 0 THEN 1 ELSE 0 END)
                        / GREATEST(COUNT(*), 1)                              AS on_time_rate,
                    -- station coverage: distinct stations reporting vs 10 expected
                    100.0 * COUNT(DISTINCT station_id) / 10.0                AS station_coverage,
                    -- deviation-free: lines where NO trip has a deviation
                    100.0 * COUNT(DISTINCT CASE WHEN has_deviation = 0
                                  THEN line_designation END)
                        / GREATEST(COUNT(DISTINCT line_designation), 1)      AS deviation_free_rate,
                    COUNT(DISTINCT station_id)                               AS reporting_stations,
                    COUNT(DISTINCT line_designation)                         AS total_lines,
                    COUNT(*)                                                  AS total_rows
                FROM window
            )
            SELECT
                on_time_rate,
                station_coverage,
                deviation_free_rate,
                reporting_stations,
                total_lines,
                total_rows,
                ROUND(
                    LEAST(100.0, GREATEST(0.0,
                        on_time_rate        * 0.40 +
                        station_coverage    * 0.35 +
                        deviation_free_rate * 0.25
                    )), 1
                ) AS health_score
            FROM metrics
        """)

        if not health.empty and int(health.iloc[0].get("total_rows", 0) or 0) > 0:
            h = health.iloc[0]
            result["network_health_score"] = round(float(h["health_score"]       or 0), 1)
            result["network_reporting"]    = int(  h["reporting_stations"]        or 0)
            result["network_total_lines"]  = int(  h["total_lines"]               or 0)
            result["network_on_time"]      = round(float(h["on_time_rate"]        or 0), 1)
        else:
            # Fallback: reuse on_time_rate from get_live_statistics which
            # already works (same table, same window) — avoids showing 0%.
            from_stats = get_live_statistics()
            ot  = float(from_stats.get("on_time_rate", 0))
            dis = float(from_stats.get("disruption_rate", 0))
            # simplified health: 70% on-time + 30% disruption-free
            fallback_health = round(ot * 0.70 + max(0, 100 - dis) * 0.30, 1)
            result["network_health_score"] = fallback_health
            result["network_reporting"]    = int(from_stats.get("active_stations", 0))
            result["network_total_lines"]  = int(from_stats.get("active_lines", 0))
            result["network_on_time"]      = round(ot, 1)


        # ── 4. Delay trend: last 5 min vs previous 5 min ─────────────────
        trend = safe_df(con, f"""
            SELECT
                AVG(CASE WHEN expected_datetime >= current_timestamp - INTERVAL '5 minutes'
                         THEN delay_minutes END)  AS recent_delay,
                AVG(CASE WHEN expected_datetime <  current_timestamp - INTERVAL '5 minutes'
                          AND expected_datetime >= current_timestamp - INTERVAL '10 minutes'
                         THEN delay_minutes END)  AS prior_delay
            FROM {stg}
            WHERE expected_datetime >= current_timestamp - INTERVAL '10 minutes'
        """)
        if not trend.empty:
            recent = float(trend.iloc[0]["recent_delay"] or 0)
            prior  = float(trend.iloc[0]["prior_delay"]  or 0)
            delta  = round(recent - prior, 2)
            if delta > 0.2:
                result["trend_label"] = f"▲ +{delta:.1f} min  Worsening"
                result["trend_color"] = "#dc2626"
            elif delta < -0.2:
                result["trend_label"] = f"▼ {delta:.1f} min  Improving"
                result["trend_color"] = "#16a34a"
            else:
                result["trend_label"] = "● Stable"
                result["trend_color"] = "#6b7280"
            result["trend_delta"] = delta
        else:
            result["trend_label"] = "— No data"
            result["trend_color"] = "#6b7280"
            result["trend_delta"] = 0.0

    return result


@st.cache_data(ttl=120)
def get_station_performance(limit: int = 10) -> pd.DataFrame:
    tbls = resolve_tables()
    fact = tbls.get("FACT_STATION", "")
    if not fact:
        return pd.DataFrame()
    q = f"""
    SELECT station_name, total_departures, avg_delay_minutes, on_time_rate
    FROM   {fact}
    ORDER  BY total_departures DESC NULLS LAST
    LIMIT  {int(limit)}
    """
    with _db_conn() as con:
        return safe_df(con, q)


@st.cache_data(ttl=300)
def get_predictions() -> pd.DataFrame:
    tbls = resolve_tables()
    pred_table = tbls.get("PREDICTIONS", "")
    if pred_table:
        with _db_conn() as con:
            df = safe_df(con, f"SELECT * FROM {pred_table}")
            if not df.empty:
                return df
    if PREDICTIONS_CSV_FALLBACK.exists():
        try:
            return pd.read_csv(PREDICTIONS_CSV_FALLBACK)
        except Exception:
            pass
    return pd.DataFrame()


# ── [ML] New fetchers ──────────────────────────────────────────────────────────

@st.cache_data(ttl=120)
def get_ml_metrics() -> dict:
    """
    [ML] Read model_metrics.json written by congestion_predictor.save_model().
    No sklearn / joblib dependency — pure JSON read.
    Returns empty dict when the model has never been trained.
    """
    json_path = MODEL_PATH.parent / "model_metrics.json"
    if not json_path.exists():
        return {}
    try:
        import json as _json
        with open(json_path) as f:
            return _json.load(f)
    except Exception:
        return {}


@st.cache_data(ttl=300)
def get_feature_importance() -> pd.DataFrame:
    """[ML] Read feature_importance.csv saved by the training pipeline."""
    csv_path = MODEL_PATH.parent / "feature_importance.csv"
    if not csv_path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(csv_path)
        return df.head(20)   # top-20 is enough for the chart
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=120)
def get_predictions_detail(station_filter: str = "All Stations") -> pd.DataFrame:
    """
    [ML] Load full predictions from DuckDB (or CSV fallback).
    Applies optional station filter and adds helper columns for charting.
    """
    df = get_predictions()
    if df.empty:
        return df

    # Normalise column names
    df.columns = [c.lower() for c in df.columns]

    # Parse timestamp
    for col in ("timestamp", "date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    if station_filter != "All Stations" and "station_name" in df.columns:
        df = df[df["station_name"] == station_filter]

    # Add day_name if missing
    if "day_name" not in df.columns and "day_of_week" in df.columns:
        day_map = {0:"Mon", 1:"Tue", 2:"Wed", 3:"Thu", 4:"Fri", 5:"Sat", 6:"Sun"}
        df["day_name"] = df["day_of_week"].map(day_map)

    return df.sort_values("timestamp") if "timestamp" in df.columns else df


# =============================================================================
# UI helpers
# =============================================================================

def human_age(ts) -> str:
    """
    FIX 8 (CORRECTED): expected_datetime is Stockholm local time (naive).
    Compare the stored value directly against datetime.now() — both are naive
    local time so the difference is correct without any tz conversion.

    Previous attempt used TZ_STOCKHOLM-aware comparison which over-corrected
    and gave a duration off by the UTC offset in the opposite direction.
    """
    if ts is None:
        return "unknown"
    try:
        dt = pd.to_datetime(ts).to_pydatetime()
        # Strip tz-info if DuckDB returned a tz-aware object
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        diff = datetime.now() - dt         # both naive local → correct duration
        sec  = int(diff.total_seconds())
        if sec < 0:    return "just now"
        if sec < 60:   return f"{sec}s ago"
        if sec < 3600: return f"{sec // 60}m ago"
        return f"{sec // 3600}h ago"
    except Exception:
        return "unknown"


def r5_alert(html_msg: str) -> None:
    st.markdown(f'<div class="r5-alert">{html_msg}</div>', unsafe_allow_html=True)


def hero_header(last_update, refresh_seconds: int) -> None:
    pulse     = "🟢 LIVE" if last_update else "⚪ NO DATA"
    local_str = _fmt_local(last_update)

    st.markdown(f"""
<div class="r5-hero">
  <div class="r5-hero-inner">
    <div style="display:flex;align-items:flex-start;justify-content:space-between;
                gap:18px;flex-wrap:wrap;">
      <div>
        <div style="display:flex;align-items:center;gap:16px;">
          <div style="font-size:52px;">📡</div>
          <div>
            <div style="font-size:44px;font-weight:950;line-height:1.1;">
              Stockholm Traffic Analytics
            </div>
            <div style="font-size:17px;font-weight:800;
                        color:rgba(255,255,255,0.85);margin-top:6px;">
              AI-Powered Real-Time Intelligence Platform
            </div>
          </div>
        </div>
        <div style="margin-top:14px;">
          <span class="r5-badge">{pulse}</span>
        </div>
      </div>
      <div style="min-width:200px;">
        <div class="r5-badge" style="justify-content:space-between;">
          <span>🕒 Last Update</span>
          <span>{local_str}</span>
        </div>
      </div>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)


def kpi_card(
    title: str, value: str, subtitle: str, color: str,
    badge: str | None = None,
) -> None:
    badge_html = f'<span class="r5-pill">{badge}</span>' if badge else ""
    st.markdown(f"""
<div class="r5-kpi-card r5-leftbar" style="border-left-color:{color};">
  <div class="r5-kpi-title">{title} {badge_html}</div>
  <div class="r5-kpi-value" style="color:{color};">{value}</div>
  <div class="r5-kpi-sub">{subtitle}</div>
</div>
""", unsafe_allow_html=True)


def progress_card(title: str, value_pct: float) -> None:
    v = float(max(0.0, min(100.0, value_pct)))
    st.markdown(f"""
<div class="r5-white-card">
  <div style="font-size:20px;font-weight:950;margin-bottom:10px;">{title}</div>
  <div style="font-size:44px;font-weight:950;color:#16a34a;margin-bottom:14px;">
    {v:.1f}%
  </div>
  <div style="width:100%;height:14px;background:#e5e7eb;
              border-radius:999px;overflow:hidden;">
    <div style="width:{v:.2f}%;height:100%;background:#16a34a;"></div>
  </div>
</div>
""", unsafe_allow_html=True)


def ml_accuracy_card(metrics: dict) -> None:
    """
    [ML] Homepage ML card — shows real metrics from model_metrics.json.
    Falls back to a 'Model not trained yet' state when metrics is empty.
    """
    if not metrics:
        st.markdown("""
<div class="r5-white-card">
  <div style="font-size:18px;font-weight:950;margin-bottom:8px;">ML Prediction Accuracy</div>
  <div style="font-size:14px;font-weight:700;color:#9ca3af;">
    ⚠️ No model trained yet — run <code style="background:#f3f4f6;padding:2px 6px;
    border-radius:6px;font-weight:900;">ENABLE_ML=1</code> and trigger the Dagster schedule.
  </div>
</div>
""", unsafe_allow_html=True)
        return

    acc     = float(metrics.get("accuracy_pct", 0.0))
    mae     = metrics.get("test_mae",  "—")
    r2      = metrics.get("test_r2",   "—")
    trained = metrics.get("trained_at", "")
    trained_fmt = trained[:16].replace("T", " ") if trained else "unknown"
    n_feat  = metrics.get("n_features", "—")

    # colour based on accuracy
    col = "#16a34a" if acc >= 80 else ("#eab308" if acc >= 60 else "#dc2626")
    bar = min(acc, 100)

    st.markdown(f"""
<div class="r5-white-card">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:10px;">
    <div>
      <div style="font-size:18px;font-weight:950;margin-bottom:4px;">ML Prediction Accuracy</div>
      <div style="font-size:11px;font-weight:700;color:#9ca3af;">
        Trained {trained_fmt} · {n_feat} features
      </div>
    </div>
    <div style="font-size:11px;font-weight:800;color:#6b7280;text-align:right;">
      MAE&nbsp;<strong style="color:#4f46e5">{mae}</strong>&nbsp;&nbsp;
      R²&nbsp;<strong style="color:#4f46e5">{r2}</strong>
    </div>
  </div>
  <div style="font-size:42px;font-weight:950;color:{col};margin:8px 0 6px;">{acc:.1f}%</div>
  <div style="width:100%;height:10px;background:#e5e7eb;border-radius:999px;overflow:hidden;">
    <div style="width:{bar:.1f}%;height:100%;background:{col};
                transition:width 0.4s ease;"></div>
  </div>
  <div style="font-size:11px;font-weight:700;color:#9ca3af;margin-top:6px;">
    Random Forest · 7-day Stockholm congestion forecast
  </div>
</div>
""", unsafe_allow_html=True)


# =============================================================================
# Main
# =============================================================================

def main() -> None:

    # ── Sidebar ────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("## ⚙️ Controls")
        auto_refresh    = st.checkbox("🔄 Auto-refresh", value=True)
        refresh_seconds = st.selectbox("Interval (s)", [5, 10, 15, 30, 60], index=2)

        if st.button("🔃 Refresh now"):
            st.cache_data.clear()
            st.rerun()

        st.divider()
        st.markdown("### 🗄️ DuckDB path")
        st.code(DUCKDB_DATABASE, language="text")

        st.divider()
        st.markdown("### ✅ Resolved tables")
        tbls = resolve_tables()
        for k, v in tbls.items():
            icon = "✅" if v else "❌"
            st.markdown(f"- {icon} **{k}**: `{v or 'NOT FOUND'}`")

        st.divider()
        st.markdown("### 🌍 Timezone")
        st.info("All times shown in **Europe/Stockholm**\n(UTC+1 winter / UTC+2 summer)")

        st.divider()
        st.markdown("### 🚉 Monitored stations (first 10)")
        for k, v in list(STOCKHOLM_STATIONS.items())[:10]:
            st.markdown(f"- **{k}**: {v}")

    # ── Auto-refresh ───────────────────────────────────────────────────────
    if auto_refresh:
        last_ts = st.session_state.get("last_refresh_ts")
        now     = time.time()
        if last_ts is None or (now - last_ts) >= int(refresh_seconds):
            st.session_state["last_refresh_ts"] = now
            st.cache_data.clear()
            st.rerun()

    # ── Load stats ─────────────────────────────────────────────────────────
    stats = get_live_statistics()
    if "__error__" in stats:
        st.error("⚠️ Could not load live KPI stats — see detail below.")
        st.code(stats["__error__"])
        st.stop()
    if "__locked__" in stats:
        st.warning("⏳ DuckDB is being written to by Dagster — showing last cached data. Refreshing shortly...")
        # Don't stop — fall through with whatever cached stats we have
        stats = {k: v for k, v in stats.items() if k != "__locked__"}
        if not stats:
            st.stop()

    # ── Hero ───────────────────────────────────────────────────────────────
    hero_header(
        last_update=stats.get("last_update"),
        refresh_seconds=int(refresh_seconds),
    )
    st.write("")

    # ── 4 top KPI cards ────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)

    with c1:
        window_lbl = stats.get("window_used", "last 60 min")
        kpi_card(
            "AVERAGE DELAY",
            f"{stats['avg_delay']:.1f} min",
            f"Avg across all stations: {window_lbl}",
            "#e02a2a",
            badge="HIGH" if stats["avg_delay"] > 5 else None,
        )

    with c2:
        cong = stats.get("congestion_level", 0)
        kpi_card(
            "CONGESTION LEVEL",
            f"{cong:.0f}%",
            # FIX 2: meaningful thresholds now that formula is rate-based
            "Low"      if cong < 25 else
            "Moderate" if cong < 50 else
            "High"     if cong < 75 else "Severe",
            "#ea580c",
            badge="PEAK" if cong > 75 else None,
        )

    with c3:
        # Bottleneck Line — which single line is causing most disruption right now
        # Pulled from live stats significant_delays as proxy; full detail in insights
        dl    = stats.get("disrupted_lines", 0)
        total = stats.get("total_departures", 1)
        dis_r = stats.get("disruption_rate", 0)
        bl_color = "#dc2626" if dis_r > 50 else "#eab308" if dis_r > 20 else "#16a34a"
        kpi_card(
            "🚌 DISRUPTION RATE",
            f"{dis_r:.0f}%",
            f"{dl} disrupted line{'s' if dl != 1 else ''} of {stats.get('active_lines',0)} active",
            bl_color,
            badge="CONTACT OP" if dis_r > 50 else None,
        )

    with c4:
        insights_top = get_live_business_insights()
        ws_name  = insights_top.get("worst_station_name", "—")
        ws_delay = float(insights_top.get("worst_station_delay", 0))
        ws_color = "#dc2626" if ws_delay > 5 else "#eab308" if ws_delay > 2 else "#16a34a"
        kpi_card(
            "🚨 WORST STATION NOW",
            ws_name,
            f"Avg delay {ws_delay:.1f} min · last 30 min",
            ws_color,
            badge="ACTION" if ws_delay > 5 else None,
        )

    st.write("")
    progress_card("On-Time Performance", float(stats.get("on_time_rate", 0)))

    st.write("")
    ml_metrics = get_ml_metrics()
    ml_accuracy_card(ml_metrics)

    st.write("")

    # ── Tabs ───────────────────────────────────────────────────────────────
    live_tab, ai_tab, analysis_tab = st.tabs(
        ["🔴 Live Stream", "🤖 AI Predictions", "📋 Analysis"]
    )

    # ════════════════════════════════════════
    # TAB 1 — LIVE STREAM
    # ════════════════════════════════════════
    with live_tab:
        # FIX 7: opening tag always matched by closing </div> at bottom of tab
        st.markdown("""
<div class="r5-white-card">
  <div style="display:flex;justify-content:space-between;
              align-items:center;gap:12px;">
    <div style="font-size:26px;font-weight:950;">🔴 Live Stream</div>
    <div style="background:rgba(239,68,68,0.12);
         border:1px solid rgba(239,68,68,0.25);
         color:#ef4444;font-weight:950;border-radius:999px;padding:8px 14px;">
      ● LIVE
    </div>
  </div>
""", unsafe_allow_html=True)

        df_live = get_live_stream_minutes(window_minutes=30)

        if df_live.empty:
            r5_alert(
                "<strong>No live data in the last 30 minutes.</strong> "
                "Ensure <code>stg_departures</code> exists and the ingestion "
                "pipeline is running."
            )
        else:
            
            df_plot = df_live.copy()
            try:
                df_plot["ts_local"] = (
                    pd.to_datetime(df_plot["ts_min"], utc=True)
                    .dt.tz_convert("Europe/Stockholm")
                )
            except Exception:
                df_plot["ts_local"] = df_plot["ts_min"]  

            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_plot["ts_local"], y=df_plot["avg_delay"],
                mode="lines", name="Avg Delay (min)",
                line=dict(width=3, color="#EB4C4C"),
            ))
            fig.add_trace(go.Scatter(
                x=df_plot["ts_local"], y=df_plot["congestion_pct"],
                mode="lines", name="Congestion %",
                line=dict(width=3, color="#f55a07"), yaxis="y2",
            ))
            fig.add_trace(go.Scatter(
                x=df_plot["ts_local"], y=df_plot["departures"],
                mode="lines", name="Departures / min",
                line=dict(width=3, color="#16a34a"), yaxis="y2",
            ))
            fig.update_layout(
                template="plotly_white", height=520,
                margin=dict(l=10, r=10, t=20, b=10),
                hovermode="x unified",
                legend=dict(
                    orientation="h", yanchor="bottom",
                    y=-0.18, xanchor="center", x=0.5,
                ),
                xaxis=dict(title="Stockholm local time", tickformat="%H:%M"),
                yaxis=dict(title="Delay (min)"),
                yaxis2=dict(
                    title="Congestion % / Departures",
                    overlaying="y", side="right", showgrid=False,
                ),
            )
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

            # ── 2 Business Insight cards ──────────────────────────────────
            insights = get_live_business_insights()

            k1, k2 = st.columns(2)

            with k1:
                # Network Health Score — composite 0-100
                nh       = float(insights.get("network_health_score", 0))
                n_rep    = int(insights.get("network_reporting", 0))
                n_ot     = float(insights.get("network_on_time", 0))
                nh_color = "#16a34a" if nh >= 80 else "#eab308" if nh >= 60 else "#dc2626"
                nh_label = "Healthy" if nh >= 80 else "Degraded" if nh >= 60 else "Critical"
                kpi_card(
                    "🏥 NETWORK HEALTH",
                    f"{nh:.0f}%",
                    f"{nh_label} · {n_rep}/10 stations · {n_ot:.0f}% on-time",
                    nh_color,
                    badge="CRITICAL" if nh < 60 else None,
                )

            with k2:
                # Delay Trend — improving / stable / worsening vs 5 min ago
                kpi_card(
                    "📈 DELAY TREND",
                    insights.get("trend_label", "—").split("  ")[0],
                    insights.get("trend_label", "—").split("  ")[-1],
                    insights.get("trend_color", "#6b7280"),
                )

        st.markdown("</div>", unsafe_allow_html=True)

    # ════════════════════════════════════════
    # TAB 2 — AI PREDICTIONS
    # ════════════════════════════════════════
    with ai_tab:

        ml_metrics = get_ml_metrics()
        fi_df      = get_feature_importance()

        # ── Model status banner ────────────────────────────────────────────
        model_trained = bool(ml_metrics)
        acc_pct = float(ml_metrics.get("accuracy_pct", 0.0))
        trained_at = ml_metrics.get("trained_at", "")
        trained_fmt = trained_at[:16].replace("T", " ") if trained_at else "—"

        status_cls  = "r5-model-ok"   if model_trained and acc_pct >= 70 else \
                      "r5-model-warn" if model_trained else "r5-model-none"
        status_icon = "✅" if model_trained and acc_pct >= 70 else \
                      "⚠️" if model_trained else "❌"
        status_txt  = f"Model ready · trained {trained_fmt}" if model_trained else \
                      "No model found — run ENABLE_ML=1 and trigger Dagster schedule"

        st.markdown(f"""
<div class="r5-ai-hero">
  <div style="display:flex;align-items:center;justify-content:space-between;
              flex-wrap:wrap;gap:12px;">
    <div>
      <div style="font-size:36px;font-weight:950;">⚡ AI-Powered 7-Day Forecast</div>
      <div style="font-size:15px;font-weight:800;opacity:0.9;margin-top:6px;">
        Random Forest · Stockholm Congestion Prediction
      </div>
    </div>
    <span class="r5-model-status {status_cls}">{status_icon} {status_txt}</span>
  </div>
</div>
""", unsafe_allow_html=True)
        st.write("")

        if not model_trained:
            r5_alert(
                "No trained model found. "
                "To train: set <code>ENABLE_ML=1</code> and trigger the "
                "<code>weekly_model_training</code> Dagster schedule, or run:<br>"
                "<code>python ml_models/congestion_predictor.py train</code>"
            )
        else:
            # ── [ML] 4 Model Metric KPI cards ─────────────────────────────
            m1, m2, m3, m4 = st.columns(4)
            with m1:
                st.markdown(f"""
<div class="r5-ml-metric">
  <div class="r5-ml-label">Test MAE</div>
  <div class="r5-ml-val">{ml_metrics.get("test_mae","—")}</div>
  <div class="r5-ml-sub">Mean absolute error (lower = better)</div>
</div>""", unsafe_allow_html=True)
            with m2:
                st.markdown(f"""
<div class="r5-ml-metric">
  <div class="r5-ml-label">Test R²</div>
  <div class="r5-ml-val">{ml_metrics.get("test_r2","—")}</div>
  <div class="r5-ml-sub">Variance explained (higher = better)</div>
</div>""", unsafe_allow_html=True)
            with m3:
                st.markdown(f"""
<div class="r5-ml-metric">
  <div class="r5-ml-label">CV MAE</div>
  <div class="r5-ml-val">{ml_metrics.get("cv_mae","—")}</div>
  <div class="r5-ml-sub">5-fold time-series cross-validation</div>
</div>""", unsafe_allow_html=True)
            with m4:
                st.markdown(f"""
<div class="r5-ml-metric">
  <div class="r5-ml-label">Features</div>
  <div class="r5-ml-val">{ml_metrics.get("n_features","—")}</div>
  <div class="r5-ml-sub">{ml_metrics.get("n_training","—")} train · {ml_metrics.get("n_test","—")} test rows</div>
</div>""", unsafe_allow_html=True)

            st.write("")

        # ── Load predictions ───────────────────────────────────────────────
        pred_raw = get_predictions()

        if pred_raw.empty:
            r5_alert(
                "<strong>No predictions available yet.</strong> "
                "Run the model: <code>python ml_models/congestion_predictor.py predict</code> "
                "or trigger the <code>daily_prediction_generation</code> Dagster schedule."
            )
        else:
            # Normalise columns
            pred_raw.columns = [c.lower() for c in pred_raw.columns]
            for col in ("timestamp", "date"):
                if col in pred_raw.columns:
                    pred_raw[col] = pd.to_datetime(pred_raw[col], errors="coerce")
            if "day_name" not in pred_raw.columns and "day_of_week" in pred_raw.columns:
                day_map = {0:"Mon",1:"Tue",2:"Wed",3:"Thu",4:"Fri",5:"Sat",6:"Sun"}
                pred_raw["day_name"] = pred_raw["day_of_week"].map(day_map)

            # ── Station selector ───────────────────────────────────────────
            stations_avail = ["All Stations"]
            if "station_name" in pred_raw.columns:
                stations_avail += sorted(pred_raw["station_name"].dropna().unique().tolist())

            sel_col, _ = st.columns([2, 3])
            with sel_col:
                station_sel = st.selectbox(
                    "🚉 Station forecast", stations_avail, key="pred_station"
                )

            if station_sel == "All Stations":
                df_plot = pred_raw.copy()
            else:
                df_plot = pred_raw[pred_raw["station_name"] == station_sel].copy()

            # ── 7-day hourly forecast line chart ──────────────────────────
            #st.markdown('<div class="r5-white-card">', unsafe_allow_html=True)
            st.markdown( f"""<div class="r5-white-card">
                <div style="font-size:22px;font-weight:950;margin-bottom:14px;">
                📈 7-Day Hourly Forecast
                {"  — " + station_sel if station_sel != "All Stations" else " — All Stations (avg)"}
                </div>""", unsafe_allow_html=True,
            )

            if "timestamp" in df_plot.columns and "predicted_congestion" in df_plot.columns:
                if station_sel == "All Stations":
                    chart_df = (
                        df_plot.groupby("timestamp", as_index=False)["predicted_congestion"].mean()
                    )
                else:
                    chart_df = df_plot[["timestamp", "predicted_congestion"]].copy()

                # Background colour bands by congestion level
                fig_fc = go.Figure()

                # Shaded regions for levels
                x_min = chart_df["timestamp"].min()
                x_max = chart_df["timestamp"].max()
                for band_y0, band_y1, band_col, band_lbl in [
                    (0,  25,  "rgba(16,185,129,0.08)",  "Low"),
                    (25, 50,  "rgba(234,179,8,0.08)",   "Moderate"),
                    (50, 75,  "rgba(249,115,22,0.08)",  "High"),
                    (75, 100, "rgba(239,68,68,0.08)",   "Critical"),
                ]:
                    fig_fc.add_hrect(
                        y0=band_y0, y1=band_y1, fillcolor=band_col,
                        line_width=0, annotation_text=band_lbl,
                        annotation_position="right",
                        annotation_font=dict(size=10, color="#9ca3af"),
                    )

                fig_fc.add_trace(go.Scatter(
                    x=chart_df["timestamp"],
                    y=chart_df["predicted_congestion"],
                    mode="lines",
                    name="Predicted Congestion",
                    line=dict(width=2.5, color="#4f46e5"),
                    fill="tozeroy",
                    fillcolor="rgba(79,70,229,0.08)",
                    hovertemplate="%{x|%a %d %b %H:%M}<br>Congestion: <b>%{y:.1f}%</b><extra></extra>",
                ))

                fig_fc.update_layout(
                    template="plotly_white", height=380,
                    margin=dict(l=10, r=80, t=10, b=10),
                    hovermode="x unified",
                    xaxis=dict(title="", tickformat="%a %d"),
                    yaxis=dict(title="Congestion %", range=[0, 100]),
                    showlegend=False,
                )
                st.plotly_chart(fig_fc, use_container_width=True, config={"displayModeBar": False})
            else:
                r5_alert("Prediction data is missing required columns (timestamp, predicted_congestion).")
            st.markdown("</div>", unsafe_allow_html=True)

            st.write("")

            # ── Row 2: Peak-hour heatmap  |  Station ranking ──────────────
            col_heat, col_rank = st.columns(2)

            with col_heat:
                st.markdown('<div class="r5-white-card">'
                '<div style="font-size:20px;font-weight:950;margin-bottom:12px;">'
                    '🕐 Peak Hour Heatmap</div>',
                    unsafe_allow_html=True,
                )

                if "hour" in df_plot.columns and "day_of_week" in df_plot.columns \
                        and "predicted_congestion" in df_plot.columns:
                    heat_df = (
                        df_plot.groupby(["day_of_week", "hour"], as_index=False)
                        ["predicted_congestion"].mean()
                    )
                    heat_pivot = heat_df.pivot(
                        index="day_of_week", columns="hour", values="predicted_congestion"
                    ).fillna(0)

                    day_labels = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
                    y_labels   = [day_labels[i] for i in heat_pivot.index if i < 7]

                    fig_heat = go.Figure(go.Heatmap(
                        z=heat_pivot.values,
                        x=list(heat_pivot.columns),
                        y=y_labels,
                        colorscale=[
                            [0.0,  "#d1fae5"],
                            [0.25, "#fef3c7"],
                            [0.5,  "#fed7aa"],
                            [0.75, "#fecaca"],
                            [1.0,  "#991b1b"],
                        ],
                        zmin=0, zmax=100,
                        colorbar=dict(
                            title="Cong %", thickness=12,
                            tickvals=[0, 25, 50, 75, 100],
                            ticktext=["0", "25", "50", "75", "100"],
                        ),
                        hovertemplate="%{y} %{x}:00 → <b>%{z:.0f}%</b><extra></extra>",
                    ))
                    fig_heat.update_layout(
                        template="plotly_white", height=280,
                        margin=dict(l=10, r=10, t=10, b=10),
                        xaxis=dict(title="Hour of day", dtick=3),
                        yaxis=dict(title=""),
                    )
                    st.plotly_chart(fig_heat, use_container_width=True,
                                   config={"displayModeBar": False})
                else:
                    r5_alert("Need hour & day_of_week columns for heatmap.")
                st.markdown("</div>", unsafe_allow_html=True)

            with col_rank:
                st.markdown('<div class="r5-white-card">', unsafe_allow_html=True)
                st.markdown(
                    '<div style="font-size:20px;font-weight:950;margin-bottom:12px;">'
                    '🏆 Station Congestion Ranking</div>',
                    unsafe_allow_html=True,
                )

                if "station_name" in pred_raw.columns and "predicted_congestion" in pred_raw.columns:
                    rank_df = (
                        pred_raw.groupby("station_name", as_index=False)
                        ["predicted_congestion"].mean()
                        .sort_values("predicted_congestion", ascending=True)
                    )
                    colours = [
                        "#991b1b" if v >= 75 else
                        "#9a3412" if v >= 50 else
                        "#92400e" if v >= 25 else
                        "#065f46"
                        for v in rank_df["predicted_congestion"]
                    ]
                    fig_rank = go.Figure(go.Bar(
                        x=rank_df["predicted_congestion"],
                        y=rank_df["station_name"],
                        orientation="h",
                        marker_color=colours,
                        text=[f"{v:.1f}%" for v in rank_df["predicted_congestion"]],
                        textposition="outside",
                        hovertemplate="%{y}: <b>%{x:.1f}%</b><extra></extra>",
                    ))
                    fig_rank.update_layout(
                        template="plotly_white", height=280,
                        margin=dict(l=10, r=50, t=10, b=10),
                        xaxis=dict(title="Avg predicted congestion", range=[0, 110]),
                        yaxis=dict(title=""),
                    )
                    st.plotly_chart(fig_rank, use_container_width=True,
                                   config={"displayModeBar": False})
                else:
                    r5_alert("No station_name column in predictions.")
                st.markdown("</div>", unsafe_allow_html=True)

            # ── Feature importance chart ───────────────────────────────────
            if not fi_df.empty and "feature" in fi_df.columns and "importance" in fi_df.columns:
                st.write("")
                st.markdown('<div class="r5-white-card">', unsafe_allow_html=True)
                st.markdown(
                    '<div style="font-size:20px;font-weight:950;margin-bottom:12px;">'
                    '🔍 Feature Importance  <span style="font-size:13px;font-weight:700;'
                    'color:#9ca3af;">(top 15 — what drives congestion predictions)</span></div>',
                    unsafe_allow_html=True,
                )
                fi_top = fi_df.nlargest(15, "importance")
                # Clean feature names for display
                fi_top = fi_top.copy()
                fi_top["feature_label"] = (
                    fi_top["feature"]
                    .str.replace("_", " ")
                    .str.replace("congestion score", "cong")
                    .str.title()
                )
                fig_fi = go.Figure(go.Bar(
                    x=fi_top["importance"],
                    y=fi_top["feature_label"],
                    orientation="h",
                    marker=dict(
                        color=fi_top["importance"],
                        colorscale=[[0,"#e0e7ff"],[1,"#4f46e5"]],
                        showscale=False,
                    ),
                    text=[f"{v:.3f}" for v in fi_top["importance"]],
                    textposition="outside",
                    hovertemplate="%{y}: <b>%{x:.4f}</b><extra></extra>",
                ))
                fig_fi.update_layout(
                    template="plotly_white", height=460,
                    margin=dict(l=10, r=60, t=10, b=10),
                    xaxis=dict(title="Importance score"),
                    yaxis=dict(title="", autorange="reversed"),
                )
                st.plotly_chart(fig_fi, use_container_width=True,
                               config={"displayModeBar": False})
                st.markdown("</div>", unsafe_allow_html=True)

    # ════════════════════════════════════════
    # TAB 3 — ANALYSIS
    # ════════════════════════════════════════
    with analysis_tab:
        st.markdown("""
<div class="r5-white-card">
  <div style="font-size:28px;font-weight:700;margin-bottom:16px;">Deep Analysis</div>
""", unsafe_allow_html=True)

        df_station = get_station_performance(limit=10)

        if df_station.empty:
            r5_alert(
                "<strong>No station performance data found.</strong> "
                "Build <code>fact_station_performance</code> first: "
                "<code>dbt build --select fact_station_performance</code>"
            )
        else:
            left_col, right_col = st.columns(2)
            cols = [left_col, right_col]

            for i, row in enumerate(df_station.itertuples(index=False)):
                on_time_val = float(row.on_time_rate      or 0)
                delay_val   = float(row.avg_delay_minutes  or 0)
                dep_val     = int(  row.total_departures   or 0)

                ot_colour = (
                    "#16a34a" if on_time_val >= 80 else
                    "#eab308" if on_time_val >= 60 else
                    "#dc2626"
                )

                with cols[i % 2]:
                    st.markdown(f"""
<div style="border:1px solid rgba(0,0,0,0.10);border-radius:14px;
            padding:16px;margin-bottom:14px;background:white;">
  <div style="font-weight:950;font-size:18px;margin-bottom:10px;">
    {row.station_name}
  </div>
  <div style="font-size:13px;margin-bottom:6px;">
    <span style="color:#111827;font-weight:800;">Departures:</span> {dep_val:,}
  </div>
  <div style="font-size:13px;margin-bottom:6px;">
    <span style="color:#dc2626;font-weight:900;">Avg Delay:</span> {delay_val:.1f} min
  </div>
  <div style="font-size:13px;margin-bottom:10px;">
    <span style="color:{ot_colour};font-weight:900;">On-Time:</span> {on_time_val:.0f}%
  </div>
  <div style="width:100%;height:8px;background:#e5e7eb;
              border-radius:999px;overflow:hidden;">
    <div style="width:{min(on_time_val, 100):.1f}%;height:100%;
                background:{ot_colour};"></div>
  </div>
</div>
""", unsafe_allow_html=True)

        st.markdown("</div>", unsafe_allow_html=True)


if __name__ == "__main__":
    main()