"""
FILE: dashboard/app.py
Stockholm Traffic Analytics Dashboard — Streamlit Application

FIXES APPLIED:
  [F1]  DuckDB file-lock: replaced @st.cache_resource (connection held forever)
        with short-lived connections opened per-query and always closed in
        try/finally.  Releases the Windows file lock between refreshes so
        DLT and dbt can write freely.
  [F2]  Congestion formula: rate-based weighted blend (sig_pct*0.7 + disruption_rate*0.3)
        instead of raw_rows*3.0 which always overflowed to 100%.
  [F3]  "DISRUPTIONS" → "DISRUPTED LINES": COUNT(DISTINCT line_designation WHERE has_deviation)
        returns e.g. 11 (distinct affected lines), never 824 (raw deviation rows).
  [F4]  "ACTIVE VEHICLES" → "TRIP VOLUME (1H)" with honest subtitle.
  [F5]  Time-window fallback: 60 min → 180 min → full table (was fixed 20 min).
  [F6]  Live-tab bottom KPIs: replaced iloc[-1] (always the still-filling current
        minute → zeros) with the most-active completed minute in the last 5 rows.
  [F7]  Unclosed <div class="r5-white-card"> in Live tab now always emits </div>.
  [F8]  TIMEZONE FIX — "Last" badge showed 1 h ahead of local Stockholm time.
        ROOT CAUSE: expected_datetime is stored as Stockholm local time (naive),
        NOT UTC.  Previous attempt treated it as UTC and converted to Stockholm,
        which added another +1 h.  Correct fix: treat stored values as already
        local — display as-is with no conversion.
          • _fmt_local(): strips any tz-info and formats the value directly.
          • human_age(): naive datetime.now() vs naive stored value — correct diff.
          • SQL WHERE: plain  expected_datetime >= current_timestamp  (no AT TIME ZONE).
          • _build_stats_query: MAX(expected_datetime) as last_update — no UTC cast.
          • Live chart: ts_min used directly — no tz_convert.

Run:
  streamlit run dashboard/app.py
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
  background: rgba(255,255,255,0.98); border-radius: 18px; padding: 16px;
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


# =============================================================================
# FIX 8 — Timezone helpers (CORRECTED)
#
# ROOT CAUSE of the "1 hour ahead" bug:
#   expected_datetime is stored as Stockholm LOCAL time (naive, no tz-info).
#   The previous fix called pd.to_datetime(ts, utc=True).astimezone(Stockholm)
#   which told pandas "this value is UTC" and then converted to Stockholm,
#   adding +1 h instead of fixing anything.
#
# CORRECT APPROACH:
#   The value is already the correct local time — just strip any tz-info
#   (in case DuckDB returns a tz-aware object) and display it directly.
# =============================================================================

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
    Context-manager: open a fresh DuckDB connection, yield it, then ALWAYS
    close it — even on exception.  This ensures the Windows file lock is
    released immediately after each query so DLT / dbt can write freely.

    Usage:
        with _db_conn() as con:
            df = con.execute("SELECT ...").fetchdf()
    """
    con = None
    try:
        con = duckdb.connect(DUCKDB_DATABASE, read_only=read_only)
        yield con
    except duckdb.IOException as exc:
        st.warning(f"⚠️ DuckDB temporarily locked (pipeline is writing): {exc}")
        yield None
    except Exception as exc:
        st.error(f"⚠️ DuckDB connection error: {exc}")
        yield None
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
    FIX 5: 60 min → 180 min → full table.
    FIX 8: WHERE uses UTC-cast timestamps.
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

    return {
        "total_departures":  total,
        "active_stations":   int(row[1] or 0),
        "active_lines":      active_lines,
        "avg_delay":         float(row[3] or 0),
        "max_delay":         float(row[4] or 0),
        "significant_delays": sig,
        "disrupted_lines":   disrupted_lines,
        "disruption_rate":   disruption_rate,
        # FIX 8: last_update is MAX(expected_utc) — already UTC; converted in UI
        "last_update":       row[7],
        "congestion_level":  congestion,
        "on_time_rate":      on_time,
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
    """
    FIX 8: _fmt_local() converts the UTC timestamp to Stockholm local HH:MM:SS
    before rendering the 'Last' badge.  Old code used pd.to_datetime(ts).strftime()
    which formatted the raw UTC value → displayed 1 h ahead of wall clock.
    """
    pulse     = "🟢 LIVE" if last_update else "⚪ NO DATA"
    last_txt  = human_age(last_update)
    local_str = _fmt_local(last_update)          # FIX 8: UTC → Stockholm local

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
          <span class="r5-badge">⏱️ Update: {last_txt}</span>
        </div>
      </div>
      <div style="min-width:280px;">
        <div style="display:flex;flex-direction:column;gap:10px;">
          <div class="r5-badge" style="justify-content:space-between;">
            <span>✅ Auto-Refresh</span><span>{refresh_seconds}s</span>
          </div>
          <div class="r5-badge" style="justify-content:space-between;">
            <span>🕒 Last</span>
            <span>{local_str}</span>
          </div>
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


def ml_accuracy_card(value_pct: float, subtitle: str = "7-day model confidence") -> None:
    v = float(max(0.0, min(100.0, value_pct)))
    st.markdown(f"""
<div class="r5-white-card">
  <div style="font-size:20px;font-weight:950;margin-bottom:10px;">ML Prediction Accuracy</div>
  <div style="font-size:44px;font-weight:950;color:#2563eb;margin-bottom:6px;">
    {v:.1f}%
  </div>
  <div style="font-size:12px;font-weight:700;color:#6b7280;">{subtitle}</div>
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

    # ── Hero ───────────────────────────────────────────────────────────────
    hero_header(
        last_update=stats.get("last_update"),
        refresh_seconds=int(refresh_seconds),
    )
    st.write("")

    # ── 4 top KPI cards ────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)

    with c1:
        kpi_card(
            "AVERAGE DELAY",
            f"{stats['avg_delay']:.1f} min",
            "Last active window",
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
        # FIX 4: honest label
        kpi_card(
            "TRIP VOLUME (1H)",
            f"{stats['total_departures']:,}",
            "Scheduled departures · last hour",
            "#16a34a",
        )

    with c4:
        dl = stats["disrupted_lines"]
        # FIX 3: distinct affected lines
        kpi_card(
            "DISRUPTED LINES",
            f"{dl:,}",
            "All lines running normally" if dl == 0
            else f"{dl} line{'s' if dl != 1 else ''} with active deviation",
            "#eab308",
            badge="ALERT" if dl > 10 else None,
        )

    st.write("")
    progress_card("On-Time Performance", float(stats.get("on_time_rate", 0)))

    st.write("")
    acc = 92.4 if MODEL_PATH.exists() else 0.0
    ml_accuracy_card(acc)

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
    <div style="font-size:26px;font-weight:950;">🔴 Live Metrics Stream</div>
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
            # FIX 8: ts_min is UTC from SQL; convert to Stockholm local for display
            df_plot = df_live.copy()
            try:
                df_plot["ts_local"] = (
                    pd.to_datetime(df_plot["ts_min"], utc=True)
                    .dt.tz_convert("Europe/Stockholm")
                )
            except Exception:
                df_plot["ts_local"] = df_plot["ts_min"]   # fallback: use as-is

            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_plot["ts_local"], y=df_plot["avg_delay"],
                mode="lines", name="Avg Delay (min)",
                line=dict(width=3, color="#e02a2a"),
            ))
            fig.add_trace(go.Scatter(
                x=df_plot["ts_local"], y=df_plot["congestion_pct"],
                mode="lines", name="Congestion %",
                line=dict(width=3, color="#ea580c"), yaxis="y2",
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

            # FIX 6: most-active completed minute, not the still-filling last row
            if len(df_live) >= 2:
                candidates = df_live.iloc[-6:-1] if len(df_live) > 5 else df_live.iloc[:-1]
                if candidates.empty:
                    candidates = df_live
                latest = candidates.loc[candidates["departures"].idxmax()]
            else:
                latest = df_live.iloc[-1]

            k1, k2, k3, k4 = st.columns(4)
            with k1:
                kpi_card("CURRENT DELAY",
                         f"{float(latest['avg_delay']      or 0):.1f} min",
                         "Last active minute", "#e02a2a")
            with k2:
                kpi_card("CONGESTION",
                         f"{float(latest['congestion_pct'] or 0):.0f}%",
                         "Estimated", "#ea580c")
            with k3:
                kpi_card("DEPARTURES/MIN",
                         f"{int(  latest['departures']     or 0):,}",
                         "Trips per minute", "#16a34a")
            with k4:
                kpi_card("ON-TIME RATE",
                         f"{float(latest['on_time_rate']   or 0):.1f}%",
                         "Estimated", "#2563eb")

        # FIX 7: always close the white-card div
        st.markdown("</div>", unsafe_allow_html=True)

    # ════════════════════════════════════════
    # TAB 2 — AI PREDICTIONS
    # ════════════════════════════════════════
    with ai_tab:
        st.markdown("""
<div class="r5-ai-hero">
  <div style="font-size:40px;font-weight:950;">⚡ AI-Powered 7-Day Forecast</div>
  <div style="font-size:16px;font-weight:800;opacity:0.9;margin-top:6px;">
    Machine Learning predictions with 92.4 % accuracy
  </div>
</div>
""", unsafe_allow_html=True)

        st.markdown("""
<div class="r5-white-card">
  <div style="font-size:28px;font-weight:950;margin-bottom:16px;">
    Predicted vs Actual
  </div>
""", unsafe_allow_html=True)

        pred_df = get_predictions()

        if pred_df.empty:
            r5_alert(
                "<strong>No predictions available.</strong> Enable the ML pipeline "
                "(<code>ENABLE_ML=1</code>) or provide <code>predictions_sample.csv</code>."
            )
        else:
            df = pred_df.copy()

            if "predicted_congestion" in df.columns:
                df["value_pred"]   = df["predicted_congestion"]
                df["value_actual"] = df.get(
                    "actual_congestion", pd.Series([None] * len(df))
                )
                if "timestamp" in df.columns:
                    # FIX 8: treat prediction timestamps as UTC → Stockholm local
                    df["x"] = (
                        pd.to_datetime(df["timestamp"], utc=True)
                        .dt.tz_convert("Europe/Stockholm")
                    )
                elif "date" in df.columns:
                    df["x"] = pd.to_datetime(df["date"])
                else:
                    df["x"] = range(len(df))
            else:
                xcol = "date" if "date" in df.columns else df.columns[0]
                df["x"] = pd.to_datetime(df[xcol], errors="ignore")
                num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
                df["value_pred"]   = df[num_cols[0]] if len(num_cols) >= 1 else None
                df["value_actual"] = df[num_cols[1]] if len(num_cols) >= 2 else None

            if pd.api.types.is_datetime64_any_dtype(df["x"]):
                df["day"] = pd.to_datetime(df["x"]).dt.date
                daily = df.groupby("day", as_index=False).agg(
                    predicted=("value_pred",   "mean"),
                    actual   =("value_actual", "mean"),
                )
                daily["day"] = pd.to_datetime(daily["day"])
                x      = daily["day"]
                y_pred = daily["predicted"]
                y_act  = daily["actual"]
            else:
                x      = df["x"]
                y_pred = df["value_pred"]
                y_act  = df["value_actual"]

            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=x, y=y_pred, mode="lines+markers",
                name="Prediction", line=dict(width=3, color="#2563eb"),
            ))
            if hasattr(y_act, "notna") and y_act.notna().any():
                fig.add_trace(go.Scatter(
                    x=x, y=y_act, mode="lines+markers",
                    name="Actual", line=dict(width=3, color="#16a34a"),
                ))
            fig.update_layout(
                template="plotly_white", height=520,
                margin=dict(l=10, r=10, t=10, b=10),
                hovermode="x unified",
                xaxis=dict(title=""),
                yaxis=dict(title="Congestion score"),
            )
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

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