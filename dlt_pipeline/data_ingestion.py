"""
DLT ingestion: SL Transport API (replaces SL Departures v4)
- Fetches realtime departures for selected Stockholm sites
- Writes into ./warehouse/stockholm_traffic.duckdb (project root)
- Keeps REALTIME_API_KEY in env
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import dlt
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os
from config import (
    DUCKDB_PATH,
    DUCKDB_CREDENTIALS,
    SL_TRANSPORT_BASE_URL,
    STOCKHOLM_SITES,
    RATE_LIMIT_DELAY_SECONDS,
    API_TIMEOUT_SECONDS,
    DLT_PIPELINE_NAME,
    DLT_DESTINATION,
    DLT_DATASET_NAME,
    REALTIME_API_KEY,  # optional
)




# -----------------------------
# SL Transport API (NEW)
# -----------------------------
BASE_URL = SL_TRANSPORT_BASE_URL
STOCKHOLM_SITES = STOCKHOLM_SITES



# -----------------------------
# HTTP helpers
# -----------------------------
def _make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(
        {
            "User-Agent": "ExJobb-Stockholm-Traffic-DLT/1.0",
            "Accept": "application/json",
        }
    )
    return s


def _pick(d: Any, *keys: str, default=None):
    """Pick first existing non-null key from dict-like object."""
    if not isinstance(d, dict):
        return default
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default


def _normalize_departures(payload: Any) -> List[Dict[str, Any]]:
    """
    SL Transport responses may differ slightly by version/endpoint.
    This tries common shapes safely.
    """
    if payload is None:
        return []
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]

    if isinstance(payload, dict):
        # Most common
        for key in ("departures", "Departures"):
            v = payload.get(key)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]

        # Sometimes nested
        data = payload.get("data")
        if isinstance(data, dict):
            v = data.get("departures")
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]

    return []


class SLTransportClient:
    def __init__(self):
        self.session = _make_session()

    def fetch_departures(self, site_id: int) -> List[Dict[str, Any]]:
        url = f"{BASE_URL}/sites/{site_id}/departures"
        try:
            r = self.session.get(url, timeout=API_TIMEOUT_SECONDS)
            time.sleep(RATE_LIMIT_DELAY_SECONDS)
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Request error for site {site_id}: {e}")
            return []

        if r.status_code >= 400:
            print(f"   ❌ HTTP {r.status_code} for site {site_id}: {r.text[:160]}")
            return []

        try:
            payload = r.json()
        except Exception as e:
            print(f"   ❌ JSON decode error for site {site_id}: {e}")
            return []

        return _normalize_departures(payload)


# -----------------------------
# DLT Resource
# -----------------------------
@dlt.resource(
    name="realtime_departures",
    write_disposition="append",
    columns={
        "site_id": {"data_type": "bigint"},
        "site_name": {"data_type": "text"},
        "line": {"data_type": "text"},
        "destination": {"data_type": "text"},
        "direction": {"data_type": "text"},
        "transport_mode": {"data_type": "text"},  # force materialization even if null
        "expected_datetime": {"data_type": "text"},
        "scheduled_datetime": {"data_type": "text"},
        "stop_point": {"data_type": "text"},
        "deviations_raw": {"data_type": "text"},
        "has_deviation": {"data_type": "bool"},
        "ingestion_timestamp_utc": {"data_type": "text"},
    },
)
def realtime_departures() -> Iterator[Dict[str, Any]]:
    client = SLTransportClient()
    ingestion_ts = datetime.now(timezone.utc).isoformat()

    ok_sites = 0
    total_rows = 0

    for site_id, site_name in STOCKHOLM_SITES.items():
        print(f"📍 Fetching {site_name} (site_id={site_id})...")
        deps = client.fetch_departures(site_id)

        if not deps:
            continue

        ok_sites += 1
        site_rows = 0

        for dep in deps:
            deviations = _pick(dep, "deviations", "Deviations", default=[])

            yield {
                "site_id": site_id,
                "site_name": site_name,
                "line": _pick(dep, "line", "line_number", "lineNumber", "LineNumber"),
                "destination": _pick(dep, "destination", "Destination"),
                "direction": _pick(dep, "direction", "journey_direction", "JourneyDirection"),
                "transport_mode": _pick(dep, "transport_mode", "transportMode", "TransportMode"),
                "expected_datetime": _pick(dep, "expected", "expected_datetime", "expectedDateTime", "ExpectedDateTime"),
                "scheduled_datetime": _pick(dep, "scheduled", "scheduled_datetime", "timeTabledDateTime", "TimeTabledDateTime"),
                "stop_point": _pick(dep, "stop_point", "stopPoint", "stopPointDesignation", "StopPointDesignation"),
                "deviations_raw": str(deviations),
                "has_deviation": bool(deviations),
                "ingestion_timestamp_utc": ingestion_ts,
            }
            site_rows += 1

        total_rows += site_rows
        print(f"   ✅ rows: {site_rows}")

        # gentle rate limiting
        time.sleep(0.3)

    print("\n📊 Summary")
    print(f"   Successful sites: {ok_sites}/{len(STOCKHOLM_SITES)}")
    print(f"   Total rows: {total_rows}")

    if ok_sites == 0:
        print("\n❌ WARNING: No data collected.")
        print("   If this happens, test DNS/internet and try again.")

    



# -----------------------------
# Run Pipeline
# -----------------------------
def run_dlt_pipeline():
    print("\n" + "=" * 70)
    print("🚇 STARTING DLT DATA INGESTION (SL Transport API)")
    print("=" * 70)

    # (Optional) show that you still have the key stored (not used here)
    if REALTIME_API_KEY:
        print("🔑 REALTIME_API_KEY found in .env (not used for SL Transport departures).")
    else:
        print("ℹ️  REALTIME_API_KEY not set (OK for SL Transport departures).")

    pipeline = dlt.pipeline(
        pipeline_name=DLT_PIPELINE_NAME,
        destination=DLT_DESTINATION,
        dataset_name=DLT_DATASET_NAME,
        dev_mode=False,
    )
    os.environ["DESTINATION__DUCKDB__CREDENTIALS"] = DUCKDB_CREDENTIALS
    load_info = pipeline.run([realtime_departures()])

    print("\n" + "=" * 70)
    print("✅ DLT PIPELINE COMPLETED")
    print("=" * 70)
    print("DuckDB saved at:", DUCKDB_PATH)
    print(load_info)


if __name__ == "__main__":
 run_dlt_pipeline()
