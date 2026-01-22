import os
import sys
import time
from datetime import datetime, timedelta
from typing import Iterator, Dict, Any, Optional, List, Union

from pathlib import Path
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

import dlt

# -----------------------------
# ENV
# -----------------------------
load_dotenv()

# Trafiklab Realtime Timetables API base
BASE_URL = "https://realtime-api.trafiklab.se/v1"

# Stockholm C / T-Centralen (hub)
DEFAULT_AREA_ID = "740000001"

# If you want a bit wider coverage, you can add more area ids here
DEFAULT_AREA_IDS = [DEFAULT_AREA_ID]  # keep simple: only Stockholm C


# -----------------------------
# Helpers
# -----------------------------
def _safe_str(x: Any) -> str:
    """Convert anything to string safely (None -> '')."""
    if x is None:
        return ""
    return str(x)


def _requests_session() -> requests.Session:
    """Requests session with retry/backoff for unstable connections."""
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


SESSION = _requests_session()


def _call_timetables_departures(area_id: str, when: Optional[datetime] = None) -> Dict[str, Any]:
    """
    Call departures endpoint:
      - current:  /departures/{area_id}?key=...
      - at time:  /departures/{area_id}/{YYYY-MM-DDTHH:MM}?key=...
    """
    api_key = os.getenv("REALTIME_API_KEY")
    if not api_key:
        raise RuntimeError(
            "REALTIME_API_KEY is not set. Put it in .env or env vars.\n"
            "Example .env:\nREALTIME_API_KEY=your_key_here\n"
        )

    if when is None:
        url = f"{BASE_URL}/departures/{area_id}"
    else:
        time_str = when.strftime("%Y-%m-%dT%H:%M")
        url = f"{BASE_URL}/departures/{area_id}/{time_str}"

    params = {"key": api_key}

    last_err: Optional[Exception] = None
    for attempt in range(1, 6):
        try:
            resp = SESSION.get(url, params=params, timeout=(10, 60))
            resp.raise_for_status()
            data = resp.json()

            # Validate expected structure
            if "departures" not in data:
                raise ValueError(
                    f"Unexpected API response structure. Keys: {list(data.keys())}. "
                    f"Check API key / subscription / endpoint."
                )
            return data

        except (
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as e:
            last_err = e
            sleep_s = min(2 ** attempt, 20)
            print(f"[WARN] Request failed ({type(e).__name__}) attempt {attempt}/5. Sleeping {sleep_s}s...")
            time.sleep(sleep_s)

        except requests.exceptions.HTTPError as e:
            last_err = e
            code = getattr(e.response, "status_code", None)
            if code == 401:
                raise RuntimeError("Authentication failed (401). Check REALTIME_API_KEY.") from e
            if code == 403:
                raise RuntimeError("Access forbidden (403). Check subscription / endpoint access.") from e
            if code == 404:
                raise RuntimeError(f"Area ID not found (404): {area_id}") from e

            if code in (429, 500, 502, 503, 504):
                sleep_s = min(2 ** attempt, 20)
                print(f"[WARN] HTTP {code} attempt {attempt}/5. Sleeping {sleep_s}s...")
                time.sleep(sleep_s)
                continue

            # Try to show API error message
            try:
                msg = e.response.json().get("message", str(e))
            except Exception:
                msg = str(e)
            raise RuntimeError(f"HTTP {code}: {msg}") from e

    raise RuntimeError(f"Failed to fetch departures after retries. Last error: {last_err}")


def _categorize_transport_mode(
    transport_mode: Optional[str],
    transport_mode_code: Any,
    agency_operator: Optional[str],
    route_name: Optional[str],
) -> str:
    """
    Robust categorization. Handles transport_mode_code sometimes being int.
    """
    mode = (_safe_str(transport_mode)).upper()
    code = (_safe_str(transport_mode_code)).upper()
    agency = (_safe_str(agency_operator)).upper()
    route = (_safe_str(route_name)).upper()

    if not mode:
        return "UNKNOWN"

    # Metro
    if mode == "METRO":
        return "Metro (Green/Red/Blue)"

    # Bus
    if mode == "BUS":
        return "SL Bus"

    # Train (Pendeltåg vs SJ)
    if mode == "TRAIN":
        if "PENDEL" in route or "PENDEL" in agency or code in ("PENDEL", "PEN"):
            return "Pendeltåg"
        if "SJ" in agency:
            return "National Rail (SJ)"
        return "Train (Other)"

    # Tram etc.
    if mode == "TRAM":
        return "Tram"

    return mode


def _flatten_departures(response: Dict[str, Any], requested_area_id: str) -> Iterator[Dict[str, Any]]:
    """
    Flatten each departure record into one row.
    Adds requested_area_id to track which stop was queried.
    """
    timestamp = response.get("timestamp")
    query = response.get("query", {}) or {}
    query_time = query.get("queryTime")
    query_area_id = query.get("query")

    for dep in response.get("departures", []):
        route = dep.get("route", {}) or {}
        trip = dep.get("trip", {}) or {}
        agency = dep.get("agency", {}) or {}
        stop = dep.get("stop", {}) or {}

        transport_mode = route.get("transport_mode")
        transport_mode_code = route.get("transport_mode_code")
        agency_operator = agency.get("operator")
        route_name = route.get("name")

        transport_category = _categorize_transport_mode(
            transport_mode=transport_mode,
            transport_mode_code=transport_mode_code,
            agency_operator=agency_operator,
            route_name=route_name,
        )

        yield {
            # ✅ NEW: which stop we requested
            "requested_area_id": _safe_str(requested_area_id),

            # Metadata
            "response_timestamp": timestamp,
            "query_time": query_time,
            "query_area_id": query_area_id,

            # Departure timing
            "scheduled_time": dep.get("scheduled"),
            "realtime_time": dep.get("realtime"),
            "delay_seconds": dep.get("delay"),
            "canceled": dep.get("canceled"),
            "is_realtime": dep.get("is_realtime"),

            # Platforms (paid/free sometimes differ)
            "scheduled_platform": dep.get("scheduled_platform") or dep.get("platform"),
            "realtime_platform": dep.get("realtime_platform") or dep.get("platform"),

            # Route info
            "route_name": route_name,
            "route_designation": route.get("designation"),
            "route_transport_mode_code": transport_mode_code,
            "route_transport_mode": transport_mode,
            "route_direction": route.get("direction"),

            "transport_category": transport_category,

            # Origin/destination
            "origin_stop_id": (route.get("origin") or {}).get("id"),
            "origin_stop_name": (route.get("origin") or {}).get("name"),
            "destination_stop_id": (route.get("destination") or {}).get("id"),
            "destination_stop_name": (route.get("destination") or {}).get("name"),

            # Trip info
            "trip_id": trip.get("trip_id"),
            "trip_start_date": trip.get("start_date"),
            "trip_technical_number": trip.get("technical_number"),

            # Agency
            "agency_id": agency.get("id"),
            "agency_name": agency.get("name"),
            "agency_operator": agency_operator,

            # Stop info
            "stop_id": stop.get("id"),
            "stop_name": stop.get("name"),
            "stop_lat": stop.get("lat"),
            "stop_lon": stop.get("lon"),
        }


# -----------------------------
# DLT Source
# -----------------------------
@dlt.source
def trafiklab_realtime_source(
    area_ids: Optional[Union[str, List[str]]] = None,
    backfill_last_hours: int = 0,
):
    """
    area_ids: single or list. default: Stockholm C only.
    backfill_last_hours:
        0 => only current call
        4 => calls departures at (now-4h, now-3h, now-2h, now-1h) + current
    """
    if area_ids is None:
        area_ids = DEFAULT_AREA_IDS
    elif isinstance(area_ids, str):
        area_ids = [area_ids]

    # Use MERGE so duplicates across backfill/current are upserted by PK
    @dlt.resource(
        name="trafiklab_departures",
        write_disposition="merge",
        primary_key=["trip_id", "scheduled_time", "stop_id"],
        # ✅ Important: requested_area_id is nullable; NO constraints (DuckDB-safe)
        columns={
            "requested_area_id": {"data_type": "text", "nullable": True},
            "trip_id": {"data_type": "text", "nullable": True},
            "scheduled_time": {"data_type": "timestamp", "nullable": True},
            "stop_id": {"data_type": "text", "nullable": True},
            "transport_category": {"data_type": "text", "nullable": True},
            "route_name": {"data_type": "text", "nullable": True},
            "scheduled_platform": {"data_type": "text", "nullable": True},
            "realtime_platform": {"data_type": "text", "nullable": True},
        },
    )
    def departures() -> Iterator[Dict[str, Any]]:
        # Build list of "when" timestamps for backfill (hourly) + current (None)
        whens: List[Optional[datetime]] = []
        if backfill_last_hours and backfill_last_hours > 0:
            now = datetime.now()
            # hourly points: now-4h, now-3h, now-2h, now-1h
            for h in range(backfill_last_hours, 0, -1):
                whens.append(now - timedelta(hours=h))
        whens.append(None)  # current

        for area_id in area_ids:
            for when in whens:
                try:
                    if when is None:
                        print(f"[INFO] Fetching CURRENT departures for area_id={area_id}")
                    else:
                        print(f"[INFO] Fetching departures for area_id={area_id} at {when.strftime('%Y-%m-%d %H:%M')}")

                    resp = _call_timetables_departures(area_id=area_id, when=when)
                    yield from _flatten_departures(resp, requested_area_id=_safe_str(area_id))

                except Exception as e:
                    print(f"[WARN] Failed for area_id={area_id} when={when}: {e}")
                    continue

    return departures()


# -----------------------------
# Runner
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # repo root (dlt_pipeline/..)
DUCKDB_PATH = PROJECT_ROOT / "warehouse" / "trafiklab_realtime.duckdb"


def run_once(
    area_ids: Optional[Union[str, List[str]]] = None,
    backfill_last_hours: int = 0,
):
    api_key = os.getenv("REALTIME_API_KEY")
    if not api_key:
        print("[ERROR] REALTIME_API_KEY not found. Put it in .env.")
        sys.exit(1)

    print(f"[INFO] Loading Trafiklab departures into: {DUCKDB_PATH}")
    print(f"[INFO] area_ids={area_ids or DEFAULT_AREA_IDS} | backfill_last_hours={backfill_last_hours}")

    pipeline = dlt.pipeline(
        pipeline_name="trafiklab_realtime",
        destination=dlt.destinations.duckdb(str(DUCKDB_PATH)),
        dataset_name="raw_trafiklab",
    )

    load_info = pipeline.run(
        trafiklab_realtime_source(area_ids=area_ids, backfill_last_hours=backfill_last_hours)
    )

    print("[SUCCESS] Pipeline completed")
    print(load_info)
    print(f"[INFO] DuckDB path: {DUCKDB_PATH}")


if __name__ == "__main__":
    # Default: Stockholm C only, current
    # If you want 4h backfill, set backfill_last_hours=4
    run_once(
    area_ids=[
        "740000001",  # Stockholm C (SJ)
        "740000003",  # Slussen (Metro)
        "740000002",  # Gamla Stan (Metro)
        "740000004",  # Odenplan (Bus + Pendeltåg)
        "740000005",  # Stockholm Södra (Pendeltåg)
    ],
    backfill_last_hours=4
)

