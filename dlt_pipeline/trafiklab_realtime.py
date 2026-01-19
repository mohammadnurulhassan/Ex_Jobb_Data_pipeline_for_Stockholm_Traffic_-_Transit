import os
import time
from datetime import datetime
from typing import Iterator, Dict, Any

import dlt
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

# Load variables from .env (REALTIME_API_KEY)
load_dotenv()

# ✅ Correct base URL for Trafiklab Realtime Timetables API
BASE_URL = "https://realtime-api.trafiklab.se/v1"

# Stockholm C (you can change later if needed)
DEFAULT_AREA_ID = "740000001"


def _requests_session() -> requests.Session:
    """Requests session with retry/backoff for unstable connections."""
    session = requests.Session()

    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=1.0,  # 1s, 2s, 4s, 8s...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


SESSION = _requests_session()


def _call_timetables_departures(area_id: str, when: datetime | None = None) -> Dict[str, Any]:
    """
    Call Trafiklab Realtime Timetables 'departures' endpoint for one area_id.

    Example:
      https://realtime-api.trafiklab.se/v1/departures/{area_id}?key=API_KEY
    """
    api_key = os.getenv("REALTIME_API_KEY")
    if not api_key:
        raise RuntimeError("REALTIME_API_KEY is not set. Put it in .env or environment variables.")

    if when is None:
        url = f"{BASE_URL}/departures/{area_id}"
    else:
        time_str = when.strftime("%Y-%m-%dT%H:%M")
        url = f"{BASE_URL}/departures/{area_id}/{time_str}"

    params = {"key": api_key}

    # Manual retry layer for ChunkedEncodingError / ProtocolError type issues
    last_err: Exception | None = None
    for attempt in range(1, 6):
        try:
            resp = SESSION.get(url, params=params, timeout=(10, 60))  # connect, read
            resp.raise_for_status()
            return resp.json()

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
            if code in (429, 500, 502, 503, 504):
                sleep_s = min(2 ** attempt, 20)
                print(f"[WARN] HTTP {code} attempt {attempt}/5. Sleeping {sleep_s}s...")
                time.sleep(sleep_s)
                continue
            raise

    raise RuntimeError(f"Failed to fetch departures after retries. Last error: {last_err}")


def _flatten_departures(response: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
    """
    Take JSON response from Trafiklab Realtime Timetables and yield flat rows
    for each departure (good for analytics).
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

        yield {
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

            # Route info
            "route_name": route.get("name"),
            "route_designation": route.get("designation"),
            "route_transport_mode_code": route.get("transport_mode_code"),
            "route_transport_mode": route.get("transport_mode"),
            "route_direction": route.get("direction"),

            # Origin / destination
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
            "agency_operator": agency.get("operator"),

            # Stop (where this departure happens)
            "stop_id": stop.get("id"),
            "stop_name": stop.get("name"),
            "stop_lat": stop.get("lat"),
            "stop_lon": stop.get("lon"),
        }


@dlt.resource(
    name="trafiklab_departures",
    write_disposition="append",
    primary_key=["trip_id", "scheduled_time"],
    columns={
        "route__name": {"data_type": "text"},
        "scheduled_platform": {"data_type": "text", "nullable": True},
        "realtime_platform": {"data_type": "text", "nullable": True},
    },
)
def trafiklab_departures_resource(
    area_id: str = DEFAULT_AREA_ID,
    when: datetime | None = None,
) -> Iterator[Dict[str, Any]]:
    """dlt resource: calls API and yields flattened departure rows."""
    response = _call_timetables_departures(area_id=area_id, when=when)
    yield from _flatten_departures(response)


@dlt.source
def trafiklab_realtime_source(
    area_id: str = DEFAULT_AREA_ID,
    when: datetime | None = None,
):
    """dlt source combining all resources (for now only departures)."""
    return trafiklab_departures_resource(area_id=area_id, when=when)


# --- Destination path in warehouse/ ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DUCKDB_PATH = PROJECT_ROOT / "warehouse" / "trafiklab_realtime.duckdb"


def run_once(
    area_id: str = DEFAULT_AREA_ID,
    when: datetime | None = None,
):
    """Helper to run pipeline once from CLI / VS Code."""
    pipeline = dlt.pipeline(
        pipeline_name="trafiklab_realtime",
        destination=dlt.destinations.duckdb(str(DUCKDB_PATH)),
        dataset_name="raw_trafiklab",
    )

    load_info = pipeline.run(trafiklab_realtime_source(area_id=area_id, when=when))
    print("Load info:", load_info)
    print("DuckDB path:", DUCKDB_PATH)


if __name__ == "__main__":
    run_once()
