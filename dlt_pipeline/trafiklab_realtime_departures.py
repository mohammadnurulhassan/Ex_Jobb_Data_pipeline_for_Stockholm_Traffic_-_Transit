import os
from datetime import datetime
from typing import Iterator, Dict, Any

import dlt
from pathlib import Path
import requests
from dotenv import load_dotenv

# Load variables from .env (REALTIME_API_KEY)
load_dotenv()

BASE_URL = "https://realtime-api.trafiklab.se/v1"

# 👉 Example area_id from docs: 740000002 = Göteborg C (demo)
# Later you should replace this with a Stockholm area id
# that you fetch via Trafiklab Stop Lookup.
DEFAULT_AREA_ID = "740000002"


def _call_timetables_departures(area_id: str, when: datetime | None = None) -> Dict[str, Any]:
    """
    Call Trafiklab Realtime Timetables 'departures' endpoint for one area_id.

    Docs example:
      https://realtime-api.trafiklab.se/v1/departures/{area_id}?key=API_KEY
    """
    api_key = os.getenv("REALTIME_API_KEY")
    if not api_key:
        raise RuntimeError("REALTIME_API_KEY is not set. Put it in .env or environment variables.")

    if when is None:
        # current time window (next 60 minutes)
        url = f"{BASE_URL}/departures/{area_id}"
    else:
        # specific datetime in format YYYY-MM-DDTHH:mm
        time_str = when.strftime("%Y-%m-%dT%H:%M")
        url = f"{BASE_URL}/departures/{area_id}/{time_str}"

    # key is sent as query parameter ?key=API_KEY
    response = requests.get(url, params={"key": api_key}, timeout=30)
    response.raise_for_status()
    data = response.json()
    return data


def _flatten_departures(response: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
    """
    Take the JSON response from Trafiklab Timetables and yield flat rows
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

        # One flattened row per departure
        yield {
            # Metadata about this API call
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
    write_disposition="append",  # append new rows each run
    primary_key=["trip_id", "scheduled_time"]  # a simple composite key
)
def trafiklab_departures_resource(
    area_id: str = DEFAULT_AREA_ID,
    when: datetime | None = None,
) -> Iterator[Dict[str, Any]]:
    """
    dlt resource: calls the API and yields flattened departure rows.
    """
    response = _call_timetables_departures(area_id=area_id, when=when)
    yield from _flatten_departures(response)


@dlt.source
def trafiklab_realtime_source(
    area_id: str = DEFAULT_AREA_ID,
    when: datetime | None = None,
):
    """
    dlt source combining all resources (for now only departures).
    Later you can add arrivals or multiple stops here.
    """
    return trafiklab_departures_resource(area_id=area_id, when=when)


# --- Destination path in warehouse/ ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DUCKDB_PATH = PROJECT_ROOT / "warehouse" / "trafiklab_realtime.duckdb"


def run_once(
    area_id: str = DEFAULT_AREA_ID,
    when: datetime | None = None,
):
    """
    Helper to run the pipeline once from CLI / VS Code.
    """
    pipeline = dlt.pipeline(
        pipeline_name="trafiklab_realtime",
        destination=dlt.destinations.duckdb(str(DUCKDB_PATH)),  # ✅ correct attribute
        dataset_name="raw_trafiklab",
    )

    load_info = pipeline.run(trafiklab_realtime_source(area_id=area_id, when=when))
    print("Load info:", load_info)
    print("DuckDB path:", DUCKDB_PATH)


if __name__ == "__main__":
    # Example: run for default area id, current time
    run_once()
