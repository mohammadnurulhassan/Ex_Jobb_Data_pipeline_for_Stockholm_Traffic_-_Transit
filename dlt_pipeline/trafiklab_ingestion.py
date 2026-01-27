import os
import time
from pathlib import Path
from datetime import datetime
from typing import Iterator, Dict, Any, Optional

import dlt
import requests
from dotenv import load_dotenv


# -----------------------------
# Paths + env
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]   # repo root (one level above dlt_pipeline)
WAREHOUSE_DIR = PROJECT_ROOT / "warehouse"
WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

DUCKDB_PATH = WAREHOUSE_DIR / "trafiklab_realtime.duckdb"

# Load .env from project root (more reliable than plain load_dotenv())
load_dotenv(PROJECT_ROOT / ".env")


class TrafiklabClient:
    """Client for SL Trafiklab Real-time API v4"""

    BASE_URL = "https://realtime-api.trafiklab.se/v1"


    def __init__(self, api_key: str):
        self.api_key = api_key

    def get_departures(self, site_id: int, time_window: int = 60) -> Optional[Dict[str, Any]]:
        params = {"key": self.api_key, "siteid": site_id, "timewindow": time_window}

        try:
            response = requests.get(self.BASE_URL, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()

            # API-level error
            if data.get("StatusCode") not in (0, None):
                print(f"API Error for site {site_id}: {data.get('Message')}")
                return None

            return data

        except requests.exceptions.RequestException as e:
            print(f"Request failed for site {site_id}: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error for site {site_id}: {e}")
            return None


# Major Stockholm stations and stops
STOCKHOLM_STATIONS = {
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


@dlt.resource(name="realtime_departures", write_disposition="append")
def realtime_departures(api_key: str) -> Iterator[Dict[str, Any]]:
    """
    DLT resource to fetch real-time departure data from all major Stockholm stations.
    Raises if it produces 0 rows total (so you don't get "successful" empty loads).
    """
    client = TrafiklabClient(api_key)

    total_yielded = 0
    total_failed = 0

    for site_id, station_name in STOCKHOLM_STATIONS.items():
        print(f"Fetching data for {station_name} (ID: {site_id})...")

        data = client.get_departures(site_id)

        if not data or "ResponseData" not in data:
            total_failed += 1
            continue

        response_data = data["ResponseData"]

        for mode in ["Buses", "Metros", "Trains", "Trams", "Ships"]:
            for vehicle in response_data.get(mode, []):
                total_yielded += 1
                yield {
                    "station_id": site_id,
                    "station_name": station_name,
                    "line_number": vehicle.get("LineNumber"),
                    "destination": vehicle.get("Destination"),
                    "display_time": vehicle.get("DisplayTime"),
                    "expected_datetime": vehicle.get("ExpectedDateTime"),
                    "timetabled_datetime": vehicle.get("TimeTabledDateTime"),
                    "journey_direction": vehicle.get("JourneyDirection"),
                    "stop_area_name": vehicle.get("StopAreaName"),
                    "stop_area_number": vehicle.get("StopAreaNumber"),
                    "stop_point_number": vehicle.get("StopPointNumber"),
                    "stop_point_designation": vehicle.get("StopPointDesignation"),
                    "transport_mode": vehicle.get("TransportMode"),
                    "group_of_line": vehicle.get("GroupOfLine"),
                    "deviations": str(vehicle.get("Deviations", [])),
                    "ingestion_timestamp": datetime.now().isoformat(),
                }

        time.sleep(0.5)

    # IMPORTANT: avoid silent "success" with 0 rows
    if total_yielded == 0:
        raise RuntimeError(
            f"No rows produced. Failed stations: {total_failed}/{len(STOCKHOLM_STATIONS)}. "
            f"Likely DNS/network issue or invalid endpoint/key."
        )


@dlt.source
def stockholm_traffic_source(api_key: str):
    return realtime_departures(api_key)


def run_ingestion_pipeline():
    api_key = os.getenv("TRAFIKLAB_API_KEY")
    if not api_key:
        raise ValueError(
            "Missing TRAFIKLAB_API_KEY. Add it to your .env in project root, e.g.\n"
            "TRAFIKLAB_API_KEY=xxxxx"
        )

    # Force DuckDB file to be in /warehouse
    pipeline = dlt.pipeline(
        pipeline_name="stockholm_traffic",
        destination=dlt.destinations.duckdb(str(DUCKDB_PATH)),
        dataset_name="raw_traffic",
        dev_mode=False,
    )

    print("Starting ingestion pipeline...")
    load_info = pipeline.run(stockholm_traffic_source(api_key))

    print("\nPipeline completed successfully!")
    print(load_info)
    print(f"\n✅ DuckDB stored at: {DUCKDB_PATH}")

    return pipeline


if __name__ == "__main__":
    run_ingestion_pipeline()

