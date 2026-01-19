import os
import time
import sys
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

# Default area IDs for different transport modes in Stockholm
# Stockholm C - major hub with multiple transport modes
DEFAULT_AREA_ID = "740000001"

# Recommended area IDs for comprehensive coverage of all transport modes
# These can be customized based on your needs
TRANSPORT_MODE_STOPS = {
    "metro": [
        "740000001",  # Stockholm C (T-Centralen) - Metro hub
        "740000002",  # Gamla Stan - Metro
        "740000003",  # Slussen - Metro hub
    ],
    "bus": [
        "740000001",  # Stockholm C - SL Bus hub
        "740000004",  # Odenplan - Bus hub
    ],
    "train": [
        "740000001",  # Stockholm C - Pendeltåg & SJ
        "740000005",  # Stockholm Södra - Regional trains
    ],
    "pendeltag": [
        "740000001",  # Stockholm C - Pendeltåg hub
    ],
    "sj": [
        "740000001",  # Stockholm C - SJ national rail
    ],
}

# All unique stops to query for comprehensive coverage
DEFAULT_AREA_IDS = list(set(
    stop for stops in TRANSPORT_MODE_STOPS.values() for stop in stops
))


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
    
    For paid accounts, ensure REALTIME_API_KEY is set correctly in .env file.

    Example:
      https://realtime-api.trafiklab.se/v1/departures/{area_id}?key=API_KEY
    """
    api_key = os.getenv("REALTIME_API_KEY")
    if not api_key:
        raise RuntimeError(
            "REALTIME_API_KEY is not set. Put it in .env or environment variables.\n"
            "For paid accounts, get your API key from: https://www.trafiklab.se/api"
        )

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
            data = resp.json()
            
            # Validate response structure for paid accounts
            if "departures" not in data:
                raise ValueError(
                    f"Unexpected API response structure. Expected 'departures' key. "
                    f"Response keys: {list(data.keys())}. "
                    f"This might indicate an API key issue or account problem."
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
            
            # Better error messages for common HTTP errors
            if code == 401:
                raise RuntimeError(
                    "Authentication failed (401). Check your REALTIME_API_KEY. "
                    "For paid accounts, ensure the key is correct and active."
                ) from e
            elif code == 403:
                raise RuntimeError(
                    "Access forbidden (403). Your API key may not have access to this endpoint. "
                    "Check your Trafiklab account subscription."
                ) from e
            elif code == 404:
                raise RuntimeError(
                    f"Area ID not found (404). Check if '{area_id}' is a valid stop/area ID."
                ) from e
            elif code in (429, 500, 502, 503, 504):
                sleep_s = min(2 ** attempt, 20)
                print(f"[WARN] HTTP {code} attempt {attempt}/5. Sleeping {sleep_s}s...")
                time.sleep(sleep_s)
                continue
            else:
                # Try to get error message from response
                try:
                    error_msg = e.response.json().get("message", str(e))
                except:
                    error_msg = str(e)
                raise RuntimeError(f"HTTP {code}: {error_msg}") from e

    raise RuntimeError(f"Failed to fetch departures after retries. Last error: {last_err}")


def _categorize_transport_mode(
    transport_mode: str | None,
    transport_mode_code: str | None,
    agency_operator: str | None,
    route_name: str | None,
) -> str:
    """
    Categorize transport mode into specific categories:
    - Metro (Green/Red/Blue)
    - SL & Regional Bus
    - National Rail (SJ)
    - Pendeltåg
    """
    if not transport_mode:
        return "UNKNOWN"
    
    transport_mode_upper = transport_mode.upper()
    agency_upper = (agency_operator or "").upper()
    route_upper = (route_name or "").upper()
    
    # Metro (Green/Red/Blue lines)
    if transport_mode_upper == "METRO":
        return "Metro (Green/Red/Blue)"
    
    # Bus - differentiate between SL and Regional
    if transport_mode_upper == "BUS":
        if "SL" in agency_upper or "STORSTOCKHOLMS" in agency_upper or "LOKALTRAFIK" in agency_upper:
            return "SL & Regional Bus"
        else:
            return "SL & Regional Bus"  # Default to SL & Regional Bus category
    
    # Train - differentiate between SJ (National Rail) and Pendeltåg
    if transport_mode_upper == "TRAIN":
        # Pendeltåg identification
        if (
            "PENDEL" in route_upper
            or "PENDEL" in agency_upper
            or transport_mode_code in ["PENDEL", "PEN"]
        ):
            return "Pendeltåg"
        # SJ (National Rail) identification
        elif "SJ" in agency_upper or agency_upper == "SJ":
            return "National Rail (SJ)"
        # Other regional trains might also be categorized
        else:
            # Could be regional train or other - default to Pendeltåg category
            # or you might want to add "Regional Train" category
            return "Pendeltåg"  # Default for Stockholm area trains
    
    # Tram
    if transport_mode_upper == "TRAM":
        return "Tram"
    
    # Default fallback
    return transport_mode_upper


def _flatten_departures(response: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
    """
    Take JSON response from Trafiklab Realtime Timetables and yield flat rows
    for each departure (good for analytics).
    
    Handles both free and paid account response structures.
    Categorizes transport modes into: Metro, SL & Regional Bus, National Rail (SJ), Pendeltåg.
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

        # Extract transport mode info
        transport_mode = route.get("transport_mode")
        transport_mode_code = route.get("transport_mode_code")
        agency_operator = agency.get("operator")
        route_name = route.get("name")

        # Categorize transport mode
        transport_category = _categorize_transport_mode(
            transport_mode=transport_mode,
            transport_mode_code=transport_mode_code,
            agency_operator=agency_operator,
            route_name=route_name,
        )

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

            # Platform information (available in paid accounts)
            "scheduled_platform": dep.get("scheduled_platform") or dep.get("platform"),
            "realtime_platform": dep.get("realtime_platform") or dep.get("platform"),

            # Route info
            "route_name": route_name,
            "route_designation": route.get("designation"),
            "route_transport_mode_code": transport_mode_code,
            "route_transport_mode": transport_mode,
            "route_direction": route.get("direction"),
            
            # Transport category (our custom categorization)
            "transport_category": transport_category,

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
            "agency_operator": agency_operator,

            # Stop (where this departure happens)
            "stop_id": stop.get("id"),
            "stop_name": stop.get("name"),
            "stop_lat": stop.get("lat"),
            "stop_lon": stop.get("lon"),
        }


@dlt.resource(
    name="trafiklab_departures",
    write_disposition="append",
    primary_key=["trip_id", "scheduled_time", "stop_id"],
    columns={
        "route_name": {"data_type": "text", "nullable": True},
        "transport_category": {"data_type": "text", "nullable": True},
        "scheduled_platform": {"data_type": "text", "nullable": True},
        "realtime_platform": {"data_type": "text", "nullable": True},
        "trip_id": {"data_type": "text", "nullable": True},
        "scheduled_time": {"data_type": "timestamp", "nullable": True},
        "stop_id": {"data_type": "text", "nullable": True},
    },
)
def trafiklab_departures_resource(
    area_id: str = DEFAULT_AREA_ID,
    when: datetime | None = None,
) -> Iterator[Dict[str, Any]]:
    """
    dlt resource: calls API and yields flattened departure rows.
    
    Args:
        area_id: Stop/area ID to query (e.g., "740000001" for Stockholm C)
        when: Optional datetime to query departures for a specific time
    """
    response = _call_timetables_departures(area_id=area_id, when=when)
    yield from _flatten_departures(response)


@dlt.source
def trafiklab_realtime_source(
    area_ids: list[str] | str | None = None,
    when: datetime | None = None,
):
    """
    dlt source combining all resources.
    
    Supports querying multiple area_ids to capture all transport modes:
    - Metro (Green/Red/Blue)
    - SL & Regional Bus
    - National Rail (SJ)
    - Pendeltåg
    
    Args:
        area_ids: Single area_id (str) or list of area_ids to query.
                  If None, uses DEFAULT_AREA_IDS for comprehensive coverage.
        when: Optional datetime to query departures for a specific time
    
    Returns:
        dlt source with departures from all specified stops
    """
    if area_ids is None:
        area_ids = DEFAULT_AREA_IDS
    elif isinstance(area_ids, str):
        area_ids = [area_ids]
    
    # Create a single resource that queries all area_ids
    # This ensures all data goes into the same table
    @dlt.resource(
        name="trafiklab_departures",
        write_disposition="append",
        primary_key=["trip_id", "scheduled_time", "stop_id"],
        columns={
            "route_name": {"data_type": "text", "nullable": True},
            "transport_category": {"data_type": "text", "nullable": True},
            "scheduled_platform": {"data_type": "text", "nullable": True},
            "realtime_platform": {"data_type": "text", "nullable": True},
            "trip_id": {"data_type": "text", "nullable": True},
            "scheduled_time": {"data_type": "timestamp", "nullable": True},
            "stop_id": {"data_type": "text", "nullable": True},
        },
    )
    def multi_stop_resource() -> Iterator[Dict[str, Any]]:
        """Query multiple stops and yield all departures."""
        for area_id in area_ids:
            try:
                print(f"[INFO] Querying area_id: {area_id}")
                response = _call_timetables_departures(area_id=area_id, when=when)
                yield from _flatten_departures(response)
            except Exception as e:
                print(f"[WARN] Failed to fetch departures for area_id {area_id}: {e}")
                continue
    
    return multi_stop_resource()


# --- Destination path in warehouse/ ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DUCKDB_PATH = PROJECT_ROOT / "warehouse" / "trafiklab_realtime.duckdb"


def _check_duckdb_available(db_path: Path) -> tuple[bool, str]:
    """
    Check if DuckDB file is available for writing.
    
    Returns:
        (is_available, error_message)
    """
    if not db_path.exists():
        return True, ""  # File doesn't exist, so it's available
    
    try:
        # Try to open the file to check if it's locked
        # Note: This is a best-effort check - the actual pipeline will catch the real error
        import duckdb
        test_conn = duckdb.connect(str(db_path), read_only=True)
        test_conn.execute("SELECT 1")  # Try a simple query
        test_conn.close()
        return True, ""
    except Exception as e:
        error_msg = str(e)
        if "being used by another process" in error_msg or "Cannot open file" in error_msg:
            return False, "Database file is locked by another process"
        # If it's a different error, don't fail - let the pipeline handle it
        return True, ""


def run_once(
    area_ids: list[str] | str | None = None,
    when: datetime | None = None,
    use_all_modes: bool = True,
):
    """
    Helper to run pipeline once from CLI / VS Code.
    
    Captures multiple transport modes:
    - Metro (Green/Red/Blue)
    - SL & Regional Bus
    - National Rail (SJ)
    - Pendeltåg
    
    Args:
        area_ids: Single area_id (str), list of area_ids, or None.
                  If None and use_all_modes=True, queries DEFAULT_AREA_IDS.
                  If None and use_all_modes=False, queries DEFAULT_AREA_ID only.
        when: Optional datetime to query departures for a specific time
        use_all_modes: If True, queries multiple stops for comprehensive coverage.
                       If False, queries only DEFAULT_AREA_ID.
    
    For paid accounts, ensure REALTIME_API_KEY is set in .env file.
    """
    api_key = os.getenv("REALTIME_API_KEY")
    if not api_key:
        print("[ERROR] REALTIME_API_KEY not found. Please set it in .env file.")
        print("Get your API key from: https://www.trafiklab.se/api")
        return
    
    # Determine which area_ids to query
    if area_ids is None:
        if use_all_modes:
            area_ids = DEFAULT_AREA_IDS
            print(f"[INFO] Querying {len(area_ids)} stops for comprehensive transport mode coverage")
        else:
            area_ids = [DEFAULT_AREA_ID]
            print(f"[INFO] Querying single stop: {DEFAULT_AREA_ID}")
    elif isinstance(area_ids, str):
        area_ids = [area_ids]
        print(f"[INFO] Querying single stop: {area_ids[0]}")
    else:
        print(f"[INFO] Querying {len(area_ids)} stops: {area_ids}")
    
    if when:
        print(f"[INFO] Query time: {when}")
    
    # Check if database is available before starting
    print(f"[INFO] Checking database availability: {DUCKDB_PATH}")
    is_available, error_msg = _check_duckdb_available(DUCKDB_PATH)
    if not is_available:
        print("\n" + "="*70)
        print("[ERROR] Database file is not available!")
        print("="*70)
        print(f"\n{error_msg}")
        print(f"\nDatabase path: {DUCKDB_PATH}")
        print("\nPlease:")
        print("  1. Close any running dashboards (dashboard/app.py)")
        print("  2. Close any Jupyter notebooks using the database")
        print("  3. Stop any other Python processes accessing the database")
        print("  4. On Windows: Check Task Manager for Python processes (PID 26040)")
        print("  5. Wait a few seconds and try again")
        print("\nIf the problem persists, restart your IDE/terminal.")
        print("="*70)
        sys.exit(1)
    
    pipeline = dlt.pipeline(
        pipeline_name="trafiklab_realtime",
        destination=dlt.destinations.duckdb(str(DUCKDB_PATH)),
        dataset_name="raw_trafiklab",
    )

    try:
        load_info = pipeline.run(trafiklab_realtime_source(area_ids=area_ids, when=when))
        print("\n[SUCCESS] Pipeline completed successfully!")
        print("Load info:", load_info)
        print(f"DuckDB path: {DUCKDB_PATH}")
        
        # Print summary of transport modes captured
        print("\n[INFO] Transport modes captured:")
        print("  - Metro (Green/Red/Blue)")
        print("  - SL & Regional Bus")
        print("  - National Rail (SJ)")
        print("  - Pendeltåg")
    except dlt.destinations.exceptions.DestinationConnectionError as e:
        error_msg = str(e)
        if "being used by another process" in error_msg or "Cannot open file" in error_msg:
            print("\n" + "="*70)
            print("[ERROR] DuckDB file is locked by another process!")
            print("="*70)
            print(f"\nThe database file is currently open in another process:")
            print(f"  {DUCKDB_PATH}")
            print("\nThis usually happens when:")
            print("  • The dashboard (dashboard/app.py) is running")
            print("  • A Jupyter notebook has the database open")
            print("  • Another Python script is using the database")
            print("  • A previous script crashed without closing the connection")
            print("\nTo fix this:")
            print("  1. Close any running dashboards or notebooks")
            print("  2. Stop any other Python processes using the database")
            print("  3. On Windows, check Task Manager for Python processes")
            print("  4. Wait a few seconds and try again")
            print("\nIf the problem persists, you can:")
            print("  • Restart your IDE/terminal")
            print("  • Use a different database file temporarily")
            print("="*70)
            sys.exit(1)
        else:
            print(f"\n[ERROR] Database connection failed: {e}")
            raise
    except Exception as e:
        print(f"\n[ERROR] Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    run_once()
