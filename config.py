import os
from dotenv import load_dotenv

load_dotenv()

# API Configuration
TRAFIKLAB_API_KEY = os.getenv("REALTIME_API_KEY")
TRAFIKLAB_API_URL = "https://api.sl.se/api2/realtimedeparturesV4.json"

# Database Configuration
DUCKDB_DATABASE = "stockholm_traffic.duckdb"

# Stockholm Stations to Monitor
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
    1051: "Hötorget"
}

# Data Collection Settings
COLLECTION_INTERVAL_MINUTES = 5
API_TIMEOUT_SECONDS = 15
RATE_LIMIT_DELAY_SECONDS = 0.5