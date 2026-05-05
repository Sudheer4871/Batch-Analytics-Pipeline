import requests
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://api.open-meteo.com/v1/forecast"

# cities we care about - can expand this later
LOCATIONS = [
    {"name": "London",   "lat": 51.5074,  "lon": -0.1278},
    {"name": "New York", "lat": 40.7128,  "lon": -74.0060},
    {"name": "Tokyo",    "lat": 35.6895,  "lon": 139.6917},
    {"name": "Sydney",   "lat": -33.8688, "lon": 151.2093},
]


def fetch_weather(lat: float, lon: float, days_back: int = 7) -> dict:
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=days_back)

    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max",
        "start_date": str(start_date),
        "end_date": str(end_date),
        "timezone": "UTC",
    }

    resp = requests.get(BASE_URL, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def extract_all() -> list[dict]:
    records = []

    for loc in LOCATIONS:
        logger.info(f"Fetching weather for {loc['name']}")
        try:
            raw = fetch_weather(loc["lat"], loc["lon"])
            daily = raw.get("daily", {})
            dates = daily.get("time", [])

            for i, date in enumerate(dates):
                records.append({
                    "city":            loc["name"],
                    "date":            date,
                    "temp_max_c":      daily["temperature_2m_max"][i],
                    "temp_min_c":      daily["temperature_2m_min"][i],
                    "precipitation_mm": daily["precipitation_sum"][i],
                    "windspeed_max_kmh": daily["windspeed_10m_max"][i],
                    "loaded_at":       datetime.utcnow().isoformat(),
                })

        except requests.RequestException as e:
            # TODO: add retry logic here
            logger.error(f"Failed to fetch {loc['name']}: {e}")
            continue

    logger.info(f"Extracted {len(records)} records total")
    return records