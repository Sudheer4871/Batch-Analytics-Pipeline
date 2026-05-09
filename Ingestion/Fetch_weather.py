import requests
import pandas as pd
from datetime import datetime, timedelta
import logging

from config import LOCATIONS, FETCH_DAYS_BACK, API_BASE_URL, WEATHER_VARIABLES

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def build_api_params(location: dict, start_date: str, end_date: str) -> dict:
    return {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "hourly": ",".join(WEATHER_VARIABLES),
        "start_date": start_date,
        "end_date": end_date,
        "timezone": "Asia/Kolkata",
    }


def fetch_weather_for_location(location: dict, start_date: str, end_date: str) -> pd.DataFrame:
    params = build_api_params(location, start_date, end_date)

    logger.info(f"Fetching weather data for {location['name']} from {start_date} to {end_date}")

    response = requests.get(API_BASE_URL, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    if "hourly" not in data:
        raise ValueError(f"Unexpected API response for {location['name']}: {data}")

    hourly = data["hourly"]

    df = pd.DataFrame({
        "timestamp":    hourly["time"],
        "temperature":  hourly["temperature_2m"],
        "humidity":     hourly["relative_humidity_2m"],
        "precipitation":hourly["precipitation"],
        "wind_speed":   hourly["wind_speed_10m"],
    })

    df["location"] = location["name"]
    df["latitude"]  = location["latitude"]
    df["longitude"] = location["longitude"]
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["ingested_at"] = datetime.utcnow()

    logger.info(f"Fetched {len(df)} rows for {location['name']}")
    return df


def validate_dataframe(df: pd.DataFrame, location_name: str):
    if df.empty:
        raise ValueError(f"No data returned for {location_name}")

    required_columns = ["timestamp", "temperature", "humidity", "precipitation", "wind_speed"]
    for col in required_columns:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            logger.warning(f"{location_name}: {null_count} nulls found in column '{col}'")

    logger.info(f"Validation passed for {location_name}")


def fetch_all_locations() -> pd.DataFrame:
    end_date   = datetime.today().strftime("%Y-%m-%d")
    start_date = (datetime.today() - timedelta(days=FETCH_DAYS_BACK)).strftime("%Y-%m-%d")

    all_data = []

    for location in LOCATIONS:
        try:
            df = fetch_weather_for_location(location, start_date, end_date)
            validate_dataframe(df, location["name"])
            all_data.append(df)
        except Exception as e:
            logger.error(f"Failed to fetch data for {location['name']}: {e}")

    if not all_data:
        raise RuntimeError("No data fetched for any location. Aborting.")

    combined = pd.concat(all_data, ignore_index=True)
    logger.info(f"Total rows fetched across all locations: {len(combined)}")
    return combined


if __name__ == "__main__":
    df = fetch_all_locations()
    print(df.head())
    print(f"\nShape: {df.shape}")
    print(f"\nColumns: {list(df.columns)}")