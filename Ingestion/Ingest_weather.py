"""
Weather Data Ingestion Pipeline
================================
Source  : Open-Meteo Historical Weather API (free, no API key required)
Target  : Snowflake  —  raw schema  →  RAW.WEATHER_DAILY
Schedule: Daily batch (run via Airflow or cron)

What it does:
  1. Fetches daily weather data for multiple cities
  2. Validates and cleans the response
  3. Loads into Snowflake raw table using MERGE (idempotent)
"""

import os
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

API_BASE_URL = "https://api.open-meteo.com/v1/forecast"

# Cities to ingest — latitude / longitude / timezone
CITIES = [
    {"name": "Hyderabad", "latitude": 17.385,  "longitude": 78.4867, "timezone": "Asia/Kolkata"},
    {"name": "Mumbai",    "latitude": 19.0760,  "longitude": 72.8777, "timezone": "Asia/Kolkata"},
    {"name": "Delhi",     "latitude": 28.6139,  "longitude": 77.2090, "timezone": "Asia/Kolkata"},
    {"name": "London",    "latitude": 51.5074,  "longitude": -0.1278, "timezone": "Europe/London"},
    {"name": "New York",  "latitude": 40.7128,  "longitude": -74.0060,"timezone": "America/New_York"},
]

# Daily variables to pull from the API
DAILY_VARIABLES = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "windspeed_10m_max",
    "weathercode",
]

SNOWFLAKE_CONFIG = {
    "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
    "user":      os.getenv("SNOWFLAKE_USER"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "database":  os.getenv("SNOWFLAKE_DATABASE",  "ANALYTICS"),
    "schema":    os.getenv("SNOWFLAKE_SCHEMA",    "RAW"),
    "role":      os.getenv("SNOWFLAKE_ROLE",      "SYSADMIN"),
}

# ── API Fetch ──────────────────────────────────────────────────────────────────

def fetch_weather(city: dict, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Call Open-Meteo API for one city and return a tidy DataFrame.
    """
    params = {
        "latitude":        city["latitude"],
        "longitude":       city["longitude"],
        "timezone":        city["timezone"],
        "start_date":      start_date,
        "end_date":        end_date,
        "daily":           ",".join(DAILY_VARIABLES),
    }

    logger.info(f"Fetching weather for {city['name']} ({start_date} → {end_date})")

    response = requests.get(API_BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    daily = data.get("daily", {})
    if not daily:
        raise ValueError(f"Empty daily payload for {city['name']}")

    df = pd.DataFrame(daily)
    df.rename(columns={"time": "date"}, inplace=True)
    df["city"]      = city["name"]
    df["latitude"]  = city["latitude"]
    df["longitude"] = city["longitude"]
    df["timezone"]  = city["timezone"]
    df["ingested_at"] = datetime.utcnow()

    return df

# ── Validation ─────────────────────────────────────────────────────────────────

def validate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Basic data-quality checks before loading.
    Raises ValueError if critical checks fail.
    """
    original_count = len(df)

    # Drop rows where the date is null
    df = df.dropna(subset=["date"])

    # Temperature sanity check (°C)
    invalid_temp = df[
        (df["temperature_2m_max"] > 60) | (df["temperature_2m_min"] < -90)
    ]
    if not invalid_temp.empty:
        logger.warning(f"Dropping {len(invalid_temp)} rows with impossible temperatures")
        df = df.drop(invalid_temp.index)

    # Precipitation cannot be negative
    df["precipitation_sum"] = df["precipitation_sum"].clip(lower=0)

    dropped = original_count - len(df)
    if dropped:
        logger.warning(f"Validation dropped {dropped} rows total")

    logger.info(f"Validation passed — {len(df)} rows ready to load")
    return df

# ── Snowflake Load ─────────────────────────────────────────────────────────────

def get_snowflake_connection():
    return connect(**SNOWFLAKE_CONFIG)

def ensure_table(cursor):
    """
    Create the raw table if it does not exist.
    Uses DATE + CITY as the natural key for MERGE deduplication.
    """
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS RAW.WEATHER_DAILY (
            DATE                  DATE,
            CITY                  VARCHAR(100),
            LATITUDE              FLOAT,
            LONGITUDE             FLOAT,
            TIMEZONE              VARCHAR(100),
            TEMPERATURE_2M_MAX    FLOAT,
            TEMPERATURE_2M_MIN    FLOAT,
            PRECIPITATION_SUM     FLOAT,
            WINDSPEED_10M_MAX     FLOAT,
            WEATHERCODE           INTEGER,
            INGESTED_AT           TIMESTAMP_NTZ,
            PRIMARY KEY (DATE, CITY)
        )
    """)
    logger.info("Table RAW.WEATHER_DAILY verified / created")

def load_to_snowflake(df: pd.DataFrame, conn):
    """
    Write DataFrame to a temp stage table, then MERGE into the target.
    This makes the load idempotent — safe to re-run on failure.
    """
    df.columns = [c.upper() for c in df.columns]

    cursor = conn.cursor()
    ensure_table(cursor)

    # Stage in a temp table
    cursor.execute("CREATE TEMP TABLE IF NOT EXISTS WEATHER_STAGE LIKE RAW.WEATHER_DAILY")
    cursor.execute("TRUNCATE TABLE WEATHER_STAGE")

    write_pandas(conn, df, "WEATHER_STAGE", auto_create_table=False)
    logger.info(f"Staged {len(df)} rows into WEATHER_STAGE")

    # MERGE — insert new, update existing
    cursor.execute("""
        MERGE INTO RAW.WEATHER_DAILY tgt
        USING WEATHER_STAGE src
            ON tgt.DATE = src.DATE AND tgt.CITY = src.CITY
        WHEN MATCHED THEN UPDATE SET
            tgt.TEMPERATURE_2M_MAX  = src.TEMPERATURE_2M_MAX,
            tgt.TEMPERATURE_2M_MIN  = src.TEMPERATURE_2M_MIN,
            tgt.PRECIPITATION_SUM   = src.PRECIPITATION_SUM,
            tgt.WINDSPEED_10M_MAX   = src.WINDSPEED_10M_MAX,
            tgt.WEATHERCODE         = src.WEATHERCODE,
            tgt.INGESTED_AT         = src.INGESTED_AT
        WHEN NOT MATCHED THEN INSERT (
            DATE, CITY, LATITUDE, LONGITUDE, TIMEZONE,
            TEMPERATURE_2M_MAX, TEMPERATURE_2M_MIN,
            PRECIPITATION_SUM, WINDSPEED_10M_MAX,
            WEATHERCODE, INGESTED_AT
        )
        VALUES (
            src.DATE, src.CITY, src.LATITUDE, src.LONGITUDE, src.TIMEZONE,
            src.TEMPERATURE_2M_MAX, src.TEMPERATURE_2M_MIN,
            src.PRECIPITATION_SUM, src.WINDSPEED_10M_MAX,
            src.WEATHERCODE, src.INGESTED_AT
        )
    """)
    logger.info("MERGE into RAW.WEATHER_DAILY complete")
    cursor.close()

# ── Orchestration ──────────────────────────────────────────────────────────────

def run(days_back: int = 7):
    """
    Main entry point.
    Ingests the last `days_back` days of weather data for all cities.
    """
    end_date   = datetime.utcnow().date()
    start_date = end_date - timedelta(days=days_back)

    start_str = start_date.strftime("%Y-%m-%d")
    end_str   = end_date.strftime("%Y-%m-%d")

    all_frames = []

    for city in CITIES:
        try:
            df = fetch_weather(city, start_str, end_str)
            df = validate(df)
            all_frames.append(df)
        except Exception as e:
            logger.error(f"Failed to fetch {city['name']}: {e}")

    if not all_frames:
        raise RuntimeError("No data fetched for any city — aborting load")

    combined = pd.concat(all_frames, ignore_index=True)
    logger.info(f"Total rows to load: {len(combined)}")

    conn = get_snowflake_connection()
    try:
        load_to_snowflake(combined, conn)
    finally:
        conn.close()
        logger.info("Snowflake connection closed")

    logger.info("✅ Ingestion pipeline complete")


if __name__ == "__main__":
    run(days_back=7)