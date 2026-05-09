import os
from dotenv import load_dotenv
 
load_dotenv()
 
SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "ANALYTICS"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
}
 
# Locations to pull weather data for
LOCATIONS = [
    {"name": "Hyderabad", "latitude": 17.385, "longitude": 78.4867},
    {"name": "Mumbai",    "latitude": 19.076, "longitude": 72.8777},
    {"name": "Delhi",     "latitude": 28.6139, "longitude": 77.2090},
]
 
# How many days of historical data to fetch per run
FETCH_DAYS_BACK = 7
 
# Open-Meteo API base URL
API_BASE_URL = "https://api.open-meteo.com/v1/forecast"
 
# Weather variables to fetch from the API
WEATHER_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
]
 