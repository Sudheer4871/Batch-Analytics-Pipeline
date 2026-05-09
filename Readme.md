End-to-end batch data pipeline that pulls weather data from the Open-Meteo API, loads it into Snowflake, and transforms it using dbt to make it ready for analysis.

What this does

Fetches hourly weather data (temperature, humidity, wind speed, precipitation) from the Open-Meteo API
Loads the raw data into Snowflake
Uses dbt to clean and transform the data into daily summaries
Runs basic data quality checks at each stage


Tech Stack

Python 3.11 - ingestion and loading
Snowflake - data warehouse
dbt Core - transformations
Apache Airflow - orchestration (planned)


Project Structure
Batch-Analytics-Pipeline/
├── Ingestion/
│   ├── fetch_weather.py        # pulls data from API
│   ├── load_to_snowflake.py    # loads data into Snowflake
│   └── config.py               # environment variables
├── DBT Project/
│   ├── models/
│   │   ├── staging/
│   │   │   └── stg_weather_hourly.sql
│   │   └── marts/
│   │       └── mart_daily_weather_summary.sql
│   └── dbt_project.yml
├── Tests/
│   └── test_ingestion.py
├── requirements.txt
└── Readme.md

How to run it

Clone the repo

git clone https://github.com/Sudheer4871/Batch-Analytics-Pipeline.git
cd Batch-Analytics-Pipeline

Install dependencies

pip install -r requirements.txt

Set up a .env file with your Snowflake credentials

SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=ANALYTICS
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_WAREHOUSE=COMPUTE_WH

Run the ingestion

python Ingestion/fetch_weather.py
python Ingestion/load_to_snowflake.py

Run dbt

cd "DBT Project"
dbt run
dbt test

Pipeline flow
Raw API data comes in hourly. The staging layer cleans it up and fixes data types. The mart layer aggregates it into daily summaries with avg, min and max temperature and total rainfall. dbt tests run after each step to catch nulls and duplicates.

Status
In progress. Airflow DAG for scheduling is the next step.