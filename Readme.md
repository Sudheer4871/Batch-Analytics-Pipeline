# Batch Analytics Pipeline

End-to-end batch data pipeline that ingests daily weather data from the
Open-Meteo API, transforms it with dbt, and loads it into Snowflake
for analytics.


## Architecture

Open-Meteo API  →  Python Ingestion  →  Snowflake RAW
                                              │
                                    dbt staging (views)
                                              │
                                    dbt marts (tables)
                                              │
                                  Analytics / Dashboards
## Tech Stack
LayerToolIngestionPython 3.11, Requests, Pandas
WarehouseS nowflake
Transformation dbt (staging → marts pattern)
Data Quality dbt schema tests + custom Python tests
OrchestrationApache Airflow (planned)

## Pipeline Flow
Raw API → Python ingestion → Snowflake raw schema
→ dbt staging models → dbt mart models → Analytics

## Data Quality 
- dbt schema tests on every model
- Not-null, unique, accepted-values checks
- Row count validation between layers

## Setup
pip install -r requirements.txt
python ingestion/ingest.py
dbt run

## Status
In Progress