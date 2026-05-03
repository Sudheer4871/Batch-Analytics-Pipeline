# Batch Analytics Pipeline

End-to-end data pipeline ingesting [data source] data,
transforming with dbt and loading into Snowflake for analytics.

## Architecture


## Tech Stack
- **Ingestion:** Python, Requests
- **Transformation:** dbt (staging → marts pattern)
- **Warehouse:** Snowflake
- **Orchestration:** Apache Airflow (planned)
- **Data Quality:** dbt tests + custom checks

## Pipeline Flow
Raw API → Python ingestion → Snowflake raw schema
→ dbt staging models → dbt mart models → Analytics

## Data Quality 
- dbt schema tests on every model
- Not-null, unique, accepted-values checks
- Row count validation between layers

## Setup
[instructions to run locally]

## Status
In Progress