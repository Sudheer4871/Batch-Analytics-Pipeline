import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import logging

from config import SNOWFLAKE_CONFIG
from fetch_weather import fetch_all_locations

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

TABLE_NAME = "WEATHER_HOURLY"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    TIMESTAMP           TIMESTAMP_NTZ,
    TEMPERATURE         FLOAT,
    HUMIDITY            FLOAT,
    PRECIPITATION       FLOAT,
    WIND_SPEED          FLOAT,
    LOCATION            VARCHAR(100),
    LATITUDE            FLOAT,
    LONGITUDE           FLOAT,
    INGESTED_AT         TIMESTAMP_NTZ
);
"""


def get_connection():
    logger.info("Connecting to Snowflake...")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    logger.info("Connected.")
    return conn


def create_table_if_not_exists(conn):
    cursor = conn.cursor()
    cursor.execute(CREATE_TABLE_SQL)
    cursor.close()
    logger.info(f"Table {TABLE_NAME} is ready.")


def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Snowflake write_pandas expects uppercase column names
    df.columns = [col.upper() for col in df.columns]
    return df


def load_to_snowflake(df: pd.DataFrame):
    conn = get_connection()

    try:
        create_table_if_not_exists(conn)

        df = prepare_dataframe(df)

        success, num_chunks, num_rows, output = write_pandas(
            conn=conn,
            df=df,
            table_name=TABLE_NAME,
            database=SNOWFLAKE_CONFIG["database"],
            schema=SNOWFLAKE_CONFIG["schema"],
        )

        if success:
            logger.info(f"Successfully loaded {num_rows} rows into {TABLE_NAME} in {num_chunks} chunk(s).")
        else:
            logger.error(f"Load failed. Output: {output}")

    except Exception as e:
        logger.error(f"Error loading data to Snowflake: {e}")
        raise

    finally:
        conn.close()
        logger.info("Snowflake connection closed.")


if __name__ == "__main__":
    logger.info("Starting ingestion pipeline...")

    df = fetch_all_locations()
    load_to_snowflake(df)

    logger.info("Ingestion complete.")