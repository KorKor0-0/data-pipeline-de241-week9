import pandas as pd 
import logging
import time
from airflow.exceptions import AirflowException

def clean_taxi_data(**context):

    logger = logging.getLogger(__name__)

    file_path = context["ti"].xcom_pull(
        key="raw_path",
        task_ids="ingest_taxi_data"
    )

    if not file_path:
        raise AirflowException("No input file path found in XCom")

    logger.info(f"Loading raw data from: {file_path}")

    df = pd.read_csv(file_path, on_bad_lines="skip")

    original_count = len(df)
    logger.info(f"Original rows: {original_count}")

    # Convert types first
    df["fare_amount"] = pd.to_numeric(df["fare_amount"], errors="coerce")
    df["trip_distance"] = pd.to_numeric(df["trip_distance"], errors="coerce")

    # Drop nulls
    before = len(df)
    df = df.dropna(subset=["fare_amount", "trip_distance", "tpep_pickup_datetime"])
    logger.info(f"Removed null rows: {before - len(df)}")

    # Convert datetime
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"], errors="coerce")
    before = len(df)
    df = df.dropna(subset=["tpep_pickup_datetime"])
    logger.info(f"Removed invalid datetime rows: {before - len(df)}")

    # Fare filter
    before = len(df)
    df = df[(df["fare_amount"] > 0) & (df["fare_amount"] <= 500)]
    logger.info(f"Removed invalid fare rows: {before - len(df)}")

    # Distance filter
    before = len(df)
    df = df[(df["trip_distance"] > 0) & (df["trip_distance"] <= 100)]
    logger.info(f"Removed invalid distance rows: {before - len(df)}")

    # Coordinate filter
    coord_cols = [
        "pickup_latitude", "pickup_longitude",
        "dropoff_latitude", "dropoff_longitude"
    ]

    if all(col in df.columns for col in coord_cols):
        before = len(df)

        df = df[
            (df["pickup_latitude"].between(40.4, 41.0)) &
            (df["dropoff_latitude"].between(40.4, 41.0)) &
            (df["pickup_longitude"].between(-74.3, -73.5)) &
            (df["dropoff_longitude"].between(-74.3, -73.5))
        ]

        logger.info(f"Removed invalid coordinate rows: {before - len(df)}")
    else:
        logger.warning("Coordinate columns not found → skipping coordinate filter")

    final_count = len(df)

    if df.empty:
        raise AirflowException("All data removed after cleaning")

    logger.info(f"Final rows after cleaning: {final_count}")
    logger.info(f"Retention rate: {final_count/original_count:.2%}")

    output_path = "/tmp/nyc_taxi_cleaned.csv"
    df.to_csv(output_path, index=False)

    logger.info(f"Saved cleaned data to: {output_path}")

    context["ti"].xcom_push(key="clean_path", value=output_path)

    return output_path