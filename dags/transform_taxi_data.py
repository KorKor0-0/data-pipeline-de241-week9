import pandas as pd
import logging
import numpy as np
import time
from airflow.exceptions import AirflowException


def transform_taxi_data(**context):
    logger = logging.getLogger(__name__)

    ti = context["ti"]
    clean_path = ti.xcom_pull(
        key="clean_path",
        task_ids="clean_taxi_data"
    )

    if not clean_path:
        raise AirflowException("No clean_path found in XCom")

    df = pd.read_csv(clean_path)

    if df.empty:
        raise AirflowException("Input data is empty before transform")

    logger.info(f"Rows before transform: {len(df)}")

    # dtype safety
    df["trip_distance"] = pd.to_numeric(df["trip_distance"], errors="coerce")
    df["fare_amount"] = pd.to_numeric(df["fare_amount"], errors="coerce")

    # datetime
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"], errors="coerce")
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"], errors="coerce")

    df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime"])

    # duration
    df["trip_duration_minutes"] = (
        (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
        .dt.total_seconds() / 60
    )

    # filter invalid
    df = df[
        (df["trip_duration_minutes"] > 0) &
        (df["trip_distance"] > 0)
    ]

    # features
    df["speed_mph"] = df["trip_distance"] / (df["trip_duration_minutes"] / 60)
    df["fare_per_mile"] = df["fare_amount"] / df["trip_distance"]
    df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
    df["pickup_day_of_week"] = df["tpep_pickup_datetime"].dt.dayofweek
    df["is_weekend"] = df["pickup_day_of_week"] >= 5

    # clean inf/nan
    df = df.replace([np.inf, -np.inf], np.nan).dropna()

    # outlier filter
    df = df[
        (df["speed_mph"] > 0) &
        (df["speed_mph"] <= 80) &
        (df["trip_duration_minutes"] >= 1)
    ]

    logger.info(f"Rows after transform: {len(df)}")

    if df.empty:
        raise AirflowException("All data removed after transform")

    output_path = "/tmp/nyc_taxi_transformed.csv"
    df.to_csv(output_path, index=False)

    logger.info(df[[
        "trip_duration_minutes", "speed_mph", "fare_per_mile"
    ]].describe().to_string())

    ti.xcom_push(key="transform_path", value=output_path)

    return output_path