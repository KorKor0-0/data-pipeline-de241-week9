import pandas as pd
import logging
from sqlalchemy import create_engine
from airflow.models import Variable
from airflow.exceptions import AirflowException

def load_taxi_data(**context):

    logger = logging.getLogger(__name__)

    #Pull transformed data path from XCom
    transformed_path = context["ti"].xcom_pull(
        task_ids="transform_taxi_data",
        key="transform_path"
    )

    if not transformed_path:
        raise AirflowException("No transform_path found in XCom")

    logger.info(f"Loading transformed data from: {transformed_path}")

    #Load CSV
    df = pd.read_csv(transformed_path)

    if df.empty:
        raise AirflowException("No data to load")

    logger.info(f"Rows to load: {len(df)}")

    #Connect to MySQL (Airflow Variables)
    engine = create_engine(
        f"mysql+pymysql://{Variable.get('MYSQL_USER')}:"
        f"{Variable.get('MYSQL_PASS')}@{Variable.get('MYSQL_HOST')}/"
        f"{Variable.get('MYSQL_DB')}"
    )

    logger.info("Connected to MySQL")

    #Create dimension: dim_time
    dim_time = df[[
        "pickup_hour",
        "pickup_day_of_week",
        "is_weekend"
    ]].drop_duplicates().reset_index(drop=True)

    dim_time["time_id"] = dim_time.index + 1

    dim_time.to_sql(
        "dim_time",
        engine,
        if_exists="replace",  # replace for demo
        index=False
    )

    logger.info(f"dim_time: {len(dim_time)} rows")

    #Create dimension: dim_payment
    dim_payment = df[["payment_type"]].drop_duplicates().reset_index(drop=True)

    dim_payment["payment_id"] = dim_payment.index + 1

    dim_payment.to_sql(
        "dim_payment",
        engine,
        if_exists="replace",  # replace for demo
        index=False
    )

    logger.info(f"dim_payment: {len(dim_payment)} rows")

    #Create fact table (join dimensions)
    fact_trips = df.merge(
        dim_time,
        on=["pickup_hour", "pickup_day_of_week", "is_weekend"]
    )

    fact_trips = fact_trips.merge(
        dim_payment,
        on="payment_type"
    )

    fact_trips = fact_trips[[
        "time_id",
        "payment_id",
        "fare_amount",
        "trip_distance",
        "trip_duration_minutes",
        "speed_mph",
        "fare_per_mile",
        "passenger_count"
    ]]

    fact_trips.to_sql(
        "fact_trips",
        engine,
        if_exists="replace",  # replace for demo
        index=False,
        chunksize=1000
    )

    logger.info(f"fact_trips: {len(fact_trips)} rows")

    return "Load completed successfully"