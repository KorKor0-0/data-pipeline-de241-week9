import requests
import pandas as pd
import logging
import time
from airflow.exceptions import AirflowException

def ingest_taxi_data(**context):

    logger = logging.getLogger(__name__)

    url = "https://data.cityofnewyork.us/api/views/t29m-gskq/rows.csv"
    output_path = "/tmp/nyc_taxi_raw.csv"
    max_rows = 10000

    for attempt in range(3):
        try:
            logger.info(f"Attempt {attempt + 1}: Streaming download")

            with requests.get(url, stream=True, timeout=60) as response:
                response.raise_for_status()

                with open(output_path, "wb") as f:
                    lines_written = 0

                    for line in response.iter_lines():
                        if line:
                            f.write(line + b"\n")
                            lines_written += 1

                            if lines_written >= max_rows:
                                logger.info(f"Reached {max_rows} rows")
                                break

            logger.info("Download finished")
            break

        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt + 1} failed: {e}")
            if attempt == 2:
                raise AirflowException(f"Download failed: {e}")
            time.sleep(5 * (attempt + 1))

    # validate
    df = pd.read_csv(output_path, usecols=[0], on_bad_lines='skip')

    if df.empty:
        raise AirflowException("Downloaded file is empty")

    row_count = len(df)

    if row_count < 1000:
        raise AirflowException(f"Too few rows: {row_count}")

    logger.info(f"SUCCESS: {row_count} rows")

    context["ti"].xcom_push(key="raw_path", value=output_path)

    return output_path