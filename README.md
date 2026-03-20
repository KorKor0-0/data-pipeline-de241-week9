# data-pipeline-de241-week9
End-to-end ETL pipeline using Airflow for NYC Taxi data
# NYC Taxi ETL Pipeline (DE241)

End-to-end ETL pipeline built with Apache Airflow to process NYC Taxi data, including ingestion, cleaning, transformation, and loading into a MySQL database.

---

## 📌 Overview

This project demonstrates a complete data engineering workflow:

* Extract raw NYC taxi data from source
* Clean and validate the dataset
* Transform data and create new features
* Load structured data into MySQL

---

## ⚙️ Tech Stack

* Python
* Apache Airflow
* Pandas
* MySQL / SQLAlchemy

---

## 📂 Project Structure

```id="p9f0d8"
dags/
 ├── ingest_taxi_data.py
 ├── clean_taxi_data.py
 ├── transform_taxi_data.py
 ├── load_taxi_data.py
 └── taxi_etl_pipeline_dag.py
```

---

## 🔄 Pipeline Workflow

```id="mhy4w5"
Ingest → Clean → Transform → Load
```

### Task Summary

* **Ingest:** Load raw data from external source
* **Clean:** Handle missing values and format columns
* **Transform:** Create features (duration, speed, fare per mile)
* **Load:** Store data in MySQL as dimension and fact tables

---

## 🗄️ Output

* Cleaned data: `/tmp/nyc_taxi_cleaned.csv`
* Transformed data: `/tmp/nyc_taxi_transformed.csv`

### Database Tables

* `dim_time`
* `dim_payment`
* `fact_trips`

---

## 🎯 Key Learning

* Build end-to-end ETL pipelines
* Use Airflow for orchestration
* Apply data modeling (fact & dimension)
* Handle real-world data processing

---

## 👤 Author

Chanaporn Saikaew
