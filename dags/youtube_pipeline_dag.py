
# dags/yt_etl_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import pandas as pd
from etl_pipeline import extract_pipeline, clean_yt_data, load_to_csv, load_to_bigquery

with open("/opt/airflow/config/etl_config.json") as f:
    CONFIG = json.load(f)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "yt_etl_pipeline",
    default_args=default_args,
    description="YouTube ETL pipeline with Airflow",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ETL", "YouTube"],
) as dag:

    def task_extract(**context):
        df = extract_pipeline(CONFIG["user_name"])
        context["ti"].xcom_push(key="raw_df", value=df.to_json())  # store raw as JSON

    def task_transform(**context):
        raw_df = pd.read_json(context["ti"].xcom_pull(task_ids="extract_task", key="raw_df"))
        df_clean = clean_yt_data(raw_df)
        context["ti"].xcom_push(key="clean_df", value=df_clean.to_json())

    def task_load(**context):
        df_clean = pd.read_json(context["ti"].xcom_pull(task_ids="transform_task", key="clean_df"))
        file_path = load_to_csv(df_clean, CONFIG["output_path"], CONFIG["output_file"])
        load_to_bigquery(df_clean, CONFIG["bigquery_table"], CONFIG["schema"])
        print(f"Saved and loaded: {file_path}")

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=task_extract,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=task_transform,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=task_load,
        provide_context=True,
    )

    extract_task >> transform_task >> load_task



# # Import your ETL functions (adjust path if they live in another module)
# from my_etl_module import save_yt_data

# # Default arguments for all tasks
# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "email_on_failure": False,
#     "email_on_retry": False,
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
# }

# # Define the DAG
# with DAG(
#     "yt_etl_pipeline",
#     default_args=default_args,
#     description="YouTube ETL pipeline DAG",
#     schedule_interval="@daily",   # Run daily (can also be "0 6 * * *" for 6 AM)
#     start_date=datetime(2025, 1, 1),
#     catchup=False,
#     tags=["ETL", "YouTube"],
# ) as dag:

#     # Extract + Transform + Load in one go
#     def run_etl(**kwargs):
#         user_name = "SomeYouTubeChannel"
#         output_path = "/opt/airflow/data/"
#         output_file_name = f"yt_data_{datetime.now().strftime('%Y%m%d')}"
#         save_yt_data(user_name, output_path, output_file_name, save_df=True, max_results=50)

#     etl_task = PythonOperator(
#         task_id="yt_etl_task",
#         python_callable=run_etl,
#         provide_context=True
#     )

#     etl_task
