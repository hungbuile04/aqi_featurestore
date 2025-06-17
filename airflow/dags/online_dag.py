from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import extract
import load
import redis_data

# DAG Configuration
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 12),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "air_quality_to_gcs",
    default_args=default_args,
    schedule_interval= None, #"5 0 * * 1",  # Runs weekly at Monday, 7:05 UTC+7
    catchup=False
)

task_redis = PythonOperator(
    task_id="fetch_and_load_to_redis",
    python_callable=redis_data.extract_and_load_to_redis,
    dag=dag,
    on_failure_callback=lambda context: print("Fetch data failed!")
)



task_redis 