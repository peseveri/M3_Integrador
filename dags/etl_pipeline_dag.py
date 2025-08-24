import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
from scripts import airbyte
from dotenv import load_dotenv

load_dotenv("/opt/airflow/.env")


AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")

with DAG(
    dag_id="etl_orquestado",
    start_date=datetime(2000, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    airbyte_task = PythonOperator(
        task_id="run_airbyte",
        python_callable=airbyte.main, 
    )

    pyspark_task = DockerOperator(
    task_id="run_pyspark",
    image="spark-app:latest",
    command="spark-submit /app/etl_script.py",
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge",
    mount_tmp_dir=False,
    environment={
        "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
        "AWS_DEFAULT_REGION": AWS_DEFAULT_REGION
    },
)

    airbyte_task >> pyspark_task
    
