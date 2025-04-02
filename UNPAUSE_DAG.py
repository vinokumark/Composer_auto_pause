from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from airflow.configuration import conf
import google.auth
from google.auth.transport.requests import Request
import logging
import os

# Get the Airflow API URL (for internal access within Composer)
AIRFLOW_API_URL = conf.get("webserver", "base_url")

# File path where paused DAGs were saved
FILE_PATH = "/home/airflow/gcs/data/active_dags.txt"  # Change this if you used a different path

# Function to get an OAuth 2.0 token
def get_access_token():
    credentials, _ = google.auth.default()
    credentials.refresh(Request())
    return credentials.token

# Function to unpause DAGs from the saved list
def unpause_dags():
    if not os.path.exists(FILE_PATH):
        logging.error(f"File {FILE_PATH} not found. No DAGs to unpause.")
        return

    # Read the paused DAGs from file
    with open(FILE_PATH, "r") as file:
        dag_ids = [line.strip() for line in file if line.strip()]

    if not dag_ids:
        logging.info("No DAGs to unpause.")
        return

    headers = {
        "Authorization": f"Bearer {get_access_token()}",
        "Content-Type": "application/json",
    }

    unpaused_dags = []

    for dag_id in dag_ids:
        unpause_url = f"{AIRFLOW_API_URL}/api/v1/dags/{dag_id}"
        payload = {"is_paused": False}
        response = requests.patch(unpause_url, json=payload, headers=headers)

        if response.status_code == 200:
            logging.info(f"Unpaused DAG: {dag_id}")
            unpaused_dags.append(dag_id)
        else:
            logging.error(f"Failed to unpause {dag_id}: {response.text}")

    logging.info(f"Total unpaused DAGs: {len(unpaused_dags)}")

    # Optional: Clear the file after unpausing
    os.remove(FILE_PATH)

# Define DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 5),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    "unpause_dags",
    default_args=default_args,
    description="Unpause previously paused DAGs from a file",
    schedule_interval=None,  # Manually triggered
    catchup=False,
)

# Define the task
unpause_dags_task = PythonOperator(
    task_id="unpause_dags_task",
    python_callable=unpause_dags,
    dag=dag,
)

# Task execution order
unpause_dags_task
