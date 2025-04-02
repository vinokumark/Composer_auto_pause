from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from airflow.configuration import conf
import google.auth
from google.auth.transport.requests import Request
import logging

# Get the Airflow API URL (for internal access within Composer)
AIRFLOW_API_URL = conf.get("webserver", "base_url")

# List of DAGs to exclude from pausing
EXCLUDE_DAGS = {"airflow_db_cleanup", "airflow_monitoring", "list_running_dags"}  # Update this list as needed

# File path to save paused DAGs
FILE_PATH = "/home/airflow/gcs/data/active_dags.txt"

# Function to get an OAuth 2.0 token
def get_access_token():
    credentials, _ = google.auth.default()
    credentials.refresh(Request())
    return credentials.token

# Function to fetch and pause DAGs, excluding some
def pause_dags():
    url = f"{AIRFLOW_API_URL}/api/v1/dags"
    headers = {
        "Authorization": f"Bearer {get_access_token()}",
        "Content-Type": "application/json",
    }

    running_dags = []
    limit = 100  # Max limit per page
    offset = 0   # Pagination offset

    while True:
        response = requests.get(f"{url}?limit={limit}&offset={offset}", headers=headers)

        if response.status_code == 200:
            data = response.json()
            dags = data.get("dags", [])

            # Filter active DAGs (is_paused=False) and exclude specific DAGs
            active_dags = [dag["dag_id"] for dag in dags if not dag.get("is_paused", True)]
            running_dags.extend(active_dags)

            if len(dags) < limit:
                break  # Exit if this is the last page
            offset += limit  # Move to the next page
        else:
            logging.error("Failed to fetch DAGs: %s", response.text)
            raise Exception(f"Airflow API error: {response.text}")

    # Filter out DAGs that should not be paused
    dags_to_pause = [dag for dag in running_dags if dag not in EXCLUDE_DAGS]

    # Save paused DAGs to a text file
    with open(FILE_PATH, "w") as file:
        for dag_id in dags_to_pause:
            pause_url = f"{AIRFLOW_API_URL}/api/v1/dags/{dag_id}"
            payload = {"is_paused": True}
            response = requests.patch(pause_url, json=payload, headers=headers)

            if response.status_code == 200:
                logging.info(f"Paused DAG: {dag_id}")
                file.write(dag_id + "\n")  # Save to file
            else:
                logging.error(f"Failed to pause {dag_id}: {response.text}")

    logging.info(f"Total paused DAGs: {len(dags_to_pause)}")
    logging.info(f"Paused DAGs saved to {FILE_PATH}")

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
    "list_running_dags",
    default_args=default_args,
    description="Pause all active Airflow DAGs except excluded ones",
    schedule_interval=None,  # Adjust as needed
    catchup=False,
)

# Define the task
pause_dags_task = PythonOperator(
    task_id="pause_dags_task",
    python_callable=pause_dags,
    dag=dag,
)

# Task execution order
pause_dags_task
