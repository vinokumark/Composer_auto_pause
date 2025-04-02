# Composer_auto_pause
To pause gcp composer all job one click using two DAG.

## Why this DAG

We have 500+ DAG and some dag manual dag based on the Data process. During the composer update and deployment the manual pause took more time.
Here the simple DAG used internal airflow api to pause and resume the job.

#### PAUSE_RUNNING_DAGS_V1.py
mention the dag which need to exclude
EXCLUDE_DAGS = {"airflow_db_cleanup", "airflow_monitoring", "list_running_dags"}
this dag will get list of dag and pause it, save the list in to text path

#### UNPAUSE_DAG.py
This Dag will use the list of dag from text from and do the unpause activity.
