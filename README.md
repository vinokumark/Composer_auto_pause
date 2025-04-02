# Composer_auto_pause
To pause gcp composer all job one click using two DAG.

## Why this DAG

We have 500+ DAG and some dag manual dag based on the Data process. During the composer update and deployment the manual pause took more time.
Here the simple DAG used internal airflow api to pause and resume the job.

#
