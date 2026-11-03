#!/bin/bash

venv_name="airflow_env"
venv_path="/Users/paulledily/Documents/Project/Work/Python/virtual_env/airflow_env/"
airflow_webserver="/path/to/airflow/bin/airflow webserver"

# Chemin vers le script de démarrage de l'scheduler Airflow (à adapter)
airflow_scheduler="/path/to/airflow/bin/airflow scheduler"

function activate_venv {
  source "$venv_path/bin/activate"
}

activate_venv
export AIRFLOW_HOME=/Users/paulledily/Documents/Project/Work/Python/airflow_project/airflow_project_one
