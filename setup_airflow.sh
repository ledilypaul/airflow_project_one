#!/bin/bash

export AIRFLOW_HOME="$(pwd)"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"

airflow standalone

airflow users create \
    --username pol \
    --firstname pol \
    --lastname pol \
    --role Admin \
    --email paul@example.com \
    --password pol
