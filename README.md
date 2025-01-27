# airflow_project_one

source ../../virtual_env/airflow_env/bin/activate

export AIRFLOW_HOME=/Users/paulledily/Documents/Project/Work/Python/airflow_project/airflow_project_one
airflow webserver -p 8282

export AIRFLOW_HOME=/Users/paulledily/Documents/Project/Work/Python/airflow_project/airflow_project_one
airflow scheduler


airflow.cfg :
Variable: AIRFLOW__CORE__LOAD_EXAMPLES

load_examples = False

