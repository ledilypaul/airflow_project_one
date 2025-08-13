# airflow_project_one

source ../../virtual_env/airflow_env/bin/activate

export AIRFLOW_HOME=/Users/paulledily/Documents/Project/Work/Python/airflow_project/airflow_project_one
airflow webserver -p 8282

export AIRFLOW_HOME=/Users/paulledily/Documents/Project/Work/Python/airflow_project/airflow_project_one
airflow scheduler


airflow.cfg :
Variable: AIRFLOW__CORE__LOAD_EXAMPLES

load_examples = False

# admin d6Mn8yzmKb4xrGWG
# export AIRFLOW__CORE__DAGS_FOLDER=/mnt/c/Users/paull/Documents/Code/Python/airflow_project_one/dags
# export PYTHONPATH=/mnt/c/Users/paull/Documents/Code/Python/airflow_project_one
export AIRFLOW_HOME=/mnt/c/Users/paull/Documents/Code/Python/airflow_project_one



Airflow 3.0.0 comes with Simple auth manager by default. You can not create users using 'airflow users create' when using simple auth manager.

Run 'pip install apache-airflow-providers-fab' to install fab auth manager and set the below variable in airflow.cfg file to enable fab auth manager.

auth_manager = airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager

After you set this, you should be able to create users using 'airflow users create' command.

(.venv) ledilypaul@PC-Pol:/mnt/c/Users/paull/Documents/Code/Python/airflow_project_one$ export AIRFLOW_HOME=/mnt/c/Users/paull/Documents/Code/Python/airflow_project_one/dags
(.venv) ledilypaul@PC-Pol:/mnt/c/Users/paull/Documents/Code/Python/airflow_project_one$ airflow db migrate

airflow config get-value core dags_folder
airflow config get-value core airflow_home

