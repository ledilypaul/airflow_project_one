#!/bin/bash

# Activer l'environnement virtuel
echo "Activation de l'environnement virtuel..."
source /Users/paulledily/virtual_env/airflow_env/bin/activate

# Définir la variable d'environnement AIRFLOW_HOME
echo "Définition de la variable d'environnement AIRFLOW_HOME..."
export AIRFLOW_HOME=/Users/paulledily/Documents/Project/Work/Python/airflow_project/airflow_project_one

# Ouvrir un nouveau terminal pour le serveur web
echo "Démarrage du serveur web Airflow..."
osascript -e 'tell application "Terminal" to do script "bash -c \"source /Users/paulledily/virtual_env/airflow_env/bin/activate; export AIRFLOW_HOME=/Users/paulledily/Documents/Project/Work/Python/airflow_project/airflow_project_one; airflow webserver -p 8282\""'

# Ouvrir un nouveau terminal pour le scheduler
echo "Démarrage du scheduler Airflow..."
osascript -e 'tell application "Terminal" to do script "bash -c \"source /Users/paulledily/virtual_env/airflow_env/bin/activate; export AIRFLOW_HOME=/Users/paulledily/Documents/Project/Work/Python/airflow_project/airflow_project_one; airflow scheduler\""'