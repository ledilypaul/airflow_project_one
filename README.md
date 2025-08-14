# airflow_project_one

Mac : 
    source ../../virtual_env/airflow_env/bin/activate

    ## Obsolète
    export AIRFLOW_HOME=/Users/paulledily/Documents/Project/Work/Python/airflow_project/airflow_project_one
    airflow webserver -p 8282

    export AIRFLOW_HOME=/Users/paulledily/Documents/Project/Work/Python/airflow_project/airflow_project_one
    airflow scheduler
 
## New 
Mac
export AIRFLOW_HOME=/Users/paulledily/Documents/Project/Work/Python/airflow_project/airflow_project_one

Windows
export AIRFLOW_HOME=/mnt/c/Users/paull/Documents/Code/Python/airflow_project_one

airflow standalone

Only work in a local env


Specification to add in airflow.cfg :

# Variable: AIRFLOW__CORE__LOAD_EXAMPLES
#
load_examples = False

# Variable: AIRFLOW__CORE__PLUGINS_FOLDER
#
plugins_folder = /mnt/c/Users/paull/Documents/Code/Python/airflow_project_one/plugins


# export AIRFLOW__CORE__DAGS_FOLDER=/mnt/c/Users/paull/Documents/Code/Python/airflow_project_one/dags
# export PYTHONPATH=/mnt/c/Users/paull/Documents/Code/Python/airflow_project_one
export AIRFLOW_HOME=/mnt/c/Users/paull/Documents/Code/Python/airflow_project_one

airflow config get-value core dags_folder
airflow config get-value core airflow_home

Airflow 3.0.0 comes with Simple auth manager by default. You can not create users using 'airflow users create' when using simple auth manager.

Run 'pip install apache-airflow-providers-fab' to install fab auth manager and set the below variable in airflow.cfg file to enable fab auth manager.

auth_manager = airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager

After you set this, you should be able to create users using 'airflow users create' command.

(.venv) ledilypaul@PC-Pol:/mnt/c/Users/paull/Documents/Code/Python/airflow_project_one$ export AIRFLOW_HOME=/mnt/c/Users/paull/Documents/Code/Python/airflow_project_one/dags
(.venv) ledilypaul@PC-Pol:/mnt/c/Users/paull/Documents/Code/Python/airflow_project_one$ airflow db migrate

airflow config get-value core dags_folder
airflow config get-value core airflow_home

airflow_project_one
📌 Prérequis

    Python 3.12

    Poetry pour la gestion des dépendances

    Apache Airflow (≥ 3.0.0)

    Environnement virtuel dédié

🚀 Installation
1️⃣ Cloner le projet

git clone <url-du-repo>
cd airflow_project_one

2️⃣ Créer et activer l’environnement virtuel

MacOS :

python3 -m venv .venv
source .venv/bin/activate

Windows (WSL ou PowerShell) :

python -m venv .venv
source .venv/bin/activate

3️⃣ Installer les dépendances

poetry install

⚙️ Configuration Airflow
1️⃣ Définir AIRFLOW_HOME

MacOS :

export AIRFLOW_HOME=/Users/paulledily/Documents/Project/Work/Python/airflow_project/airflow_project_one

Windows (WSL) :

export AIRFLOW_HOME=/mnt/c/Users/paull/Documents/Code/Python/airflow_project_one

    💡 Tu peux mettre cette commande dans ~/.bashrc ou ~/.zshrc pour éviter de la retaper à chaque fois.

2️⃣ Empêcher le chargement des DAGs d’exemple

Dans le fichier airflow.cfg (ou via variables d’environnement) :

[core]
load_examples = False

3️⃣ Spécifier les dossiers personnalisés

Toujours dans airflow.cfg :

[core]
plugins_folder = /mnt/c/Users/paull/Documents/Code/Python/airflow_project_one/plugins
dags_folder = /mnt/c/Users/paull/Documents/Code/Python/airflow_project_one/dags

Ou en variables d’environnement :

export AIRFLOW__CORE__PLUGINS_FOLDER=/mnt/c/Users/paull/Documents/Code/Python/airflow_project_one/plugins
export AIRFLOW__CORE__DAGS_FOLDER=/mnt/c/Users/paull/Documents/Code/Python/airflow_project_one/dags

4️⃣ Ajouter le projet au PYTHONPATH

Pour que tes DAGs puissent importer tes modules :

export PYTHONPATH=/mnt/c/Users/paull/Documents/Code/Python/airflow_project_one

▶️ Lancer Airflow
Initialiser ou migrer la base

airflow db migrate

Démarrer Airflow en standalone

airflow standalone

Airflow sera disponible par défaut sur http://localhost:8080.
🔑 Gestion des utilisateurs (Airflow ≥ 3.0.0)

Airflow 3.0.0 utilise par défaut Simple Auth Manager qui ne permet pas de créer d’utilisateurs avec :

airflow users create ...

Pour activer FAB Auth Manager (qui permet de créer des utilisateurs via CLI) :

    Installer le provider :

pip install apache-airflow-providers-fab

    Dans airflow.cfg :

[core]
auth_manager = airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager

    Créer un utilisateur :

airflow users create \
    --username admin \
    --firstname Paul \
    --lastname Ledily \
    --role Admin \
    --email paul.ledily@hotmail.fr \
    --password <ton_mot_de_passe>

🛠 Vérifier la configuration

airflow config get-value core dags_folder
airflow config get-value core airflow_home

📂 Structure du projet

airflow_project_one/
├── dags/                # Contient les DAGs Airflow
├── pipeline/            # Code de traitement des données
├── utils/               # Fonctions utilitaires
├── config/              # Fichiers de configuration YAML / JSON
├── sandbox/             # Scripts de test
├── tests/               # Tests unitaires
├── airflow.cfg          # Configuration Airflow
└── pyproject.toml       # Dépendances Poetry