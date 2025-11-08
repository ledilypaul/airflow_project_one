# airflow_project_one

## 📌 Prerequisites

- Python **3.12**
- **Poetry** for dependency management
- **Apache Airflow ≥ 3.0.0**
- A dedicated virtual environment (**recommended**)

---

## 🚀 Installation

### 1️⃣ Clone the repository

```bash
git clone https://github.com/ledilypaul/airflow_project_one.git
cd airflow_project_one
```

## ⚙️ Airflow Configuration

### 1️⃣ Set the AIRFLOW_HOME environment variable

## Mac
export AIRFLOW_HOME=path/airflow_project_one

## Windows (WSL)
export AIRFLOW_HOME=/mnt/c/Users/paull/Documents/Code/Python/airflow_project_one

Add this line to your ~/.bashrc or ~/.zshrc to persist it.

### 2️⃣ Specification to add in airflow.cfg :

#### Disable exampls Dags
```bash
[core]
load_examples = False
```
#### Specify custom folders
```bash
[core]
plugins_folder = path/airflow_project_one/plugins
dags_folder = path/airflow_project_one/dags
```

Or via environment variables:
```bash
export AIRFLOW__CORE__PLUGINS_FOLDER=path/airflow_project_one/plugins
export AIRFLOW__CORE__DAGS_FOLDER=path/airflow_project_one/dags
```

#### Add the project to PYTHONPATH
```bash
export PYTHONPATH=/mnt/c/Users/paull/Documents/Code/Python/airflow_project_one
```
This allows your DAGs to import internal modules.

## Run Airflow

Initialize or migrate the database:
```bash
airflow db migrate
```

Start Airflow:
```bash
airflow standalone
```
Accessible at: http://localhost:8080

## 🔑 User Management (Airflow ≥ 3.0.0)

Airflow 3.0.0 uses Simple Auth Manager by default.
It does not allow creating users via:
```bash
airflow users create ...
```

To enable FAB Auth Manager:

### 1️⃣ Install the provider
```bash
pip install apache-airflow-providers-fab
```

### 2️⃣ Update airflow.cfg

```ini
[core]
auth_manager = airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
```

### 3️⃣ Create a user
```bash
airflow users create \
    --username admin \
    --firstname Marty \
    --lastname McFly \
    --role Admin \
    --email marty.mcfly@hotmail.us \
    --password <your_password>
```

#### Verify configuration
```bash
airflow config get-value core dags_folder
airflow config get-value core airflow_home
```

## 📂 Project structure
airflow_project_one/
├── dags/                # Airflow DAGs
├── pipeline/            # Data extraction / transformation logic
├── utils/               # Utility functions
├── config/              # JSON / YAML config
├── sandbox/             # Test scripts
├── tests/               # Unit tests
├── airflow.cfg          # Airflow configuration
└── pyproject.toml       # Poetry dependencies

## 🐳 Running with Docker
Initialize the database:
```bash
docker compose run airflow-webserver airflow db init
```

Start services:
```bash
docker compose up -d
```
