"""
DAG: list_files_dag
====================

Liste les fichiers du répertoire source et insère leurs métadonnées en base.

------------------------------------------------------------------------------
VERSION : PythonOperator (style classique Airflow)
------------------------------------------------------------------------------

Cette version utilise le style classique avec PythonOperator.
Une version équivalente écrite avec TaskFlow API (@task / @dag) est disponible
dans le fichier : `local_list_files_dag_taskflow.py`.

Les deux versions sont fonctionnellement équivalentes — sous le capot, le
décorateur @task génère lui-même un PythonOperator. Le choix entre les deux
relève du style et de la maintenabilité, pas de la performance.

Quand préférer PythonOperator (cette version) :
  - Cohérence avec une codebase existante qui utilise déjà ce style
  - Préférence d'équipe pour le style explicite
  - Tâches très dynamiques construites en boucle avec des task_id calculés

Quand préférer TaskFlow API (l'autre version) :
  - Nouveau projet (recommandation officielle Airflow depuis 2.0)
  - Besoin de typage clair des entrées/sorties des tasks
  - Passage XCom implicite et refactoring-safe
  - Dépendances exprimées par les appels de fonction (plus lisible)

------------------------------------------------------------------------------
BONNES PRATIQUES APPLIQUÉES ICI
------------------------------------------------------------------------------

1. AUCUNE INSTANCIATION AU TOP-LEVEL
   L'Extractor est instancié à l'intérieur de la fonction callable, PAS au
   niveau module. Raison : Airflow parse les fichiers DAG toutes les ~30s
   (min_file_process_interval). Tout code top-level est ré-exécuté à ce
   rythme, ce qui peut :
     - Ralentir le scheduler (I/O disque, ouverture YAML, connexions DB)
     - Saturer des pools de connexions inutilement
     - Faire DISPARAÎTRE le DAG de l'UI si l'init lève une exception

2. IMPORTS MÉTIER À L'INTÉRIEUR DES TASKS
   `from pipeline.extractor.extractor import Extractor` est dans la task,
   pas au top-level. Si le module fait de l'I/O à l'import (logger fichier,
   chargement de config…), on retombe dans le piège ci-dessus.

3. GESTION D'ERREURS PAR FICHIER
   Chaque fichier est traité dans un try/except : un fichier manquant ne
   fait pas planter toute la task. Les erreurs inconnues sont remontées
   (raise) pour faire échouer la task — préférable pour un job d'ingestion.

4. UTILISATION DE pathlib ET os.path.basename
   Plus propre et cross-platform que `file.split("/")[-1]` (qui casse
   sur Windows).

5. schedule=None EXPLICITE
   Évite le DeprecationWarning Airflow 2.4+ pour les DAGs sans schedule.
   À remplacer par un cron ("0 * * * *") si exécution périodique souhaitée.
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# --- Configuration des chemins (constantes top-level OK : pas d'I/O) ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

CONFIG_PATH = str(PROJECT_ROOT / "config" / "extractor_config.yaml")


# --- Callables des tasks ---

def list_files_task(**kwargs):
    """
    List files in the source directory and store the list in XCom.

    L'Extractor est instancié ICI (à l'exécution), pas au top-level.
    """
    # Import local : évite tout I/O au moment du parse du DAG
    from pipeline.extractor.extractor import Extractor

    extractor = Extractor(config_path=CONFIG_PATH)
    files = extractor.list_files()
    return files


def insert_files_to_db(**kwargs):
    """
    Insère les métadonnées (nom, chemin, date de modif) de chaque fichier
    en base de données. Récupère la liste via XCom.
    """
    # Import local : même raison que ci-dessus
    from utils.functions_utils import insert_into_file_list

    ti = kwargs['ti']
    file_list = ti.xcom_pull(task_ids='list_files_task')

    if not file_list:
        print("Aucun fichier à insérer.")
        return 0

    inserted = 0
    for filepath in file_list:
        try:
            filename = os.path.basename(filepath)
            mod_time = datetime.fromtimestamp(os.path.getmtime(filepath))
            insert_into_file_list([filename, filepath, mod_time])
            inserted += 1
        except FileNotFoundError:
            # Fichier supprimé entre le list et l'insert : on skip et on log
            print(f"Fichier introuvable, skip: {filepath}")
        except Exception as e:
            # Erreur inconnue : on remonte pour faire échouer la task
            print(f"Erreur insertion {filepath}: {e}")
            raise

    print(f"{inserted}/{len(file_list)} fichiers insérés.")
    return inserted


# --- Définition du DAG ---

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,  # 1 retry par défaut : absorbe les glitches transitoires
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='list_files_dag',
    default_args=default_args,
    description='List files in the source directory and persist metadata',
    schedule=None,  # Manuel uniquement. Mettre un cron pour exécution périodique.
    catchup=False,
    tags=['extraction', 'file-listing', 'python-operator'],
) as dag:

    list_files = PythonOperator(
        task_id='list_files_task',
        python_callable=list_files_task,
    )

    insert_files = PythonOperator(
        task_id='insert_files_to_db',
        python_callable=insert_files_to_db,
        # Note : pas besoin de provide_context=True en Airflow 2.x,
        # le contexte (ti, dag_run, etc.) est passé automatiquement via **kwargs.
    )

    list_files >> insert_files