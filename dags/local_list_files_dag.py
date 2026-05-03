import sys
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

CONFIG_PATH = str(PROJECT_ROOT / "config" / "extractor_config.yaml")

DEFAULT_ARGS  = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id="list_files_dag",
    description="List files in the source directory and persist metadata",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 12, 18),
    schedule=None,  # ou "0 * * * *" pour toutes les heures
    catchup=False,
    tags=["extraction", "file-listing"],
)

def list_files_task(): 
    """
    List files in the source directory and store the list in XCom.
    """
    @task
    def list_files() -> list[str]:
        """List empty with Extractor (instantaite during Execution)."""
        from pipeline.extractor.extractor import Extractor
        extractor = Extractor(config_path=CONFIG_PATH)
        files = extractor.list_files()
        if not files:
            # Renvoyer une liste vide est OK ; lever une exception
            # uniquement si l'absence de fichiers est anormale
            return []
        return files

    @task
    def insert_files(file_list = list[str]) -> int:
        """Insert metadata of every file in base"""
        from utils.functions_utils import insert_into_file_list
        
        if not file_list:
            raise ValueError('No files found in Xcoms')
            return 0
        
        inserted = 0
        for filepath in file_list:
            try:
                filename = os.path.basename(filepath)
                mod_time = datetime.fromtimestamp(os.path.getmtime(filepath))     
                insert_into_file_list([filename, filepath, mod_time])
                inserted += 1
            except FileNotFoundError:
                print(f"Can't find file, skip: {filepath}")
            except Exception as e:
                print(f"Insertion Error {filepath}: {e}")
                raise 
    
        print(f"{inserted}/{len(file_list)} files inserted")
        return inserted
    
    insert_files(list_files())

list_files_task()