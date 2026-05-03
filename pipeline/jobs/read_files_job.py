"""
Standalone Spark job: reads files listed in the DB and displays their content.

Run manually:
    spark-submit pipeline/jobs/read_files_job.py

Run via Airflow:
    BashOperator → spark-submit pipeline/jobs/read_files_job.py
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

from utils.functions_utils import list_file_from_db
from utils.spark_session import get_spark_session
from pipeline.file_reader.base_reader import BaseFileReader


def main():
    file_list = list_file_from_db()
    if not file_list:
        print("No files to process.")
        sys.exit(0)

    spark = get_spark_session("ReadFilesJob")
    reader = BaseFileReader(spark)

    success_count = 0
    for file in file_list:
        file_id, file_name, file_path = file[0], file[1], file[2]
        try:
            content = reader.read_file(file_path)
            print(f"Read {file_name} (id={file_id})")
            content.show(truncate=False)
            success_count += 1
        except Exception as e:
            print(f"Error reading {file_path}: {e}", file=sys.stderr)

    spark.stop()
    print(f"{success_count}/{len(file_list)} files processed successfully.")
    sys.exit(0 if success_count > 0 else 1)


if __name__ == "__main__":
    main()
