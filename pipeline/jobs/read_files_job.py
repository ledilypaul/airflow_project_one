"""
Standalone Spark job: reads files listed in the DB and displays their content.

Run manually:
    spark-submit pipeline/jobs/read_files_job.py

Run via Airflow:
    BashOperator → spark-submit pipeline/jobs/read_files_job.py
"""
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

from utils.functions_utils import list_file_from_db
from utils.spark_session import get_spark_session
from pipeline.file_reader.base_reader import BaseFileReader

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
log = logging.getLogger(__name__)


def main():
    file_list = list_file_from_db()
    if not file_list:
        log.info("No files to process.")
        sys.exit(0)

    spark = get_spark_session("ReadFilesJob")
    reader = BaseFileReader(spark)

    success_count = 0
    for file in file_list:
        file_id = file["id"]
        file_name = file["file_name"]
        file_path = file["file_path"]
        try:
            content = reader.read_file(file_path)
            log.info("Read %s (id=%s)", file_name, file_id)
            content.show(truncate=False)
            success_count += 1
        except Exception as e:
            log.error("Error reading %s: %s", file_path, e)

    spark.stop()
    log.info("%d/%d files processed successfully.", success_count, len(file_list))
    sys.exit(0 if success_count > 0 else 1)


if __name__ == "__main__":
    main()
