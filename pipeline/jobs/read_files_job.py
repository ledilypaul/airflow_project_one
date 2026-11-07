"""
Standalone Spark job: reads files listed in the DB and displays their content.

Run manually (recommended, uses the project's uv-managed venv):
    uv run spark-submit pipeline/jobs/read_files_job.py

Run via Airflow:
    BashOperator → spark-submit pipeline/jobs/read_files_job.py

Run with a bare/system/cluster `spark-submit` whose driver Python is not the
project's venv (e.g. `ENV=prod` on YARN/Kubernetes where the interpreter
isn't the one `uv sync` installed into): `pipeline.*` / `utils.*` won't be on
sys.path in that case, so the fallback below adds the project root itself.
"""

import logging
import sys
from pathlib import Path

# airflow-project-one est installé en mode éditable (`uv sync`), donc
# `pipeline.*` / `utils.*` / `storage.*` sont normalement importables sans
# manipuler sys.path (voir CLAUDE.md, M5). Mais ce script peut aussi être
# lancé avec un interpréteur qui n'est pas celui du venv uv (spark-submit
# "nu", driver/exécuteur de cluster sous ENV=prod) : dans ce cas l'install
# éditable n'existe pas pour cet interpréteur et l'import échoue. On ne
# bidouille donc sys.path qu'en dernier recours, si l'import direct rate.
try:
    from pipeline.file_reader.base_reader import BaseFileReader
    from utils.functions_utils import list_file_from_db
    from utils.spark_session import get_spark_session
except ModuleNotFoundError:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    sys.path.insert(0, str(PROJECT_ROOT))
    from pipeline.file_reader.base_reader import BaseFileReader
    from utils.functions_utils import list_file_from_db
    from utils.spark_session import get_spark_session

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
