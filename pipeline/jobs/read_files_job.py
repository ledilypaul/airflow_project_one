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

Exit codes: 0 = every file processed, 1 = at least one file failed,
2 = missing/invalid --run-id, 3 = no file_list row for that run id.
"""

import argparse
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
    from utils.functions_utils import list_file_from_db, update_file_status
    from utils.spark_session import get_spark_session
except ModuleNotFoundError:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    sys.path.insert(0, str(PROJECT_ROOT))
    from pipeline.file_reader.base_reader import BaseFileReader
    from utils.functions_utils import list_file_from_db, update_file_status
    from utils.spark_session import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
log = logging.getLogger(__name__)


def main(run_id: str):
    if run_id is None or run_id == "" or run_id == "None":
        log.error(
            "Missing or invalid run_id (%r): refusing to query file_list without a run_id "
            "to scope to (idempotence, P1-3). Pass a real --run-id.",
            run_id,
        )
        sys.exit(2)

    file_list = list_file_from_db(run_id)
    if not file_list:
        # P1-4 / C-3: a source run that emitted the `file_list` asset always
        # has at least one row (empty listings and "every file vanished" both
        # skip the insertion task, so no event). Zero rows is an anomaly
        # (typo in a manual run_id, purged rows...), not a green no-op.
        log.error(
            "no file_list rows for run_id=%r: typo in the run_id or purged data? "
            "Nothing was processed.",
            run_id,
        )
        sys.exit(3)

    spark = get_spark_session("ReadFilesJob")
    try:
        reader = BaseFileReader(spark)

        success_count = 0
        fail_count = 0
        for file in file_list:
            file_id = file["id"]
            file_name = file["file_name"]
            file_path = file["file_path"]
            try:
                content = reader.read_file(file_path)
                log.info("Read %s (id=%s)", file_name, file_id)
                content.show(truncate=False)
                update_file_status(file_id, "processed")
                success_count += 1
            except Exception as e:
                log.error("Error reading %s: %s", file_path, e)
                update_file_status(file_id, "failed", error_message=str(e))
                fail_count += 1
    finally:
        spark.stop()

    log.info("%d/%d files processed successfully.", success_count, len(file_list))
    sys.exit(1 if fail_count > 0 else 0)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the job's command line (`sys.argv[1:]` when `argv` is None).

    read_file_dag passes the run id as a single `--run-id=<value>` argument,
    so a run id starting with `-` is not mistaken for an option (P1-4 / C-4).
    """
    parser = argparse.ArgumentParser(
        description="Read files listed in the DB for a given Airflow run_id."
    )
    parser.add_argument(
        "--run-id", required=True, help="dag_run_id of the list_files_dag run to process."
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    main(run_id=parse_args().run_id)
