"""
P3-5 — Airflow 3.4 Asset scheduling of transform_dag, at the scheduler level.

The unit tests (tests/unit/dags/test_p3_5_asset_batching.py) check the
timetable. This one checks what the **scheduler** of the installed Airflow
does with it, on the real DAG files synced into a migrated SQLite metadata
DB, with real `AssetManager.register_asset_change` events:
  - transform_dag (`AssetAny` of the `lake_staging__<source>` Assets of the
    example sources, `batch_asset_events=True`): three events of two different
    sources queued together give ONE run that consumes all three (P5-3: the
    union of the sources' Assets, still one `dbt build` for the events queued);
  - an event emitted while the DAG is paused is not queued (no
    AssetDagRunQueue row) and no later run consumes it: a behaviour change
    from 3.3, where the next run took every event since the previous run
    (documented in CLAUDE.md, "Unpause transform_dag").

P5-3: `read_file_dag` (one run per `file_list` event, `max_active_runs=1`) is gone,
with the Asset: the ingest DAGs are scheduled by cron, not by an Asset.

Scheduler internals (`SchedulerJobRunner._create_dagruns_for_dags`, the same
call as `_do_scheduling`) are used on purpose: only the run creation is
exercised, no task runs. If an Airflow upgrade renames them, this test
fails loudly: re-check the documented semantics before adapting it.

Isolation: same as the `dag.test()` e2e (own AIRFLOW_HOME, own SQLite DB,
subprocess).
"""

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DAGS_FOLDER = PROJECT_ROOT / "dags"
EXAMPLES_CONFIG_ROOT = PROJECT_ROOT / "examples" / "config"

pytestmark = pytest.mark.skipif(os.name != "posix", reason="needs a POSIX PATH")

SCHEDULER_SCRIPT = textwrap.dedent(
    """
    import json
    import sys

    from sqlalchemy import select, update

    from airflow.assets.manager import AssetManager
    from airflow.dag_processing.bundles.manager import DagBundlesManager
    from airflow.dag_processing.dagbag import DagBag, sync_bag_to_db
    from airflow.jobs.job import Job
    from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
    from airflow.models.asset import AssetDagRunQueue, AssetModel
    from airflow.models.dag import DagModel
    from airflow.models.dagrun import DagRun
    from airflow.utils.session import create_session
    from airflow.utils.sqlalchemy import prohibit_commit
    from airflow.utils.state import DagRunState

    dags_folder, result_file = sys.argv[1:3]
    # As dag.test() does: register the bundles (`dags-folder`), then the DAGs.
    DagBundlesManager().sync_bundles_to_db()
    bag = DagBag(dag_folder=dags_folder)
    assert not bag.import_errors, bag.import_errors
    sync_bag_to_db(bag, "dags-folder", None)

    def set_paused(dag_id, paused):
        with create_session() as session:
            session.execute(
                update(DagModel).where(DagModel.dag_id == dag_id).values(is_paused=paused)
            )

    def emit(asset_name):
        with create_session() as session:
            asset = session.scalar(select(AssetModel).where(AssetModel.name == asset_name))
            event = AssetManager.register_asset_change(asset=asset, session=session)
            session.flush()
            return event.id

    def queued(dag_id):
        with create_session() as session:
            return sorted(
                session.scalars(
                    select(AssetDagRunQueue.asset_event_id).where(
                        AssetDagRunQueue.target_dag_id == dag_id
                    )
                )
            )

    def schedule():
        runner = SchedulerJobRunner(job=Job())
        with create_session() as session:
            with prohibit_commit(session) as guard:
                runner._create_dagruns_for_dags(guard, session)

    def runs(dag_id):
        with create_session() as session:
            return [
                {
                    "state": str(run.state),
                    "run_type": str(run.run_type),
                    "events": sorted(event.id for event in run.consumed_asset_events),
                }
                for run in session.scalars(
                    select(DagRun).where(DagRun.dag_id == dag_id).order_by(DagRun.id)
                )
            ]

    result = {}

    # --- transform_dag: the queued events are batched into one run ----------
    set_paused("transform_dag", True)
    result["paused_event"] = emit("lake_staging__orders")
    result["queued_while_paused"] = queued("transform_dag")
    set_paused("transform_dag", False)
    result["transform_events"] = [
        emit("lake_staging__orders"),
        emit("lake_staging__imdb_movies"),
        emit("lake_staging__orders"),
    ]
    result["transform_queued"] = queued("transform_dag")
    schedule()
    schedule()
    result["transform_runs"] = runs("transform_dag")

    with open(result_file, "w") as f:
        json.dump(result, f)
    """
)


@pytest.fixture(
    scope="module", params=["True", "False"], ids=["scheduler-batch", "scheduler-no-batch"]
)
def scheduler_result(request, tmp_path_factory):
    """Run once with each deployment default (`[scheduler] batch_asset_events`):
    the value written in each DAG must win over both."""
    home = tmp_path_factory.mktemp("airflow_home_p3_5")
    env = {k: v for k, v in os.environ.items() if not k.startswith(("AIRFLOW", "OPENLINEAGE"))}
    env.update(
        AIRFLOW_HOME=str(home),
        AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=f"sqlite:///{home / 'airflow.db'}",
        AIRFLOW__CORE__DAGS_FOLDER=str(DAGS_FOLDER),
        AIRFLOW__CORE__LOAD_EXAMPLES="False",
        AIRFLOW__CORE__UNIT_TEST_MODE="True",
        AIRFLOW__OPENLINEAGE__DISABLED="True",
        AIRFLOW__METRICS__OTEL_ON="False",
        AIRFLOW__COSMOS__ENABLE_TELEMETRY="False",
        AIRFLOW__SCHEDULER__BATCH_ASSET_EVENTS=request.param,
        PIPELINE_CONFIG_ROOT=str(EXAMPLES_CONFIG_ROOT),
    )
    subprocess.run(
        [sys.executable, "-m", "airflow", "db", "migrate"],
        env=env,
        check=True,
        capture_output=True,
        timeout=600,
    )
    script = home / "p3_5_scheduler.py"
    script.write_text(SCHEDULER_SCRIPT)
    result_file = home / "result.json"
    proc = subprocess.run(
        [sys.executable, str(script), str(DAGS_FOLDER), str(result_file)],
        env=env,
        cwd=home,
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert proc.returncode == 0, proc.stdout[-3000:] + proc.stderr[-6000:]
    return json.loads(result_file.read_text())


def test_p3_5_transform_dag_batches_the_queued_events_of_every_source(scheduler_result):
    assert scheduler_result["transform_queued"] == scheduler_result["transform_events"]
    runs = scheduler_result["transform_runs"]
    assert len(runs) == 1, runs
    assert runs[0]["events"] == scheduler_result["transform_events"]
    assert runs[0]["run_type"] == "asset_triggered"


def test_p3_5_event_emitted_while_paused_is_never_consumed(scheduler_result):
    paused_event = scheduler_result["paused_event"]
    assert scheduler_result["queued_while_paused"] == []
    consumed = {e for run in scheduler_result["transform_runs"] for e in run["events"]}
    assert paused_event not in consumed
