"""
P3-5 — Airflow 3.4: `batch_asset_events` is explicit on both Asset-scheduled DAGs.

Airflow 3.4.0 (#68517) creates one DAG run per Asset event by default, where
3.3 put every queued event into one run. The default comes from
`[scheduler] batch_asset_events` (False) and is read when the timetable is
built, i.e. it depends on the deployment. Both DAGs therefore set it
themselves:
  - read_file_dag: False, one run per listing, so one Spark application per
    source run with its own retries and its own state;
  - transform_dag: True, the `lake_staging` events queued while a `dbt build`
    runs are consumed together by ONE next run (each run rebuilds the whole
    staging zone: one build per event would only repeat the same work).

Checked on the installed Task SDK (airflow/sdk/definitions/timetables/assets.py)
and core (airflow/timetables/base.py `group_asset_events`, scheduler
`_create_dag_runs_asset_triggered`). The scheduler-level behaviour (runs
actually created, `max_active_runs=1`, paused DAG) is covered by
tests/integration/test_p3_5_asset_scheduling.py.
"""

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from airflow.models import DagBag
from airflow.sdk import Asset, AssetTriggeredTimetable

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
DAGS_FOLDER = PROJECT_ROOT / "dags"
READ_DAG_FILE = DAGS_FOLDER / "local_read_file_dag.py"
TRANSFORM_DAG_FILE = DAGS_FOLDER / "local_transform_dag.py"

EXPECTED = {
    # dag_id: (DAG file, Asset it is scheduled on, batch_asset_events)
    "read_file_dag": (READ_DAG_FILE, "file_list", False),
    "transform_dag": (TRANSFORM_DAG_FILE, "lake_staging", True),
}


def _dag(dag_id: str):
    dag_file = EXPECTED[dag_id][0]
    bag = DagBag(dag_folder=str(dag_file))
    assert not bag.import_errors, bag.import_errors
    return bag.dags[dag_id]


@pytest.mark.parametrize("dag_id", sorted(EXPECTED))
def test_p3_5_batch_asset_events_is_explicit(dag_id):
    _, asset_name, batch = EXPECTED[dag_id]
    dag = _dag(dag_id)
    timetable = dag.timetable
    assert isinstance(timetable, AssetTriggeredTimetable), type(timetable)
    assert timetable.batch_asset_events is batch
    # Scheduled on that single Asset, matched by name (see test_p1_4_assets).
    assert timetable.asset_condition == Asset(name=asset_name)
    # P1-4 / C-7: still one run at a time.
    assert dag.max_active_runs == 1
    assert dag.catchup is False


@pytest.mark.parametrize("scheduler_default", ["True", "False"])
@pytest.mark.parametrize("dag_id", sorted(EXPECTED))
def test_p3_5_batching_does_not_follow_the_scheduler_setting(
    monkeypatch, dag_id, scheduler_default
):
    """`[scheduler] batch_asset_events` only changes the default: the value
    written in the DAG wins, whatever the deployment sets."""
    monkeypatch.setenv("AIRFLOW__SCHEDULER__BATCH_ASSET_EVENTS", scheduler_default)
    assert _dag(dag_id).timetable.batch_asset_events is EXPECTED[dag_id][2]


@pytest.mark.parametrize("dag_id", sorted(EXPECTED))
def test_p3_5_scheduler_groups_queued_events_as_declared(dag_id):
    """The scheduler reads the serialized DAG, not the DAG file: the flag must
    survive the round trip (a timetable serialized without it, by Airflow
    3.3, is read back as batching), and the core timetable then groups the
    queued events: one run per event for read_file_dag, a single run with
    all of them for transform_dag."""
    from airflow.serialization.serialized_objects import DagSerialization

    batch = EXPECTED[dag_id][2]
    timetable = DagSerialization.from_dict(DagSerialization.to_dict(_dag(dag_id))).timetable
    assert timetable.batch_asset_events is batch

    events = [
        SimpleNamespace(id=i, timestamp=datetime(2026, 11, 21, 10, i, tzinfo=UTC)) for i in range(3)
    ]
    runs = list(timetable.group_asset_events(events, events[-1].timestamp))
    assert len(runs) == (1 if batch else 3)
    assert [event for _, group in runs for event in group] == events
