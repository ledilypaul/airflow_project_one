"""
P3-5 — Airflow 3.4: `batch_asset_events` is explicit on the Asset-scheduled DAG.

Airflow 3.4.0 (#68517) creates one DAG run per Asset event by default, where
3.3 put every queued event into one run. The default comes from
`[scheduler] batch_asset_events` (False) and is read when the timetable is
built, i.e. it depends on the deployment. transform_dag therefore sets it
itself: True, the `lake_staging__<source>` events queued while a `dbt build`
runs are consumed together by ONE next run (each run rebuilds the whole staging
zone: one build per event would only repeat the same work).

P5-3: `read_file_dag` (False, one run per listing) is gone: the `ingest__<source>`
DAGs list and read in one run, on their own schedule, not on an Asset. The
condition of transform_dag is the `AssetAny` of the sources' Assets.

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
from airflow.sdk import Asset, AssetAny, AssetTriggeredTimetable

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
DAGS_FOLDER = PROJECT_ROOT / "dags"
TRANSFORM_DAG_FILE = DAGS_FOLDER / "local_transform_dag.py"
# The sources of examples/config (see tests/unit/dags/conftest.py) that have a staging zone.
STAGING_ASSETS = {"lake_staging__imdb_movies", "lake_staging__orders"}

EXPECTED = {
    # dag_id: (DAG file, batch_asset_events)
    "transform_dag": (TRANSFORM_DAG_FILE, True),
}


def _dag(dag_id: str):
    dag_file = EXPECTED[dag_id][0]
    bag = DagBag(dag_folder=str(dag_file))
    assert not bag.import_errors, bag.import_errors
    return bag.dags[dag_id]


@pytest.mark.parametrize("dag_id", sorted(EXPECTED))
def test_p3_5_batch_asset_events_is_explicit(dag_id):
    _, batch = EXPECTED[dag_id]
    dag = _dag(dag_id)
    timetable = dag.timetable
    assert isinstance(timetable, AssetTriggeredTimetable), type(timetable)
    assert timetable.batch_asset_events is batch
    # Any of the sources' Assets, matched by name.
    assert timetable.asset_condition == AssetAny(*(Asset(name=n) for n in sorted(STAGING_ASSETS)))
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
    assert _dag(dag_id).timetable.batch_asset_events is EXPECTED[dag_id][1]


@pytest.mark.parametrize("dag_id", sorted(EXPECTED))
def test_p3_5_scheduler_groups_queued_events_as_declared(dag_id):
    """The scheduler reads the serialized DAG, not the DAG file: the flag must
    survive the round trip (a timetable serialized without it, by Airflow
    3.3, is read back as batching), and the core timetable then groups the
    queued events: a single run with all of them for transform_dag."""
    from airflow.serialization.serialized_objects import DagSerialization

    batch = EXPECTED[dag_id][1]
    timetable = DagSerialization.from_dict(DagSerialization.to_dict(_dag(dag_id))).timetable
    assert timetable.batch_asset_events is batch

    events = [
        SimpleNamespace(id=i, timestamp=datetime(2026, 11, 21, 10, i, tzinfo=UTC)) for i in range(3)
    ]
    runs = list(timetable.group_asset_events(events, events[-1].timestamp))
    assert len(runs) == (1 if batch else 3)
    assert [event for _, group in runs for event in group] == events
