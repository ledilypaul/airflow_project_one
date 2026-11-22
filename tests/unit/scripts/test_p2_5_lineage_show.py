"""
P2-5 — scripts/lineage_show.py (`make lineage-show`): summary of the
OpenLineage events of the dev `file` transport.

Events are written as one JSON file per Airflow event (`airflow-<ts>.json`)
and one JSON line per Spark event (`spark.jsonl`). The summary lists each
job with its last state, its parent and its input and output datasets, and
the edges dataset -> job -> dataset; a broken line is reported, never fatal.
Pure stdlib: never imports Airflow.
"""

import importlib.util
import json
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "lineage_show.py"


@pytest.fixture(scope="module")
def lineage_show():
    spec = importlib.util.spec_from_file_location("lineage_show", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _event(job, event_type, time, *, run="r1", inputs=(), outputs=(), parent=None, ns="dev"):
    event = {
        "eventTime": time,
        "eventType": event_type,
        "job": {"namespace": ns, "name": job},
        "run": {"runId": run, "facets": {}},
        "inputs": [{"namespace": n, "name": d} for n, d in inputs],
        "outputs": [{"namespace": n, "name": d} for n, d in outputs],
    }
    if parent:
        event["run"]["facets"]["parent"] = {
            "job": {"namespace": ns, "name": parent},
            "run": {"runId": "p"},
        }
    return event


FILE_LIST = ("postgres://localhost:5434", "files.public.file_list")
CSV = ("file", "/data/imdb.csv")
STAGING = ("file", "/lake/staging/movies")
DBT_SOURCE = ("duckdb:///w.duckdb", "w.lake_staging.movies")


@pytest.fixture
def events_dir(tmp_path):
    folder = tmp_path / "events"
    folder.mkdir()
    airflow = [
        _event("list_files_dag.insert_files", "START", "2026-11-14T10:00:00Z"),
        _event(
            "list_files_dag.insert_files",
            "COMPLETE",
            "2026-11-14T10:00:01Z",
            inputs=[CSV],
            outputs=[FILE_LIST],
        ),
        _event(
            "read_file_dag.spark_read_files", "FAIL", "2026-11-14T10:01:00Z", inputs=[FILE_LIST]
        ),
        _event(
            "transform_dag.lineage_lake_staging",
            "COMPLETE",
            "2026-11-14T10:02:00Z",
            inputs=[STAGING],
            outputs=[DBT_SOURCE],
        ),
    ]
    for i, event in enumerate(airflow):
        (folder / f"airflow-2026111{i}.json").write_text(json.dumps(event))
    spark = [
        _event("job.collect_limit", "COMPLETE", "2026-11-14T10:00:30Z", run="s1", inputs=[CSV]),
        _event(
            "job.write_staging",
            "COMPLETE",
            "2026-11-14T10:00:40Z",
            run="s2",
            inputs=[CSV],
            outputs=[STAGING],
            parent="job",
        ),
    ]
    lines = [json.dumps(e) for e in spark] + ["{not json"]
    (folder / "spark.jsonl").write_text("\n".join(lines) + "\n")
    return folder


def test_p2_5_summary_lists_jobs_with_their_datasets(lineage_show, events_dir, capsys):
    assert lineage_show.main([str(events_dir)]) == 0
    out = capsys.readouterr().out

    assert "list_files_dag.insert_files" in out
    assert "files.public.file_list" in out
    # Last state of each job, by event time (not file order).
    assert "read_file_dag.spark_read_files  FAIL" in out
    assert "list_files_dag.insert_files  COMPLETE" in out
    assert "parent: dev/job" in out
    # The bridge: staging -> dbt source.
    assert "transform_dag.lineage_lake_staging" in out
    assert "6 event(s)" in out


def test_p2_5_jobs_without_datasets_only_with_all(lineage_show, events_dir, capsys):
    """Spark emits many jobs without any dataset (application, plans that
    read nothing): hidden by default, listed with --all."""
    (events_dir / "airflow-x.json").write_text(
        json.dumps(_event("job", "COMPLETE", "2026-11-14T10:00:50Z", run="app"))
    )
    lineage_show.main([str(events_dir)])
    assert "\njob  COMPLETE" not in capsys.readouterr().out
    lineage_show.main([str(events_dir), "--all"])
    assert "\njob  COMPLETE" in capsys.readouterr().out


def test_p2_5_summary_prints_the_edges(lineage_show, events_dir, capsys):
    lineage_show.main([str(events_dir)])
    out = capsys.readouterr().out
    assert "file /data/imdb.csv -> list_files_dag.insert_files" in out
    assert "list_files_dag.insert_files -> postgres://localhost:5434 files.public.file_list" in out
    assert "file /lake/staging/movies -> transform_dag.lineage_lake_staging" in out


def test_p2_5_broken_lines_are_reported_not_fatal(lineage_show, events_dir, capsys):
    assert lineage_show.main([str(events_dir)]) == 0
    captured = capsys.readouterr()
    assert "1 unreadable" in captured.err
    assert "spark.jsonl:3" in captured.err


def test_p2_5_missing_or_empty_directory(lineage_show, tmp_path, capsys):
    assert lineage_show.main([str(tmp_path / "nowhere")]) == 1
    assert "make airflow" in capsys.readouterr().err
    (tmp_path / "empty").mkdir()
    assert lineage_show.main([str(tmp_path / "empty")]) == 0
    assert "no OpenLineage event" in capsys.readouterr().out


def test_p2_5_script_imports_no_airflow():
    top_level = [
        line for line in SCRIPT.read_text().splitlines() if line.startswith(("import ", "from "))
    ]
    assert not [line for line in top_level if "airflow" in line or "openlineage" in line]


# ---------------------------------------------------------------------------
# C-8 — malformed input never breaks the summary
# ---------------------------------------------------------------------------


def test_p2_5_c8_malformed_entries_are_skipped_and_counted(lineage_show, tmp_path, capsys):
    folder = tmp_path / "events"
    folder.mkdir()
    good = _event("ok.job", "COMPLETE", "2026-11-14T10:00:00Z", inputs=[CSV], outputs=[STAGING])
    (folder / "airflow-good.json").write_text(json.dumps(good))
    (folder / "airflow-latin1.json").write_bytes('{"x": "\xe9"}'.encode("latin-1"))
    (folder / "airflow-dir.json").mkdir()  # a directory named like an event file
    bad_run = {**good, "run": "not a dict"}
    bad_inputs = {**good, "job": {"namespace": "ns", "name": "int.inputs"}, "inputs": 42}
    null_ns = {
        **good,
        "job": {"namespace": None, "name": 7},
        "outputs": [{"namespace": None, "name": None}, "not a dataset"],
    }
    lines = [json.dumps(e) for e in (bad_run, bad_inputs, null_ns)] + ["[1, 2]", "{bad"]
    (folder / "spark.jsonl").write_bytes(
        ("\n".join(lines) + "\n").encode() + b"\xff\xfe broken bytes\n"
    )

    assert lineage_show.main([str(folder)]) == 0
    captured = capsys.readouterr()
    assert "ok.job" in captured.out
    # The tolerable ones are kept, with their odd fields shown as text.
    assert "int.inputs" in captured.out
    assert "7  COMPLETE" in captured.out
    # Ignored: latin-1 file, directory, `[1, 2]`, `{bad`, the non-UTF-8 line.
    assert "5 unreadable" in captured.err


def test_p2_5_c8_reads_jsonl_line_by_line(lineage_show, tmp_path, monkeypatch):
    """Streaming: a .jsonl file is never read whole."""
    from pathlib import Path as _Path

    folder = tmp_path / "events"
    folder.mkdir()
    event = _event("j", "COMPLETE", "2026-11-14T10:00:00Z", inputs=[CSV])
    (folder / "spark.jsonl").write_text(json.dumps(event) + "\n")

    def forbidden(self, *args, **kwargs):
        raise AssertionError(f"read_text on {self}")

    monkeypatch.setattr(_Path, "read_text", forbidden)
    events, problems = lineage_show.read_events(folder)
    assert len(events) == 1 and problems == []
