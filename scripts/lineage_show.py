"""Summarise the OpenLineage events of the dev `file` transport (P2-5).

Called by ``make lineage-show``. Reads, in an events directory
(``lineage/events`` by default):

- ``*.json``: one event per file (Airflow, ``airflow-<timestamp>.json``);
- ``*.jsonl``: one event per line (Spark, ``spark-<application>.jsonl``),
  read line by line.

Prints, for every job that read or wrote a dataset (``--all``: every job),
its last state, its number of runs, its parent job, and its input and
output datasets; then the edges ``dataset -> job -> dataset``, i.e. the
answer to "where does this table come from, and what produced it?".

Never fails on its input: an entry that cannot be read (I/O error, not
UTF-8, not JSON, not a JSON object with a ``job`` object) is counted and
named on stderr, then skipped. Fields of the wrong type are tolerated:
names are shown as text, a ``run`` or a dataset list that is not what the
spec says is ignored. Memory grows with the number of jobs and datasets,
not with the number of events.

Standard library only: it never imports Airflow or OpenLineage.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_EVENTS_DIR = Path(__file__).resolve().parent.parent / "lineage" / "events"


@dataclass
class Job:
    namespace: str
    name: str
    runs: set[str] = field(default_factory=set)
    last_time: str = ""
    last_state: str = "?"
    parent: str | None = None
    inputs: set[tuple[str, str]] = field(default_factory=set)
    outputs: set[tuple[str, str]] = field(default_factory=set)


def _event(raw: bytes) -> dict | None:
    """The event in `raw`, or None when it is not one."""
    try:
        event = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None
    if isinstance(event, dict) and isinstance(event.get("job"), dict):
        return event
    return None


def iter_events(folder: Path, problems: list[str]) -> Iterator[dict]:
    """Every event of the directory; where the unreadable ones are goes to
    `problems`."""
    for path in sorted(folder.glob("*.json")):
        try:
            raw = path.read_bytes()
        except OSError:
            problems.append(path.name)
            continue
        event = _event(raw)
        if event is None:
            problems.append(path.name)
        else:
            yield event
    for path in sorted(folder.glob("*.jsonl")):
        try:
            with path.open("rb") as stream:
                for number, line in enumerate(stream, start=1):
                    if not line.strip():
                        continue
                    event = _event(line)
                    if event is None:
                        problems.append(f"{path.name}:{number}")
                    else:
                        yield event
        except OSError:
            problems.append(path.name)


def read_events(folder: Path) -> tuple[list[dict], list[str]]:
    problems: list[str] = []
    return list(iter_events(folder, problems)), problems


def _text(value: object) -> str:
    return "?" if value is None else str(value)


def _datasets(event: dict, key: str) -> set[tuple[str, str]]:
    items = event.get(key)
    if not isinstance(items, list):
        return set()
    return {(_text(d.get("namespace")), _text(d.get("name"))) for d in items if isinstance(d, dict)}


def summarise(events: Iterator[dict] | list[dict]) -> tuple[dict[tuple[str, str], Job], int]:
    """Jobs by (namespace, name), and the number of events read. The last
    state of a job is the one of its latest event (`eventTime`), whatever
    the order of the files."""
    jobs: dict[tuple[str, str], Job] = {}
    count = 0
    for event in events:
        count += 1
        key = (_text(event["job"].get("namespace")), _text(event["job"].get("name")))
        job = jobs.setdefault(key, Job(*key))
        run = event.get("run") if isinstance(event.get("run"), dict) else {}
        if run.get("runId"):
            job.runs.add(_text(run["runId"]))
        time = _text(event.get("eventTime", ""))
        if time >= job.last_time:
            job.last_time = time
            job.last_state = _text(event.get("eventType"))
        facets = run.get("facets") if isinstance(run.get("facets"), dict) else {}
        parent = facets.get("parent") if isinstance(facets.get("parent"), dict) else {}
        parent_job = parent.get("job") if isinstance(parent.get("job"), dict) else {}
        if parent_job.get("name"):
            job.parent = f"{_text(parent_job.get('namespace'))}/{_text(parent_job['name'])}"
        job.inputs |= _datasets(event, "inputs")
        job.outputs |= _datasets(event, "outputs")
    return jobs, count


def _show(dataset: tuple[str, str]) -> str:
    return f"{dataset[0]} {dataset[1]}"


def render(jobs: dict[tuple[str, str], Job], *, show_all: bool) -> list[str]:
    shown = [j for j in jobs.values() if show_all or j.inputs or j.outputs]
    lines = []
    for job in sorted(shown, key=lambda j: (j.name, j.namespace)):
        lines.append(f"{job.name}  {job.last_state}  ({len(job.runs)} run(s), {job.namespace})")
        if job.parent:
            lines.append(f"  parent: {job.parent}")
        lines += [f"  in : {_show(d)}" for d in sorted(job.inputs)]
        lines += [f"  out: {_show(d)}" for d in sorted(job.outputs)]
        lines.append("")
    edges = sorted(
        {f"{_show(d)} -> {j.name}" for j in shown for d in j.inputs}
        | {f"{j.name} -> {_show(d)}" for j in shown for d in j.outputs}
    )
    if edges:
        lines.append("Edges (dataset -> job -> dataset):")
        lines += [f"  {edge}" for edge in edges]
    return lines


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("events_dir", nargs="?", default=str(DEFAULT_EVENTS_DIR))
    parser.add_argument("--all", action="store_true", help="also list the jobs without any dataset")
    args = parser.parse_args(argv)

    folder = Path(args.events_dir)
    if not folder.is_dir():
        print(
            f"error: {folder} does not exist. Events are written there by the Airflow "
            "targets (`make airflow`, default `file` transport).",
            file=sys.stderr,
        )
        return 1
    problems: list[str] = []
    jobs, count = summarise(iter_events(folder, problems))
    if problems:
        print(
            f"warning: {len(problems)} unreadable event(s) skipped: {', '.join(problems)}",
            file=sys.stderr,
        )
    if not count:
        print(f"no OpenLineage event in {folder}")
        return 0

    shown = sum(1 for j in jobs.values() if args.all or j.inputs or j.outputs)
    print(f"{shown} job(s) out of {len(jobs)}, from {count} event(s) in {folder}\n")
    print("\n".join(render(jobs, show_all=args.all)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
