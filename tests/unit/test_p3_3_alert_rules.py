"""
P3-3 - structure of the Prometheus alert rules (deploy/observability/rules/).

These checks need neither Prometheus nor Docker. The behaviour of the rules
(when they fire, when they do not) is tested by `promtool test rules`:
tests/integration/test_p3_3_promtool.py.

What is guarded here:
- every alert has what a person on call needs (severity, summary, description);
- every metric a rule uses is a metric the installed Airflow really emits,
  under the name Prometheus gets it through the collector's exporter. An
  Airflow upgrade that renames a metric breaks this test, not the alerting.
"""

import re
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RULES_FILE = PROJECT_ROOT / "deploy" / "observability" / "rules" / "airflow.yml"
UNIT_TESTS_FILE = PROJECT_ROOT / "deploy" / "observability" / "tests" / "airflow_rules_test.yml"
EXPECTED_ALERTS = {
    "AirflowDagRunFailed",
    "AirflowSchedulerHeartbeatMissing",
    "AirflowTaskStuckScheduled",
}
SEVERITIES = {"critical", "warning"}


def _rules() -> list[dict]:
    groups = yaml.safe_load(RULES_FILE.read_text())["groups"]
    return [rule for group in groups for rule in group["rules"]]


def _airflow_metrics() -> dict[str, str]:
    """Prometheus name -> Airflow metric type, for every metric of the
    installed Airflow's registry (metrics_template.yaml), named as the
    OpenTelemetry collector's Prometheus exporter does: the prefix `airflow.`,
    dots to underscores, `_total` for a counter, `_milliseconds` and a
    `_count` / `_sum` / `_bucket` series for a timer (a histogram in ms)."""
    import airflow

    template = (
        Path(airflow.__file__).parent
        / "_shared"
        / "observability"
        / "metrics"
        / "metrics_template.yaml"
    )
    assert template.is_file(), f"{template}: Airflow moved its metrics registry, update this test"
    registry = yaml.safe_load(template.read_text())["metrics"]
    names: dict[str, str] = {}
    for metric in registry:
        base = "airflow_" + re.sub(r"[.\-]", "_", metric["name"])
        kind = metric["type"]
        if kind == "counter":
            names[f"{base}_total"] = kind
        elif kind == "timer":
            for suffix in ("count", "sum", "bucket"):
                names[f"{base}_milliseconds_{suffix}"] = kind
        else:
            names[base] = kind
    return names


def _metric_names(expr: str) -> set[str]:
    """The `airflow_*` series an expression selects (label matchers, such as
    job="airflow_project_one", are not series names)."""
    return set(re.findall(r"\bairflow_\w+", re.sub(r"\{[^}]*\}", "", expr)))


def test_p3_3_the_three_documented_signals_are_there():
    assert {rule["alert"] for rule in _rules()} == EXPECTED_ALERTS


@pytest.mark.parametrize("rule", _rules(), ids=lambda r: r["alert"])
def test_p3_3_every_alert_is_actionable(rule):
    assert rule["labels"]["severity"] in SEVERITIES
    annotations = rule["annotations"]
    assert annotations["summary"].strip()
    # A description that says where to look next, not only what happened.
    assert len(annotations["description"].split()) >= 15
    assert rule["expr"].strip()


@pytest.mark.parametrize("rule", _rules(), ids=lambda r: r["alert"])
def test_p3_3_templates_only_use_labels_the_expression_keeps(rule):
    """`{{ $labels.dag_id }}` in an alert whose expression aggregates the
    label away renders as an empty string."""
    used = set(re.findall(r"\$labels\.(\w+)", str(rule["annotations"])))
    expr = rule["expr"]
    kept = set(re.findall(r"by \(([\w, ]+)\)", expr))
    kept = {label.strip() for group in kept for label in group.split(",")}
    if "sum(" in expr and "by (" not in expr:
        kept = set()  # a bare sum() keeps no label
    if re.search(r"^\s*\(?airflow_\w+(\{[^}]*\})?\s*>", expr):
        kept |= {"dag_id", "task_id", "queue"}  # a selector keeps all its labels
    assert used <= kept, f"{rule['alert']}: {sorted(used - kept)} are not kept by the expression"


def test_p3_3_every_metric_used_exists_in_airflow():
    known = _airflow_metrics()
    used = {name for rule in _rules() for name in _metric_names(rule["expr"])}

    assert used, "no Airflow metric in the rules"
    unknown = sorted(used - set(known))
    assert not unknown, f"not emitted by the installed Airflow (renamed?): {unknown}"


def test_p3_3_metric_names_follow_what_prometheus_really_received():
    """Names seen in Prometheus on 2026-11-17 (Airflow 3.3.2, collector
    0.165.0), through the same mapping as `_airflow_metrics`: a guard on the
    mapping itself, so that `test_p3_3_every_metric_used_exists_in_airflow`
    keeps meaning something."""
    known = _airflow_metrics()
    for observed, kind in {
        "airflow_scheduler_heartbeat_total": "counter",
        "airflow_ti_failures_total": "counter",
        "airflow_ti_scheduled": "gauge",
        "airflow_ti_running": "gauge",
        "airflow_pool_scheduled_slots": "gauge",
        "airflow_dagrun_duration_failed_milliseconds_count": "timer",
        "airflow_dagrun_duration_failed_milliseconds_bucket": "timer",
    }.items():
        assert known.get(observed) == kind, observed


def test_p3_3_the_rules_have_unit_tests_for_each_alert():
    tests = yaml.safe_load(UNIT_TESTS_FILE.read_text())["tests"]
    covered = {
        check["alertname"]
        for test in tests
        for check in test["alert_rule_test"]
        if check.get("exp_alerts")
    }
    assert covered == EXPECTED_ALERTS, "each alert needs a case where it fires"
    # And a case where it must stay silent.
    silent = {
        check["alertname"]
        for test in tests
        for check in test["alert_rule_test"]
        if not check.get("exp_alerts")
    }
    assert silent == EXPECTED_ALERTS, "each alert needs a case where it must not fire"


def test_p3_3_unit_tests_follow_the_rules_file():
    rules = yaml.safe_load(UNIT_TESTS_FILE.read_text())["rule_files"]
    assert (UNIT_TESTS_FILE.parent / rules[0]).resolve() == RULES_FILE.resolve()


def _makefile_service_name() -> str:
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    namespace = re.search(r"^OPENLINEAGE_NAMESPACE \?= (\S+)$", makefile, re.MULTILINE)
    assert namespace and "OTEL_SERVICE_NAME ?= $(OPENLINEAGE_NAMESPACE)" in makefile
    return namespace.group(1)


def test_p3_3_sec4_every_airflow_series_is_filtered_on_the_service_name():
    """The collector takes OTLP from anyone who can reach it: another service's
    series (same names) must not fire these alerts nor hide a dead scheduler.
    The job is the OTEL_SERVICE_NAME `make airflow` sets, kept by the scrape."""
    service = _makefile_service_name()
    assert service == "airflow_project_one"
    for rule in _rules():
        selected = list(re.finditer(r"\b(airflow_[a-z_]+)(\{[^}]*\})?", rule["expr"]))
        for match in selected:
            name, matchers = match.group(1), match.group(2) or ""
            if name == service:  # the job's own value, inside a matcher
                continue
            assert f'job="{service}"' in matchers, f"{rule['alert']}: {name} is not filtered"
        assert any(m.group(1) != service for m in selected), rule["alert"]


def test_p3_3_the_scrape_gate_uses_the_collector_scrape_job():
    """AirflowDagRunFailed's gate reads `up` of the scrape job of prometheus.yml."""
    prometheus = yaml.safe_load((RULES_FILE.parent.parent / "prometheus.yml").read_text())
    (job,) = prometheus["scrape_configs"]
    dag_rule = next(r for r in _rules() if r["alert"] == "AirflowDagRunFailed")
    assert f'up{{job="{job["job_name"]}"}}' in dag_rule["expr"]
    assert prometheus["global"]["scrape_interval"] == "30s"  # the 28-of-32 count assumes it


def test_p3_3_c4_stuck_scheduled_names_both_causes():
    """A missing pool and a paused DAG: the two reasons to wait forever."""
    rule = next(r for r in _rules() if r["alert"] == "AirflowTaskStuckScheduled")
    description = rule["annotations"]["description"]
    assert "pool" in description and "paused" in description
