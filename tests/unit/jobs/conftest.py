import pytest


@pytest.fixture(autouse=True)
def parent_watchdog_calls(monkeypatch):
    """P1-5 / C-1: `main()` starts a parent watchdog thread that calls
    `os._exit` when its parent process disappears. These tests call `main()`
    inside the pytest process: replace it with a recorder so that no watchdog
    thread is left polling (or able to kill pytest) after each test.

    The real watchdog is exercised in separate processes by
    test_p1_5_parent_watchdog.py. Returns the list of recorded calls
    (one `kwargs` dict per call)."""
    calls: list[dict] = []
    monkeypatch.setattr(
        "pipeline.jobs.read_files_job.start_parent_watchdog",
        lambda **kwargs: calls.append(kwargs),
    )
    return calls
