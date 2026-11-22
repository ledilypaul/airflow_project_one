"""
P1-5 / C-1 — parent watchdog of the Spark job's Python driver.

In local/client mode, `spark-submit` (exec'd into a JVM) starts the Python
driver `read_files_job.py` as its child. When Airflow kills the task,
`SparkSubmitHook.on_kill` sends SIGKILL to the `spark-submit` PID only: the
JVM dies and, without a guard, the Python driver keeps running, re-parented,
outside Airflow's control (pool slot released, `failed` statuses written for
the remaining files concurrently with a retry or after a "mark success").

`start_parent_watchdog` makes the driver exit on its own, with the dedicated
code `EXIT_PARENT_GONE`, as soon as its parent process is gone. These tests
run it in real processes: no mock of `os.getppid` / `os._exit`.
"""

import json
import subprocess
import sys
import textwrap

import pytest

from pipeline.jobs import read_files_job
from pipeline.jobs.read_files_job import EXIT_PARENT_GONE, main

# Short poll interval: the orphaned child must exit well within a second.
INTERVAL = "0.05"
# Upper bound for the watchdog to fire once the parent is dead. Generous on
# purpose (slow CI runner): the expected delay is ~INTERVAL.
DEADLINE_S = 10.0

# The process under test: starts the *real* watchdog, then reports its pid
# (on stdout, only once the watchdog runs, so the parent's pid it recorded is
# the right one) and sleeps. Without a working watchdog it outlives the test
# (and is killed by the reaper below after DEADLINE_S).
CHILD_SCRIPT = textwrap.dedent(
    """
    import os
    import sys
    import time

    from pipeline.jobs.read_files_job import start_parent_watchdog

    start_parent_watchdog(interval=float(sys.argv[1]))
    print(os.getpid(), flush=True)
    time.sleep(float(sys.argv[2]))
    """
)

# The parent (stand-in for the spark-submit JVM): starts the child, then
# sleeps until it is SIGKILLed. The child inherits its stdout.
PARENT_SCRIPT = textwrap.dedent(
    """
    import subprocess
    import sys
    import time

    subprocess.Popen([sys.executable, "-c", sys.argv[1], sys.argv[2], "60"])
    time.sleep(60)
    """
)

# Run by the test in its own process: becomes a child subreaper
# (PR_SET_CHILD_SUBREAPER, Linux) so that the orphaned child is re-parented to
# it and its exit status can be collected with waitpid(). Doing this in a
# dedicated process keeps the pytest process itself untouched.
REAPER_SCRIPT = textwrap.dedent(
    """
    import ctypes
    import json
    import os
    import signal
    import subprocess
    import sys
    import time

    PR_SET_CHILD_SUBREAPER = 36
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "prctl(PR_SET_CHILD_SUBREAPER)")

    parent_script, child_script, interval, deadline = sys.argv[1:5]
    # The child inherits the parent's stdout/stderr: one pipe for both, so
    # its log lines are collected whatever stream logging writes to.
    parent = subprocess.Popen(
        [sys.executable, "-c", parent_script, child_script, interval],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        # The job module may log warnings on import: the pid is the line
        # printed once the watchdog runs.
        line = parent.stdout.readline()
        output = []
        while line and not line.strip().isdigit():
            output.append(line)
            line = parent.stdout.readline()
        child_pid = int(line)
    finally:
        os.kill(parent.pid, signal.SIGKILL)  # what SparkSubmitHook.on_kill does
        parent.wait()
    # The kernel re-parents the child to this subreaper when the parent exits.
    killed_at = time.monotonic()
    exit_code = None
    while True:
        pid, status = os.waitpid(child_pid, os.WNOHANG)
        if pid:
            exit_code = os.waitstatus_to_exitcode(status)
            break
        if time.monotonic() - killed_at > float(deadline):
            os.kill(child_pid, signal.SIGKILL)  # watchdog did not fire
            os.waitpid(child_pid, 0)
            break
        time.sleep(0.01)
    elapsed = time.monotonic() - killed_at
    output.append(parent.stdout.read())  # EOF: parent and child are both gone
    print(json.dumps({"exit_code": exit_code, "elapsed": elapsed, "output": "".join(output)}))
    """
)


def test_p1_5_exit_parent_gone_code_is_dedicated():
    """4: distinct from 0 = all processed, 1 = file failure(s), 2 = bad
    --run-id, 3 = no file_list row, and documented in the job's docstring."""
    assert EXIT_PARENT_GONE == 4
    assert "4 = " in read_files_job.__doc__


@pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="PR_SET_CHILD_SUBREAPER is Linux-only"
)
def test_p1_5_watchdog_exits_when_parent_is_killed():
    """C-1 regression: SIGKILL on the parent (the spark-submit JVM) must make
    the orphaned driver exit by itself, with EXIT_PARENT_GONE, quickly."""
    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            REAPER_SCRIPT,
            PARENT_SCRIPT,
            CHILD_SCRIPT,
            INTERVAL,
            str(DEADLINE_S),
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, proc.stderr[-5000:]
    result = json.loads(proc.stdout.splitlines()[-1])

    assert result["exit_code"] == EXIT_PARENT_GONE, result
    assert result["elapsed"] < 5, result
    # The reason is logged (stdout or stderr of the child, merged by the reaper).
    assert "parent process" in result["output"] and "is gone" in result["output"], result


def test_p1_5_watchdog_does_nothing_while_parent_is_alive():
    """With its parent (here: pytest) alive, the watchdog polls many times
    and never exits the process: the child ends normally, with code 0. This
    also proves the thread is a daemon: otherwise its endless loop would keep
    the process alive after the main thread returns (timeout)."""
    proc = subprocess.run(
        [sys.executable, "-c", CHILD_SCRIPT, "0.02", "0.5"],  # ~25 checks
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert proc.returncode == 0, proc.stdout[-5000:] + proc.stderr[-5000:]
    assert "is gone" not in proc.stdout + proc.stderr


def test_p1_5_main_starts_parent_watchdog_first(parent_watchdog_calls):
    """main() starts the watchdog before anything else, even on the early
    exit for an invalid --run-id (tests/unit/jobs/conftest.py records the
    call instead of starting a thread in the pytest process)."""
    with pytest.raises(SystemExit) as exc:
        main(run_id=None)

    assert exc.value.code == 2
    assert parent_watchdog_calls == [{}]
