"""Print the OpenLineage transport of the Airflow targets, as JSON (P2-5).

Called by ``make airflow`` (and ``make lineage-transport``) to build
``AIRFLOW__OPENLINEAGE__TRANSPORT``. The values come from the environment,
never from text interpolated into JSON or into a shell command: a ``"`` or a
backslash in a path or a URL can neither break nor inject into it.

- ``OPENLINEAGE_URL`` set: ``{"type": "http", "url": <it>}``;
- otherwise: ``{"type": "file", "log_file_path": <LINEAGE_EVENTS_DIR>/airflow,
  "append": false}``, i.e. one JSON file per event.

Standard library only.
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Mapping


def transport(environ: Mapping[str, str]) -> dict:
    url = (environ.get("OPENLINEAGE_URL") or "").strip()
    if url:
        return {"type": "http", "url": url}
    events_dir = (environ.get("LINEAGE_EVENTS_DIR") or "").strip()
    if not events_dir:
        raise SystemExit("error: LINEAGE_EVENTS_DIR is not set (the Makefile sets it)")
    return {"type": "file", "log_file_path": f"{events_dir}/airflow", "append": False}


def main() -> int:
    print(json.dumps(transport(os.environ)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
