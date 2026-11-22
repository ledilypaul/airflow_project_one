"""Tell whether the pinned openlineage-spark jar has a newer release (P3-4).

Dependabot cannot follow the jar: it is pinned by two Makefile variables
(``OPENLINEAGE_SPARK_VERSION`` and ``OPENLINEAGE_SPARK_SHA256``), and no Dependabot
ecosystem reads a Makefile. ``.github/workflows/openlineage-jar-watch.yml`` runs
this script every week and opens an issue when it reports a newer release.

It only reads: the Makefile and the public ``maven-metadata.xml`` of the artifact on
Maven Central (HTTPS). It never edits the pin, never downloads the jar: the digest of
a new version is a human decision (see the provenance comment in the Makefile).

Prints one JSON object: ``{"pinned": "1.55.0", "latest": "1.56.0", "outdated": true}``.
Only plain releases (``X.Y.Z``) count: a snapshot or a release candidate never
raises the alarm. Exit code 0 when the check ran (outdated or not), 1 when it could
not (Makefile without the pin, Maven Central unreachable or unreadable), so that a
broken check is visible in the workflow instead of passing as "up to date".

Standard library only.
"""

from __future__ import annotations

import json
import re
import sys
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MAKEFILE = PROJECT_ROOT / "Makefile"
METADATA_URL = (
    "https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.13/maven-metadata.xml"
)
PIN = re.compile(r"^OPENLINEAGE_SPARK_VERSION\s*:?=\s*(\S+)\s*$", re.MULTILINE)
PLAIN_RELEASE = re.compile(r"^\d+\.\d+\.\d+$")


def pinned_version(makefile_text: str) -> str:
    match = PIN.search(makefile_text)
    if not match:
        raise ValueError("OPENLINEAGE_SPARK_VERSION is not set in the Makefile")
    return match.group(1)


def release_key(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))


def latest_release(metadata_xml: str) -> str:
    """Highest plain release listed in a Maven ``maven-metadata.xml``."""
    versions = [(node.text or "").strip() for node in ET.fromstring(metadata_xml).iter("version")]
    releases = [v for v in versions if PLAIN_RELEASE.fullmatch(v)]
    if not releases:
        raise ValueError("no plain release found in maven-metadata.xml")
    return max(releases, key=release_key)


def check(makefile_text: str, metadata_xml: str) -> dict:
    pinned = pinned_version(makefile_text)
    if not PLAIN_RELEASE.fullmatch(pinned):
        raise ValueError(f"pinned version {pinned!r} is not a plain X.Y.Z release")
    latest = latest_release(metadata_xml)
    return {
        "pinned": pinned,
        "latest": latest,
        "outdated": release_key(latest) > release_key(pinned),
    }


def fetch_metadata(url: str = METADATA_URL) -> str:
    if not url.startswith("https://"):
        raise ValueError("maven metadata must be fetched over HTTPS")
    with urllib.request.urlopen(url, timeout=30) as response:  # noqa: S310 (https only)
        return response.read().decode("utf-8")


def main() -> int:
    try:
        result = check(MAKEFILE.read_text(encoding="utf-8"), fetch_metadata())
    except (OSError, ValueError, ET.ParseError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
