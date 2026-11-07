"""Hook PostToolUse : formate et corrige avec ruff chaque fichier Python modifié par Claude.

Cherche ruff d'abord dans le .venv du projet (créé par `uv sync`), puis dans le PATH.
Ne bloque jamais : si ruff est introuvable ou échoue, le hook sort en 0 sans rien faire.
"""
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path


def find_ruff() -> str | None:
    project = Path(os.environ.get("CLAUDE_PROJECT_DIR", "."))
    for candidate in (project / ".venv" / "bin" / "ruff", project / ".venv" / "Scripts" / "ruff.exe"):
        if candidate.is_file():
            return str(candidate)
    return shutil.which("ruff")


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        return 0

    path = (payload.get("tool_input") or {}).get("file_path", "")
    if not path.endswith(".py"):
        return 0

    ruff = find_ruff()
    if ruff is None:
        return 0

    for args in (["format", path], ["check", "--fix", "--quiet", path]):
        try:
            subprocess.run([ruff, *args], capture_output=True, timeout=25, check=False)
        except (OSError, subprocess.TimeoutExpired):
            return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
