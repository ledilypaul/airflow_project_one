import fnmatch
from pathlib import Path

import yaml


class Extractor:
    def __init__(self, config_path: str):
        with open(config_path) as file:
            config = yaml.safe_load(file)
        self.config = config.get("extractor_config", {})

    def list_files(self):
        """List the files of the configured source directories.

        Raises:
            FileNotFoundError: if at least one configured source directory does
                not exist. All missing directories are reported, each with its
                configured and resolved form (relative paths are resolved
                against the current working directory). A missing directory is
                a deployment problem and must not be mistaken for an empty one,
                which legitimately yields ``[]`` (and a downstream skip).
        """
        directories = self.config.get("source_directories", [])

        # Existence check done once per directory, before any globbing.
        missing = [d for d in directories if not Path(d).exists()]
        if missing:
            details = "\n".join(f"  - {d!s} (resolved: {Path(d).resolve()!s})" for d in missing)
            raise FileNotFoundError(
                f"{len(missing)} configured source director"
                f"{'y does' if len(missing) == 1 else 'ies do'} not exist "
                f"(cwd: {Path.cwd()!s}):\n{details}"
            )

        files = []
        for directory in directories:
            dir_path = Path(directory)
            for ext in self.config.get("files_extensions", []):
                matches = list(dir_path.glob(f"*{ext}"))

                # Apply inclusions
                include_patterns = self.config.get("include_files", ["*"])
                matches = [
                    f
                    for f in matches
                    if any(fnmatch.fnmatch(f.name, pattern) for pattern in include_patterns)
                ]

                # Apply exclusions
                exclude_patterns = self.config.get("exclude_files", [])
                matches = [
                    f
                    for f in matches
                    if not any(fnmatch.fnmatch(f.name, pattern) for pattern in exclude_patterns)
                ]

                files.extend(matches)
        return [str(f) for f in files]

    def check_integrity_files(self, files):
        valid_files = []
        for file in files:
            if Path(file).stat().st_size == 0:
                raise ValueError(f"File {file} is empty")
            valid_files.append(file)
        return valid_files
