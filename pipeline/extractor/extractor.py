from pathlib import Path
import yaml
import fnmatch

class Extractor:
    def __init__(self, config_path: str):
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        self.config = config.get("extractor_config", {})
    
    def list_files(self):
        files = []
        for directory in self.config.get("source_directories", []):
            for ext in self.config.get("files_extensions", []):
                dir_path = Path(directory)
                if not dir_path.exists():
                    continue
                matches = list(dir_path.glob(f"*{ext}"))

                # Apply inclusions
                include_patterns = self.config.get("include_files", ["*"])
                matches = [
                    f for f in matches
                    if any(fnmatch.fnmatch(f.name, pattern) for pattern in include_patterns)
                ]

                # Apply exclusions
                exclude_patterns = self.config.get("exclude_files", [])
                matches = [
                    f for f in matches
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
