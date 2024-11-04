import os
import sys
import yaml
import glob

class Extractor:
    def __init__(self, config_path : str):
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        self.config = config["extractor_config"]
            
    def list_files(self):
        files = []
        for directory in self.config.get("source_directories", []):
            for extension in self.config.get("files_extensions", []):
                pattern = os.path.join(directory, f"*{extension}")
                matching_files = glob.glob(pattern)            
                
                include_patterns = self.config.get("include_files", [])
                if include_patterns:
                    matching_files = [
                        f for f in matching_files 
                        if any(glob.fnmatch.fnmatch(os.path.basename(f), include_pattern) for include_pattern in include_patterns)
                    ]
                files.extend(matching_files)
                            

        return files if files else []
    
    def check_integrity_files(self,files):
        valid_files = []
        for file in files:
            if os.path.getsize(file) == 0:
                raise ValueError("File {file} is empty")
            else:
                valid_files.append(file)
        return valid_files
    
        