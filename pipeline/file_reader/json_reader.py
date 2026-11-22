from .base_reader import BaseFileReader


class JSONReader(BaseFileReader):
    def read(self, file_path, file_config):
        return self.spark.read.json(file_path)
