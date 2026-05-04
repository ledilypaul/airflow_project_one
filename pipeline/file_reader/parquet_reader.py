from .base_reader import BaseFileReader


class ParquetReader(BaseFileReader):
    def read(self, file_path, file_config):
        df_reader = self.spark.read
        options = file_config.get("reader_options", {})
        for key, value in options.items():
            df_reader = df_reader.option(key, str(value))
        return df_reader.parquet(file_path)
