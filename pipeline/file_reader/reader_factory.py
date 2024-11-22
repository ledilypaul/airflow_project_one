import os

class ReaderFactory:
    @staticmethod
    def get_reader(config, file_extension):
        from .csv_reader import CSVReader
        from .parquet_reader import ParquetReader
        reader_classes = {
            'csv' : CSVReader,
            'parquet' : ParquetReader
        }
        reader_class = reader_classes.get(file_extension)
        if not reader_class:
            raise ValueError(f"Unsupported file extension: {file_extension}")
        return reader_class()