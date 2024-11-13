import os

class ReaderFactory:
    @staticmethod
    def get_reader(config, file_extension):
        from .csv_reader import CSVReader
        
        reader_classes = {
            'csv' : CSVReader
        }
        reader_class = reader_classes.get(file_extension)
        if not reader_class:
            raise ValueError(f"Unsupported file extension: {file_extension}")
        return reader_class()