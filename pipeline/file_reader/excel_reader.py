from .base_reader import BaseFileReader
from pyspark.sql import SparkSession, DataFrame
import pyspark.pandas as ps
import polars as pl

class ExcelReader(BaseFileReader):
    def __init__(self,spark : SparkSession):
        self.spark = spark
        
    def read(self, file_path: str, file_config: dict = None) -> DataFrame:
        # Récupère les options spécifiques à la lecture Excel
        # Si file_config est None, on utilise un dictionnaire vide pour éviter une erreur
        excel_options = file_config.get("excel_reader", {}) if file_config else {}
        
        try:
            pdf = ps.read_excel(file_path, **excel_options)
            return pdf
            
        except Exception as e:
            print(f"Erreur lors de la lecture du fichier Excel '{file_path}' avec pyspark.pandas: {e}")
            raise # Relancer l'exception pour que le pipeline puisse la gérer
            
    # La méthode read_polars reste inchangée si vous souhaitez la conserver pour Polars
    def read_polars(self, file_path, file_config):
        try:
            file_config = file_config.get("excel_reader", {}) if file_config else {}
            df_reader = pl.read_excel(file_path)#,**file_config # La ligne commentée signifie que file_config n'est pas utilisé ici
            return df_reader
        except Exception as e:
            print(f"Error reading file {file_path}, error: {e}")
