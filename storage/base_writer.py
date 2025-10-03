from abc import ABC, abstractmethod

class BaseWriter(ABC):
    """Generic interface for writers (Postgres, Hive, HDFS, etc.)"""
    @abstractmethod
    def write(self, data, target, **kwargs):
        """
        Write data into target
        - data : DataFrame Spark, Pandas, dict, ...
        - target : table, HDFS path, bucket, ...
        - kwargs : options (mode='overwrite', partitionBy=..., etc.)
        """
        pass