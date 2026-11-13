# storage/
# ├── __init__.py
# ├── parquet_lake_writer.py      # lake Parquet partitionné, zones raw/staging (P2-2)
# ├── postgresql_writer.py        # insertions dans PostgreSQL
# ├── hive_writer.py              # insertions dans Hive (via parquet/HDFS)
# ├── hive_reader.py              # requêtes Hive
# ├── hdfs_manager.py             # gestion des fichiers sur HDFS
# ├── schema_manager.py           # gestion des schémas (optionnel)
