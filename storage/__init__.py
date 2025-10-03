# storage/
# ├── __init__.py
# ├── postgresql_writer.py        # insertions dans PostgreSQL
# ├── hive_writer.py              # insertions dans Hive (via parquet/HDFS)
# ├── hive_reader.py              # requêtes Hive
# ├── hdfs_manager.py             # gestion des fichiers sur HDFS
# ├── schema_manager.py           # gestion des schémas (optionnel)