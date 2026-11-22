-- État courant de staging/movies : pour chaque fichier source, les seules
-- lignes de son instantané le plus récent (plus grand lake_file_id, soit
-- file_list.id, attribué par une séquence Postgres au listing). Équivalent
-- DuckDB de storage.parquet_lake_writer.latest_snapshot : ni l'heure
-- d'écriture (lake_ingested_at, renouvelée par un retry) ni le run id (non
-- ordonné) ne décident. lake_ingested_at est relu en TIMESTAMP sans fuseau,
-- exprimé en UTC (INT96 écrit par Spark).
select *
from {{ lake_staging('lake_staging', 'movies') }} as snapshots
qualify lake_file_id = max(lake_file_id) over (partition by lake_source_file)
