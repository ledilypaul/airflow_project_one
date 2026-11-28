-- Généré par scripts/generate_dbt_staging.py (`make dbt-codegen`) : ne pas modifier.
-- État courant de staging/orders : pour chaque fichier source, les seules lignes
-- de son instantané le plus récent (plus grand lake_file_id), comme
-- storage.parquet_lake_writer.latest_snapshot. Colonnes fixées par le contrat.
select *
from {{ lake_staging('lake_staging', 'orders') }} as snapshots
qualify lake_file_id = max(lake_file_id) over (partition by lake_source_file)
