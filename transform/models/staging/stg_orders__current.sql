-- État courant de staging/orders, même règle que stg_movies__current
-- (latest_snapshot). Colonnes et types fixés par le contrat (P2-6,
-- reader.columns_type d'orders.yaml) ; tant qu'aucun fichier orders n'a été
-- lu, la relation vide de lake_staging() a les mêmes colonnes.
select
    order_id,
    customer,
    amount,
    order_date,
    lake_file_id,
    lake_ingested_at,
    lake_run_id,
    lake_source_file
from {{ lake_staging('lake_staging', 'orders') }} as snapshots
qualify lake_file_id = max(lake_file_id) over (partition by lake_source_file)
