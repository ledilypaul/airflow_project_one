-- État courant de staging/orders, même règle que stg_movies__current
-- (latest_snapshot). Schéma métier inconnu (inferSchema) : select *. Tant
-- qu'aucun fichier orders n'a été lu, seules les colonnes techniques
-- existent (relation vide de lake_staging).
select *
from {{ lake_staging('lake_staging', 'orders') }} as snapshots
qualify lake_file_id = max(lake_file_id) over (partition by lake_source_file)
