# examples/

Le moteur ne connaît aucune source : ce dossier contient les exemples.

- `config/sources/` : `imdb.yaml` et `orders.yaml`.
- `data/` : deux petits fichiers synthétiques (aucune donnée IMDB réelle).

Lancer la démo : `make demo` (copie `data/` dans `./data/`, exporte
`PIPELINE_CONFIG_ROOT=examples/config`, lance `make airflow`).

Les modèles dbt d'exemple (`stg_movies__current`, `stg_orders__current`, le mart) restent
dans `transform/models/` jusqu'à leur génération depuis les YAML (P5-4).
