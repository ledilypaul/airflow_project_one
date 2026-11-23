# config/

Racine de config du moteur (`PIPELINE_CONFIG_ROOT` la remplace, chemin absolu).

- `sources/` : **vos** configs de lecture, une par source (vide dans le dépôt).
- `defaults/` : le repli par extension (`*.csv`, `*.txt`, `*.xlsx`, `*.parquet`), livré avec le moteur.
- `extractor_config.yaml` : la config du listing.

Des sources d'exemple (`imdb_movies`, `orders`) sont dans `examples/` : `make demo`.
