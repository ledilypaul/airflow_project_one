# config/

Racine de config du moteur (`PIPELINE_CONFIG_ROOT` la remplace, chemin absolu).

- `sources/` : **vos** configs, une par source (vide dans le dépôt) : lecture, écriture, qualité, et le
  DAG `ingest__<nom>` qui en est généré (`listing`, `schedule`, `retries`, `tags`).
- `defaults/` : le repli par extension (`*.csv`, `*.txt`, `*.xlsx`, `*.parquet`), livré avec le moteur.
- `pipeline.yaml` : les répertoires listés par défaut (`default_directories`).

Des sources d'exemple (`imdb_movies`, `orders`) sont dans `examples/` : `make demo`.
