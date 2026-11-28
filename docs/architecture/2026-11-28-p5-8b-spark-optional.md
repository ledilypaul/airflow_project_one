# P5-8 (2/3) — PySpark en groupe optionnel

- **Date :** 2026-11-28
- **Branche :** `feat/p5-8b-spark-optional` (depuis `develop`)
- **Backlog :** fiche P5-8

## Ce qui change

- Les dépendances du projet sont celles du job Polars ; `pyspark` et `pandas<3` passent dans un
  groupe `spark`, **par défaut** (`uv sync`, l'image et pip-audit les gardent).
- Job CI `polars-only` : cœur seul (`--no-default-groups`), aucune distribution `pyspark`,
  `pyspark-client`, `apache-airflow` ni `pandas`, puis le vrai job Polars sur les fichiers
  d'exemple.

## Constats

- `uv sync --no-group spark` garde un module `pyspark` : c'est `pyspark-client` (Spark Connect,
  sans JVM ni jars), dépendance du fournisseur Spark d'Airflow. Le vrai PySpark est absent et le
  job Polars n'importe pas `pyspark`. Le retirer demanderait de rendre ce fournisseur optionnel,
  donc l'import de `ReadFilesSparkSubmitOperator` au parsing des DAG : hors périmètre.
- `--no-build` refuse aussi le projet en mode éditable ; le job CI ne l'installe donc pas
  (`--no-install-project`, dépôt sur `PYTHONPATH`). Avec Airflow, `--no-build` est impossible :
  `pyspark-client` n'est publié qu'en source.
- Le test se lance comme script dans ce job : la configuration de pytest (`filterwarnings`)
  importe Airflow.
- Taille mesurée : 408 Mo pour le cœur seul, 1,3 Go pour l'environnement complet.

## Vérifié

- Le job Polars de bout en bout dans le cœur seul (local, comme en CI) : code 0, fichiers
  `processed`, staging écrit, ni PySpark ni Airflow chargés.
- `uv lock --check`, `make check`.
