# P5-8 (1/3) — Exécution du job Polars dans Airflow

- **Date :** 2026-11-28
- **Branche :** `feat/p5-8a-polars-task` (depuis `develop`)
- **Backlog :** fiche P5-8. Étapes suivantes : 2. dépendances (`pyspark` en groupe optionnel) ;
  3. ressources Kubernetes par source et bascule du moteur par défaut.

## Ce qui est livré

- `polars_read_files` remplace `spark_read_files` dans le DAG d'une source `engine: polars`
  (`utils/polars_task.py`), avec `fail_if_polars_failed` ; pool `polars`.
- `utils/exit_codes.py` : codes, sens et codes sans retry, une seule définition pour les deux jobs,
  l'opérateur Spark, la tâche Polars et `utils/failure_alert.py` (`polars_exit_code`).
- Déplacements sans changement de comportement : `utils/job_args.py` (arguments et
  environnement des jobs), `utils/lake_lineage.py` (`spark_dataset`), watchdog dans `ingest.py`.

## Choix

- **Sous-processus**, pas un appel dans le processus de la tâche : un manque de mémoire tue le
  job, le worker reste. Sans shell ; sortie recopiée dans le log de la tâche ; job arrêté si la
  tâche est interrompue (SIGTERM puis SIGKILL), et son watchdog le fait sortir (code 4) s'il perd
  son parent.
- **`polars_job.py` plutôt que `python -m pipeline.jobs.ingest --engine polars`** (fiche) :
  `ingest.py` reste sans import de moteur.
- **Lignage par le collecteur de l'Airflow**, pas de client OpenLineage dans le job : l'événement
  de la tâche porte le run parent et le transport d'Airflow. Entrées : `file_list` et les fichiers
  listés ; sorties : les tables du lake de la source, nommées comme le listener Spark. Déclaré
  quel que soit le code de sortie.
- **Tâche `@task`**, comme demandé : le code de sortie est posé sur l'objet tâche
  (`polars_exit_code`), que le callback d'échec lit dans le même processus.

## Vérifié

- `dag.test()` : une source Polars (`orders` passé en Polars) et une source Spark dans la même
  racine de config, vertes ; arguments et environnement reçus par le job ; code 6 non retenté,
  code 1 retenté, ligne `airflow_failure` avec `polars_exit_code` sur le dernier essai.
- Le vrai job lancé par la tâche : `--run-id=` vide → code 2 → `AirflowFailException`.
- Interruption de la tâche : le job est arrêté.
- Noms de lignage comparés à ceux du listener Spark (`spark_dataset`).

## Non vérifié

- `transform_dag` derrière une source Polars : `dag.test()` ne sait pas l'exécuter (CLAUDE.md) ;
  à faire avec `make airflow` ou sur kind (étape 3).
- Le job Polars réel sous Airflow avec une vraie base `file_list` (le test e2e le remplace par un
  stub, comme spark-submit).
