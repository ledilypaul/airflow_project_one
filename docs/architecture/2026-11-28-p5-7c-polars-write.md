# P5-7 (3/3) — Écriture du lake et job Polars

- **Date :** 2026-11-28
- **Branche :** `feat/p5-7c-polars-write` (depuis `develop`)
- **Backlog :** fiche P5-7 ; étapes 1 et 2 : `2026-11-28-p5-7a-polars-reader.md`,
  `2026-11-28-p5-7b-polars-clean-quality.md`

## Ce qui est livré

- `pipeline/engine/polars/writer.py` (`PolarsLakeWriter`) et `engine.py` (`PolarsEngine`).
- `pipeline/jobs/polars_job.py` : `ingest.run` avec le moteur Polars, même contrat que le job Spark.
- `storage/lake_layout.py` : colonnes techniques, chemins, échappement, contrôles avant écriture,
  partagés par les deux writers (le writer Spark ne les porte plus lui-même).

## Relevé sur Spark avant d'écrire

Fichier écrit par `ParquetLakeWriter` : colonnes métier `optional`, `lake_file_id` et
`lake_ingested_at` `required`, horodatages en INT96 (relus `timestamp[ns]` par pyarrow), un ou
plusieurs `part-NNNNN-<uuid>.c000.snappy.parquet` par partition, et valeurs de partition échappées
pour `\x01`-`\x1F`, `"#%'*/:=?[\]^{` et `\x7F` (non-ASCII gardé).

## Choix

- **pyarrow plutôt que `sink_parquet`** : seul pyarrow écrit de l'INT96. Avec un horodatage en
  `TIMESTAMP(MICROS)`, Spark relirait la colonne en `TIMESTAMP_NTZ`, et une table écrite par les
  deux moteurs mélangerait deux types. Prix : le fichier est tenu en mémoire (pas de streaming).
- **Overwrite par renommage** : écriture dans un répertoire caché de la table (`.polars-<uuid>`,
  ignoré par les globs du lake), puis suppression de l'ancienne partition et renommage.
- **Lac local seulement** : un lac objet demandera `pyarrow.fs` (`NotImplementedError` d'ici là).

## Écart avec le découpage prévu

**Lignage reporté à P5-8.** Le transport et le run parent d'un événement OpenLineage viennent de
la tâche Airflow qui lance le job, et `openlineage-python` n'est installé qu'avec le groupe
`airflow`. Sans lanceur, les événements seraient orphelins et le contrat ne pourrait pas être
testé.

## Vérifié

- Test doré : les deux jobs sur les sources d'exemple et une source qui couvre chaque type, un
  contrôle bloquant et un en-tête faux. Mêmes partitions, schémas physiques (texte du schéma
  Parquet), lignes hors `lake_ingested_at`, résultats qualité, statuts finaux et code de sortie.
  Le test échoue sans l'INT96 (essai).
- `dbt build` sur un lake dont chaque table mélange des partitions des deux moteurs : contrats
  passés, état courant identique au `latest_snapshot` de Spark.
- Échappement comparé à `ExternalCatalogUtils.escapePathName` sur tout l'ASCII et de l'Unicode.

## Écarts connus

- Polars lit l'en-tête tout de suite : un fichier mal formé passe à `failed` sans le `pending`
  que Spark écrit d'abord (statut final identique).
- Pas de watchdog du parent dans le job Polars : il n'est pas lancé par spark-submit (P5-8).
- Gros fichiers : tenus en mémoire, non mesurés.
