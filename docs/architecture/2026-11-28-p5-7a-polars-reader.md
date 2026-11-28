# P5-7 (1/3) — Lecture par le moteur Polars

- **Date :** 2026-11-28
- **Branche :** `feat/p5-7a-polars-reader` (depuis `develop`)
- **Backlog :** `docs/audit/2026-11-23-backlog-phase5.md`, fiche P5-7
- **Découpage de P5-7 :** 1. lecture ; 2. nettoyage et contrôles qualité ; 3. écriture du lake,
  lignage et `PolarsEngine` branché sur `pipeline/jobs/ingest.py`.

## Ce qui est livré

- `pipeline/engine/polars/reader.py::read_file(path, config) -> pl.LazyFrame` : `scan_csv`,
  `scan_parquet`, `read_excel(...).lazy()`, options passées telles quelles.
- `pipeline/engine/polars/types.py::from_text` : conversion du texte d'un CSV vers un type neutre.
- Dépendance `fastexcel` (moteur Excel par défaut de Polars).

## Choix

- **Avec `columns_type`, les règles de Spark, pas celles de Polars.** Le même YAML doit donner le
  même lake. Les règles ont été relevées sur Spark 4.3.0 (valeurs pièges par type) avant d'écrire
  le code ; le test les compare valeur par valeur. Un `cast` Polars simple divergeait sur les
  doubles (espaces, `nan`, `1d`), les dates (`2026-11`, heure à la suite), les horodatages
  (décalages, `2026-11-01 10`), les booléens (pas de cast texte) et `""` (chaîne vide, null chez Spark).
- **Sans `columns_type`, les règles de Polars** : inférence et défauts de `scan_csv`.
- **En-tête toujours vérifié** (nombre, noms sans casse) : Polars n'a pas d'équivalent de
  `enforceSchema: true`, qui lirait un autre fichier décalé.
- **Horodatage en UTC, sans fuseau** (`Datetime("us")`), comme DuckDB le lit dans le lake.

## Constat hors périmètre

Spark prend un horodatage sans décalage dans le fuseau de sa session, celui de la JVM :
`utils/spark_session.py` ne fixe pas `spark.sql.session.timeZone`. Une même valeur `10:00` donne
donc `09:00` UTC sur un poste en Europe/Paris et `10:00` dans un pod en UTC. À corriger (fixer
`UTC`) avant le test doré de l'étape 3.

## Écarts connus

Ils sont listés dans les docstrings de `types.py` et de `reader.py` : nombres hexadécimaux, années
hors de 4 chiffres, fuseaux nommés, heure seule ; ligne vide (null en Polars, ignorée par Spark),
ligne trop longue et fichier vide (échec en Polars, par défaut).

## Vérifié

- Parité valeur par valeur sur les 8 types neutres, et sur les sources d'exemple traduites en
  options Polars ; parquet et xlsx sur un petit fichier.
- Le test de parité échoue si une règle est retirée (essai sur `""`).

## Non vérifié

- Gros fichiers et streaming (étape 3, `sink_parquet`).
- Encodages autres qu'UTF-8 (refusés au chargement par la liste blanche).
