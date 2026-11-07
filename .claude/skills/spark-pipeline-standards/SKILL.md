---
name: spark-pipeline-standards
description: Règles PySpark et architecture du pipeline de lecture de fichiers de ce projet - readers, configs YAML, processors, writers, job Spark, performance et qualité des données. À charger dès qu'on crée, modifie ou revoit du code dans pipeline/, storage/, utils/ ou config/.
when_to_use: "Mots-clés Spark, PySpark, DataFrame, reader, writer, config YAML, schéma, parquet, csv, excel, lakehouse, Iceberg, Delta."
paths:
  - "pipeline/**"
  - "storage/**"
  - "utils/**"
  - "config/**"
  - "tests/unit/file_reader/**"
  - "tests/unit/processor/**"
  - "tests/unit/storage/**"
  - "tests/unit/jobs/**"
---

# Standards Spark & pipeline — airflow_project_one

## 1. Contrat des readers
- Tout reader hérite d'une **classe abstraite unique** et `read(path, config) -> pyspark.sql.DataFrame`. Jamais un DataFrame pandas, pandas-on-Spark ou Polars en sortie : convertir (`.to_spark()`) avant de rendre la main.
- Les readers ne doivent pas hériter de `BaseFileReader` (qui est le routeur, pas le contrat).
- Un nouveau format = un reader + une entrée dans `ReaderFactory` + un test.

## 2. Configuration
- **Une seule forme** de config pour tous les readers. Tant que la migration n'est pas faite : quand tu touches un reader, vérifie quelle forme il lit (`reader.reader_options` vs `reader_options`) et teste les deux chemins (config dédiée et fallback `basic_reading_config.yaml`).
- Tout type déclaré dans `columns_type` doit être supporté par `get_spark_type()` — un test doit construire le schéma de **chaque** YAML de `config/`.
- Cible : config validée au chargement (Pydantic) et sélection par motif explicite, pas par sous-chaîne du nom de fichier.
- Les options déclarées (ex. `encoding`) doivent être réellement transmises à Spark, sinon les retirer.

## 3. Performance
- Pas de `.collect()`, `.toPandas()` ou `.show()` sur des volumes non bornés dans le code de production. `.show()` est réservé au debug.
- Préférer un schéma explicite à `inferSchema` en production (double lecture du fichier).
- Éviter les actions répétées sur un même DataFrame sans `cache()`/`persist()` justifié.
- Fixer `spark.sql.shuffle.partitions` selon le volume ; activer l'AQE (activée par défaut en Spark 3.2+, ne pas la désactiver).
- Pas d'UDF Python quand une fonction native `pyspark.sql.functions` existe.

## 4. Job Spark
- Reçoit le périmètre à traiter en argument (`--run-id`), ne calcule pas « aujourd'hui ».
- Met à jour le statut de chaque fichier (`processed`/`failed` + message d'erreur).
- Code de sortie non nul si **au moins un** fichier a échoué (ou politique explicite documentée).
- Écrit réellement le résultat via un `BaseWriter` ; formats cibles : Parquet partitionné, puis Iceberg ou Delta.
- `spark.stop()` dans un `finally`.

## 5. Writers
- Tout writer hérite de `BaseWriter` et implémente `write(data, target, mode, **kwargs)`.
- Mode par défaut idempotent (overwrite de partition ou merge) ; l'`append` doit être un choix explicite.
- Connexion DB : une connexion/engine réutilisée, jamais une par ligne.

## 6. Tests
- Fixture `SparkSession` partagée au scope `session`, `master("local[1]")`, UI désactivée.
- Fichiers de test minuscules générés dans `tmp_path`, jamais de dépendance à `data/`.
- Chaque bug corrigé = un test de non-régression nommé d'après l'ID du backlog (`test_b1_fallback_config_without_source_key`).

## 7. Dépendances
PySpark, pandas et pyarrow évoluent vite et cassent entre eux : borner les versions (ex. pandas 3 casse `pyspark.pandas` jusqu'à PySpark 4.3.0 inclus, vérifié sur ce projet : d'où `pandas<3` dans `pyproject.toml`). Avant de lever cette borne, vérifier `uv run python -c "import pyspark.pandas"` avec la nouvelle version.
