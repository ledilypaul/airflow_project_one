# P5-6 — Interface `Engine`

- **Date :** 2026-11-28
- **Branche :** `feat/p5-6-engine-interface` (depuis `develop`)
- **Backlog :** `docs/audit/2026-11-23-backlog-phase5.md`, fiche P5-6

## Pourquoi

L'orchestration du job (statuts, zones, codes de sortie) était mêlée aux appels Spark : le moteur
Polars (P5-7) aurait dû la recopier.

## Découpage

| Module | Contenu |
|---|---|
| `pipeline/engine/base.py` | protocole `Engine`, sans import de moteur |
| `pipeline/engine/spark/engine.py` | `SparkEngine` |
| `pipeline/jobs/ingest.py` | orchestration commune, contrat des codes 0 à 6 |
| `pipeline/jobs/read_files_job.py` | point d'entrée Spark : CLI, watchdog, session |
| `pipeline/quality/results.py` | statuts, exceptions, schéma Arrow des résultats |
| `storage/lake_root.py` | `LAKE_ROOT`, résolu avant de démarrer le moteur |

Le chemin du job, sa CLI, l'opérateur, l'image et le chart ne changent pas.

## Écarts avec la fiche

- Le code Spark **n'est pas déplacé** : readers, `BaseProcessor`, contrôles et writer restent où
  ils étaient, `SparkEngine` leur délègue. Un déplacement n'aurait changé que des chemins.
- Méthodes ajoutées au protocole : `config_problems` (regex Java), `get_config`,
  `persist`/`unpersist` et `location`. `write_lake` reçoit aussi `mode` (`storage.mode`) et
  `write_results` le nom de la source.
- Les logs de l'orchestration sortent sous le logger `pipeline.jobs.ingest`.
- Tests existants : imports, cibles des `patch` et, dans les deux helpers `_write` et le test de
  `clean_for_staging`, l'appel par `SparkEngine`. Aucune assertion modifiée.

## Vérifié

- Lake avant (`develop`) / après sur les mêmes fichiers (exemples, source par défaut, contrôle
  bloquant, fichier absent) : mêmes partitions, mêmes schémas Arrow, mêmes lignes triées hors
  `lake_ingested_at`, mêmes statuts et code de sortie.

## Non vérifié

- Image Docker et run kind non relancés.
- xlsx et parquet absents de la comparaison (couverts par les tests unitaires des readers).
