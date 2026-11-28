# P5-7 (2/3) — Nettoyage et contrôles qualité par le moteur Polars

- **Date :** 2026-11-28
- **Branche :** `feat/p5-7b-polars-clean-quality` (depuis `develop`)
- **Backlog :** `docs/audit/2026-11-23-backlog-phase5.md`, fiche P5-7 ; étape 1 :
  `2026-11-28-p5-7a-polars-reader.md`

## Ce qui est livré

- `pipeline/engine/polars/clean.py::clean_for_staging` (LazyFrame) ;
- `pipeline/engine/polars/quality.py` : `run_quality_checks` (une agrégation, un `group_by` par
  `unique`, repli contrôle par contrôle) et `polars_regex_problems` (pendant de
  `java_regex_problems`) ;
- la partie neutre des résultats (`check_result`, `as_text`, `missing_column_error`) passe de
  `pipeline/quality/checks.py` à `pipeline/quality/results.py`, partagée par les deux moteurs.

## Règles de Spark relevées et reproduites

| Sujet | Spark | Polars sans correction |
|---|---|---|
| Ligne vide | NaN compte comme null (`na.drop`) | NaN n'est pas null |
| Doublons, `unique` | NaN = NaN, `-0.0` = `0.0` | identique |
| Texte d'un horodatage | `2026-11-01 10:00:00.12` | `2026-11-01 10:00:00.120000` |
| Texte de l'infini | `Infinity` | `inf` |
| Type dans un message | `string`, `bigint` | `String`, `Int64` |

Écart laissé, documenté : notation scientifique des doubles en texte (`1.0E10` / `10000000000.0`).

## Vérifié

- Mêmes résultats (nom, statut, lignes en échec, lignes contrôlées, message) sur 21 contrôles
  couvrant chaque type et les quatre statuts ; seul le texte d'une erreur de compilation de
  regex diffère (Java / Rust).
- Le test échoue si on retire la règle NaN de `range` ou la forme texte des horodatages (essais).
- Tests existants des contrôles Spark inchangés.

## Non vérifié

- Repli contrôle par contrôle quand l'agrégation commune échoue : pas de cas qui la fasse échouer
  en Polars trouvé ; le code suit celui de Spark.
- Volumétrie (étape 3).
