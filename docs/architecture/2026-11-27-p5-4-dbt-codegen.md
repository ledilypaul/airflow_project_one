# P5-4 — Génération de la couche staging dbt

- **Date :** 2026-11-27
- **Branche :** `feat/p5-4-dbt-codegen` (depuis `develop`)
- **Backlog :** `docs/audit/2026-11-23-backlog-phase5.md`, fiche P5-4

## Pourquoi

Les sources dbt, les modèles `stg_*__current` et leurs contrats recopiaient `reader.columns_type` à
la main : trois éditions par nouvelle source.

## Choix

- `scripts/generate_dbt_staging.py` écrit `transform/models/staging/generated/` ; fichiers commités,
  `--check` dans `make check` et la CI (avant `make dbt-parse`, qui régénère).
- **Racines lues : `config/` et `examples/config/`**, plus `PIPELINE_CONFIG_ROOT` s'il est fixé.
  Le mart d'exemple dépend de ces modèles et `config/` est vide dans le dépôt. Conséquence : le
  projet dbt contient les sources d'exemple ; les en sortir demanderait de déplacer le mart.
- `select *` pour tous les modèles : le contrat fixe les colonnes. Changement pour
  `stg_orders__current`, qui les listait : une colonne en plus dans le lake fait désormais échouer
  le build, comme pour `stg_movies__current`.
- Colonnes citées (`quote: true`) : un mot réservé ou un espace passe. Accolades et `"` refusés
  (dbt rend le YAML avec Jinja).
- Un fichier inchangé n'est jamais réécrit : `transform_dag` compare les dates avec le manifest.
- Une table partagée par deux sources aux colonnes différentes est une erreur.

## Vérifié

- `make check` (1296 tests, dont les `dbt build` réels sur les modèles générés) et `make docs`.
- `dbt build` réel d'une source neuve (colonnes `order`, `Unit Price`, `At`) après régénération :
  23/23.

## Non vérifié, limites

- Un `staging_path` autre que `staging/` n'est pas généré (avertissement) : le glob des sources dbt
  est `staging/<table>`.
- Avec `PIPELINE_CONFIG_ROOT`, la régénération modifie des fichiers commités ; un `--check` lancé
  sans la variable les voit alors périmés.
- Image Docker non reconstruite, run kind non relancé (l'image embarque les fichiers commités).
