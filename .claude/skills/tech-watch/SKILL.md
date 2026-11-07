---
name: tech-watch
description: Veille technologique ciblée sur la stack du projet via le subagent tech-scout, enregistrée dans docs/tech-watch/ avec propositions pour le backlog.
disable-model-invocation: true
argument-hint: "[sujet optionnel : airflow | spark | dbt | lakehouse | sécurité | ml]"
allowed-tools: Bash(date *)
---

# Veille techno

- Date : !`date +%F`
- Sujet : $ARGUMENTS (vide = toute la stack actuelle + dbt, lakehouse, qualité de données)

1. Délègue au subagent `tech-scout` avec le sujet ci-dessus et le chemin de la baseline la plus récente de `docs/audit/`.
2. Enregistre son rapport tel quel dans `docs/tech-watch/<date>-<sujet ou "stack">.md`.
3. Réponds en 5 lignes : versions en retard critiques, la meilleure opportunité « à adopter maintenant », et les IDs de backlog proposés.
