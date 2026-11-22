---
name: modernize
description: Traite une entrée du backlog (ex. P0-2) de bout en bout - implémentation par airflow-modernizer, revue par code-auditor, une boucle de correction, puis résumé prêt à commiter.
disable-model-invocation: true
argument-hint: "<ID backlog, ex. P0-2> [précisions]"
arguments: [item]
allowed-tools: Bash(git *) Bash(uv run pytest *) Bash(uv run ruff *) Bash(uv sync *)
---

# Modernisation : $item

## Contexte
- Branche actuelle : !`git rev-parse --abbrev-ref HEAD 2>/dev/null || echo inconnue`
- Arbre de travail : !`git status --short 2>/dev/null | head -20 || true`
- Backlog : lis la table « Backlog priorisé » du fichier le plus récent de `docs/audit/` et retrouve l'entrée `$item` avec ses constats liés.
- Précisions de l'utilisateur : $ARGUMENTS

## Règles
- Si l'arbre de travail n'est pas propre, arrête-toi et demande quoi faire.
- Si `$item` n'existe pas dans le backlog, liste les 5 entrées les plus prioritaires non faites et demande laquelle traiter.

## Boucle
1. **Construire** — délègue au subagent `airflow-modernizer` : l'ID, l'intitulé, les constats liés (copie leur texte), et les précisions. Il crée sa branche, écrit les tests, implémente et lance les tests.
2. **Relire** — délègue au subagent `code-auditor` avec pour périmètre `git diff develop...HEAD`. Demande-lui uniquement les constats 🔴/🟠 introduits ou non résolus par ce changement.
3. **Corriger (une fois)** — s'il reste du 🔴/🟠, renvoie ces constats à `airflow-modernizer` (via SendMessage pour garder son contexte, sinon nouvel appel) pour une seule passe de correction, puis relance les tests. Ne boucle pas plus : s'il reste des problèmes, remonte-les à l'humain.
4. **Vérifier** — lance toi-même `uv sync` puis `uv run pytest tests/ -q` et cite le résultat.

## Rendu (en français, court)
- Branche, fichiers modifiés, résultat des tests
- Ce que la revue a trouvé et ce qui a été corrigé
- Points ouverts
- Message de commit proposé (Conventional Commits) et rappel : rien n'a été poussé.
