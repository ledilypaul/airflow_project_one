---
name: full-audit
description: Lance un audit complet du projet en parallèle (code-auditor + security-auditor), consolide un rapport unique dans docs/audit/ et propose les mises à jour du backlog.
disable-model-invocation: true
argument-hint: "[périmètre optionnel : diff | tout | chemin]"
allowed-tools: Bash(git *) Bash(date *)
---

# Audit complet

## Contexte
- Branche : !`git rev-parse --abbrev-ref HEAD 2>/dev/null || echo inconnue`
- Commit : !`git log -1 --oneline 2>/dev/null || echo aucun`
- Fichiers modifiés vs develop : !`git diff --stat develop...HEAD 2>/dev/null | tail -1 || true`
- Date : !`date +%F`
- Baselines existantes : !`ls -1 docs/audit/ 2>/dev/null || echo aucune`
- Périmètre demandé : $ARGUMENTS (vide = diff vs develop s'il existe, sinon tout le dépôt)

## Étapes
1. Lance **en parallèle**, dans le même message, les subagents `code-auditor` et `security-auditor`, avec chacun : le périmètre, la branche et le commit ci-dessus, et le chemin de la baseline la plus récente.
2. Attends les deux rapports. Ne les modifie pas sur le fond.
3. Consolide dans `docs/audit/<date>-audit.md` :
   - en tête, une synthèse unique (tableau des sévérités tous rapports confondus) et les 3 actions à faire en premier ;
   - puis les sections « Code » et « Sécurité » reprises des deux rapports ;
   - fusionne les doublons (un même problème vu par les deux auditeurs = un seul constat, deux angles).
4. Vérifie toi-même au hasard 2 constats marqués ✅ en relançant la commande de preuve. Si l'un ne se reproduit pas, rétrograde-le en ❓ et signale-le.
5. Propose (sans l'appliquer) le diff du tableau « Backlog priorisé » de la baseline : entrées à ajouter, à clore, à reprioriser.
6. Réponds en 5 lignes maximum : chemin du rapport, compteurs par sévérité, les 3 priorités.
