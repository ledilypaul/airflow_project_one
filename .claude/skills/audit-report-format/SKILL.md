---
name: audit-report-format
description: Format standard et règles de preuve pour tout rapport d'audit (code, sécurité) de ce projet - échelle de sévérité, niveau de vérification, comparaison avec la baseline. Utilisé par code-auditor et security-auditor.
user-invocable: false
---

# Format des rapports d'audit

## Règles de preuve
1. Chaque constat cite `fichier:ligne` (ou la commande et sa sortie).
2. Chaque constat porte un niveau de vérification :
   - ✅ **vérifié** — reproduit par exécution (test, script, commande), sortie citée
   - 🔍 **constaté** — certain à la lecture du code, non exécuté
   - ❓ **hypothèse** — plausible, à confirmer ; dire comment le confirmer
3. Pas de constat générique (« améliorer la gestion d'erreurs ») : un constat = un problème précis + un correctif précis.
4. Compare avec la baseline la plus récente de `docs/audit/` : chaque constat est **nouveau**, **aggravé**, **inchangé** (à ne lister qu'en résumé) ou **corrigé**.
5. Rédige en français. Ne recopie jamais la valeur d'un secret.

## Échelle de sévérité
- 🔴 **Critique** — fuite de secret, perte/corruption de données, fonctionnalité principale cassée
- 🟠 **Élevé** — risque sérieux ou bug probable en production, dette qui bloque la suite
- 🟡 **Moyen** — mauvaise pratique avec impact réel mais contourné aujourd'hui
- ⚪ **Faible** — hygiène, lisibilité, nettoyage

## Gabarit
```markdown
# Audit <code|sécurité> — <YYYY-MM-DD>
- Périmètre : <branche @ commit, ou diff>
- Commandes exécutées : <liste + résultat résumé, ex. "pytest : 63 passés, 1 échec">
- Baseline de comparaison : <fichier>

## Synthèse
| Sévérité | Nouveaux | Aggravés | Corrigés depuis la baseline |
|---|---|---|---|

## Constats
### <ID> — <titre court> · <sévérité> · <✅|🔍|❓> · <nouveau|aggravé>
- **Où :** `fichier:ligne`
- **Problème :** …
- **Preuve :** …
- **Impact :** …  (sécurité : scénario d'attaque en une phrase)
- **Correctif :** …  (effort XS/S/M/L)

## Corrigés depuis la baseline
- <ID baseline> — <comment c'est corrigé>

## Propositions pour le backlog
| ID proposé | Phase | Tâche | Constats liés | Effort |
```
IDs : `C-<n>` pour le code, `SEC-<n>` pour la sécurité, numérotés dans le rapport.
