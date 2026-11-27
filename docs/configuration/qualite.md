# Contrôles qualité

Les contrôles sont **natifs** : des modèles Pydantic (`pipeline/config/quality.py`), un moteur en
Spark pur (`pipeline/quality/checks.py`), sans Soda ni Great Expectations. Ils décident si la zone
`staging` d'un fichier est écrite.

## Déclarer des contrôles

Dans la config d'une source, `quality.checks` est une liste dont chaque élément est choisi par son
`type` :

| Type | Paramètres | Échoue quand… |
|---|---|---|
| `not_null` | `column` | la colonne contient des nuls |
| `unique` | `columns` | une combinaison de colonnes apparaît plus d'une fois (les lignes à clé nulle sont ignorées) |
| `accepted_values` | `column`, `values` | une valeur n'est pas dans la liste |
| `range` | `column`, `min` et/ou `max` | une valeur sort des bornes (inclusives ; colonne numérique) |
| `regex` | `column`, `pattern` | une valeur ne correspond pas au motif |
| `row_count` | `min` et/ou `max` | le nombre de lignes sort des bornes |

Chaque contrôle a un nom stable (`name`, ou un nom dérivé : `not_null(id)`, `unique(a,b)`,
`row_count`) et une `severity` : `error` (par défaut) ou `warn`.

```yaml
quality:
  checks:
    - {type: not_null, column: id}
    - {type: unique, columns: [id], severity: warn}
    - {type: accepted_values, column: country, values: ["FR", "NO"], severity: warn}
    - {type: range, column: amount, min: 0}
    - {type: row_count, min: 1}
```

## Règles au chargement

- `quality` exige `storage.staging_path` : les contrôles décident si staging est écrit.
- Avec `columns_type`, chaque colonne nommée doit y être déclarée, et `range` exige un type
  numérique.
- Deux contrôles identiques, ou deux noms identiques, sont refusés.
- Les bornes sont des entiers 64 bits ou des nombres finis.

## Pièges

- **`accepted_values` ne prend que du texte**, comparé à la forme texte de la colonne. PyYAML lit du
  YAML 1.1 et convertit les scalaires non quotés : `NO`, `yes`, `on`, `off` deviennent des booléens,
  `007` devient 7, `0123` devient 83. Écrivez `"NO"`, `"007"`, `"true"`, `"1"`. Le contrôle est
  refusé sur une colonne déclarée `float` ou `double` : utilisez `range`.
- **Les nuls ne sont comptés que par `not_null`.** `NaN` n'est pas nul, mais `range` le compte hors
  bornes sur une colonne `float` ou `double`.
- **`regex` utilise `rlike` de Spark**, une correspondance *partielle* : ancrez le motif
  (`^...$`). Il doit compiler en Python sans syntaxe propre à Java ou à Python, et le job le compile
  encore en Java juste après le démarrage de Spark (code 5 si ça échoue).

## Exécution

Les contrôles tournent sur les lignes **déjà nettoyées pour le staging** (lignes vides et doublons
exacts retirés) : une agrégation pour tous les contrôles par ligne et le nombre de lignes, plus un
`groupBy` par `unique`, sans jamais rapatrier de données (`collect`).

## Les quatre statuts

| Statut | Sens |
|---|---|
| `pass` | aucune ligne en échec |
| `fail` | des lignes en échec, sévérité `error` |
| `warn` | des lignes en échec, sévérité `warn` |
| `error` | le contrôle n'a pas pu s'exécuter (colonne absente, regex refusée, erreur Spark) ; `failed_rows` est nul |

Un contrôle est **bloquant** quand sa sévérité est `error` et son statut `fail` ou `error`. Un
`warn` ne bloque jamais, même en statut `error` (qui est alors journalisé au niveau ERROR).

## Quand un contrôle bloque

1. Le **raw** est écrit quand même.
2. La partition `staging` du fichier est supprimée et **n'est pas réécrite**.
3. Le fichier devient `failed`, avec `quality checks failed: <nom>: <détail>; ...`.
4. Le job sort avec le code **6** à la fin si aucun fichier n'a échoué pour une autre raison
   (sinon 1). Airflow ne rejoue pas le code 6 : il est déterministe.

Si le **moteur** lui-même lève une erreur, c'est pareil (il échoue fermé), avec
`quality checks could not run: <cause>`, mais le job sort avec le code 1 : la cause peut être
passagère.

## Où lire les résultats

`<LAKE_ROOT>/quality/<table>/` contient une ligne par contrôle, partitionnée par
`(lake_run_id, lake_source_file)` et réécrite par un retry. Quand une source a une zone staging, le
job supprime d'abord cette partition de résultats, qu'elle ait ou non une section `quality`. Si
l'écriture du staging échoue après des contrôles réussis, les résultats de cette tentative sont
retirés. Lisez les résultats **avec** le statut du fichier dans `file_list`.

## Dans le dépôt

- `examples/config/sources/imdb.yaml` déclare `not_null(id)` et `unique(id)` en `warn` pour un premier vrai
  run : à passer en `error` après vérification de `quality/movies` sur le vrai fichier
  (procédure dans `docs/architecture/2026-11-14-p2-6-phase2-followups.md`).
- `examples/config/sources/orders.yaml` déclare `not_null(order_id)`, `unique(order_id)` et `row_count min 1`,
  tous en `warn`.
- Les replis par extension n'ont aucun contrôle : ils n'ont pas de zone staging.
