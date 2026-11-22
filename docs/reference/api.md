# Référence de l'API Python

Cette référence est générée depuis les docstrings du code par `sphinx-autodoc2`, qui analyse les
sources **sans les importer** : ni PySpark ni Airflow n'ont besoin d'être installés pour la
construire. Elle couvre les paquets `pipeline`, `storage` et `utils`.

Elle ne couvre pas :

- les **DAGs** (`dags/`), qui importent Airflow : ils se décrivent dans [](../concepts/dags.md) ;
- les **scripts** de `scripts/`, des programmes en ligne de commande : voir les pages
  d'[exploitation](../exploitation/api-rest.md) ;
- `pipeline/IMDBExtractor.py` et `pipeline/IMDBTransform.py`, un ancien couple de scripts propre à
  IMDB, hors du pipeline.

```{toctree}
:maxdepth: 3

autodoc/index
```
