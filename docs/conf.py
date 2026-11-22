"""Configuration de la documentation Sphinx (``make docs``)."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))  # `pipeline`, `utils`, `storage` même sans installation éditable

project = "airflow_project_one"
author = "ledilypaul"
copyright = "2026, ledilypaul"  # noqa: A001
language = "fr"

extensions = [
    "myst_parser",
    "autodoc2",
    "sphinx_copybutton",
    "sphinxcontrib.mermaid",
]

source_suffix = {".md": "markdown", ".rst": "restructuredtext"}
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# MyST : le Markdown des notes existantes (architecture, audit, tech-watch) est repris tel quel.
myst_enable_extensions = ["colon_fence", "deflist"]
myst_heading_anchors = 3

# Documentation de l'API : les docstrings du projet sont du Markdown (backticks simples, listes,
# blocs indentés), pas du reStructuredText : autodoc2 les lit avec MyST, sans importer le code
# (ni pyspark ni airflow n'ont besoin d'être là). Les pages sont écrites à chaque build dans
# docs/reference/autodoc (ignoré par git).
autodoc2_packages = [
    {"path": "../pipeline", "module": "pipeline", "exclude_dirs": ["__pycache__"]},
    {"path": "../storage", "module": "storage"},
    {"path": "../utils", "module": "utils"},
]
autodoc2_output_dir = "reference/autodoc"
autodoc2_render_plugin = "myst"
autodoc2_docstring_parser_regexes = [(r".*", "myst")]
autodoc2_module_all_regexes = []
autodoc2_skip_module_regexes = [r"pipeline\.IMDB.*"]  # scripts ad hoc, hors du pipeline
autodoc2_hidden_objects = ["private", "inherited"]

# Une annotation comme `dict[str, type]` renvoie au builtin `type`, que Sphinx trouve aussi dans les
# attributs `type` des modèles de qualité : « more than one target found ». Ambiguïté de nom, pas un
# lien cassé ; les liens Markdown (MyST) cassés restent des erreurs.
suppress_warnings = ["ref.python"]

html_theme = "furo"
html_title = "airflow_project_one"
html_theme_options = {
    "source_repository": "https://github.com/ledilypaul/airflow_project_one/",
    "source_branch": "develop",
    "source_directory": "docs/",
}
