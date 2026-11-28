"""Moteur de calcul d'une source (P5-2) : `source.engine`, sinon
`PIPELINE_DEFAULT_ENGINE`, sinon `spark` (le défaut passera à `polars` quand
ce moteur aura la parité avec Spark, P5-8).

Le moteur décide de ce qui est valide dans une config : options de lecture
(`reader_options.py`) et syntaxe des regex qualité (`quality.py`).
"""

import os
from typing import Literal

Engine = Literal["spark", "polars"]
ENGINES: tuple[str, ...] = ("spark", "polars")
DEFAULT_ENGINE_ENV = "PIPELINE_DEFAULT_ENGINE"
FALLBACK_ENGINE: Engine = "spark"


def default_engine() -> Engine:
    """`$PIPELINE_DEFAULT_ENGINE` (lue à chaque appel), sinon `spark`.

    Raises:
        ValueError: valeur autre que `spark` ou `polars`.
    """
    value = os.environ.get(DEFAULT_ENGINE_ENV, "").strip()
    if not value:
        return FALLBACK_ENGINE
    if value not in ENGINES:
        raise ValueError(
            f"{DEFAULT_ENGINE_ENV}={value!r} : moteur inconnu (valeurs acceptées : "
            f"{', '.join(ENGINES)}, en minuscules)"
        )
    return value  # type: ignore[return-value]
