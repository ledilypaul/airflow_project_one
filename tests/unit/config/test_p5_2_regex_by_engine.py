"""P5-2 — a quality regex is validated for the engine that runs it."""

import pytest

from pipeline.config import SourceConfig
from pipeline.config.quality import python_regex_problem, rust_regex_problem
from pipeline.quality.checks import java_regex_problems

STORAGE = {
    "format": "parquet",
    "mode": "overwrite",
    "raw_path": "raw/",
    "staging_path": "staging/",
}


def _config(engine: str, pattern: str) -> dict:
    return {
        "source": {"name": "t", "format": "csv", "patterns": ["*"], "engine": engine},
        "storage": STORAGE,
        "quality": {"checks": [{"type": "regex", "column": "c", "pattern": pattern}]},
    }


@pytest.mark.parametrize(
    "pattern",
    [r"(?=a)", r"(?!a)", r"(?<=a)", r"(?<!a)", r"(?>a)", r"(a)\1", r"(?P<n>a)(?P=n)", r"(a)\k<x>"],
)
def test_p5_2_polars_refuses_lookaround_and_backreferences(pattern):
    assert rust_regex_problem(pattern)
    with pytest.raises(ValueError, match="crate Rust"):
        SourceConfig.model_validate(_config("polars", pattern))


@pytest.mark.parametrize("pattern", [r"^[A-Z]{3}$", r"\\1", r"(?i)ab", r"(?:a|b)+", r"\d+(\.\d+)?"])
def test_p5_2_polars_accepts_plain_patterns(pattern):
    """`\\\\1` is an escaped backslash then `1`, not a back reference."""
    assert rust_regex_problem(pattern) is None
    SourceConfig.model_validate(_config("polars", pattern))


def test_p5_2_syntax_valid_in_rust_only_is_accepted_for_polars_not_for_spark():
    pattern = r"\p{L}+"  # Java and Rust: yes on Rust's side, refused by Python's `re`
    assert python_regex_problem(pattern)
    assert rust_regex_problem(pattern) is None
    SourceConfig.model_validate(_config("polars", pattern))
    with pytest.raises(ValueError, match="regex invalide"):
        SourceConfig.model_validate(_config("spark", pattern))


def test_p5_2_spark_keeps_accepting_lookaround():
    SourceConfig.model_validate(_config("spark", r"(?=a)a"))


def test_p5_2_job_does_not_compile_a_polars_regex_in_java():
    """`spark=None` would crash on a Java compile: a Polars source must be skipped."""
    config = SourceConfig.model_validate(_config("polars", r"\p{L}+"))
    assert java_regex_problems(None, [("polars.yaml", config)]) == []


def test_p5_2_rust_regex_problem_agrees_with_polars_when_installed():
    pl = pytest.importorskip("polars")
    for pattern in [r"(?=a)", r"(?<!a)", r"(?>a)", r"(a)\1", r"(?P<n>a)(?P=n)", r"^[A-Z]{3}$"]:
        try:
            pl.select(pl.lit("x").str.contains(pattern))
            polars_refuses = False
        except pl.exceptions.ComputeError:
            polars_refuses = True
        assert (rust_regex_problem(pattern) is not None) == polars_refuses, pattern
