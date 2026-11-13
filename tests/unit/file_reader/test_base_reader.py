from unittest.mock import MagicMock, patch

import pytest
import yaml

from pipeline.file_reader.base_reader import BaseFileReader


@pytest.fixture
def base_reader(spark, config_dir):
    """Fixture pour initialiser un lecteur de base."""
    return BaseFileReader(spark, config_path=config_dir)


@pytest.fixture
def config_dir(tmp_path):
    """Config root in the P2-1 layout: dedicated sources + defaults by extension."""
    config_dir = tmp_path / "config"
    (config_dir / "sources").mkdir(parents=True)
    (config_dir / "defaults").mkdir()

    # Exemple de config spécifique
    (config_dir / "sources" / "example.yaml").write_text(
        yaml.safe_dump(
            {
                "source": {"name": "sample_config", "format": "csv", "patterns": ["example_*"]},
                "reader": {"options": {"header": True, "inferSchema": True, "delimiter": ","}},
            }
        )
    )

    # Exemples de configs par défaut
    (config_dir / "defaults" / "csv.yaml").write_text(
        yaml.safe_dump(
            {
                "source": {"name": "csv_config", "format": "csv", "patterns": ["*.csv"]},
                "reader": {"options": {"header": True, "inferSchema": False, "delimiter": ","}},
            }
        )
    )
    (config_dir / "defaults" / "parquet.yaml").write_text(
        yaml.safe_dump(
            {
                "source": {
                    "name": "parquet_config",
                    "format": "parquet",
                    "patterns": ["*.parquet"],
                },
                "reader": {"options": {"mergeSchema": True}},
            }
        )
    )

    return config_dir


@pytest.fixture
def test_file(tmp_path):
    test_file = tmp_path / "file1.csv"
    test_file.write_text("col1,col2\n1,2\n3,4")
    return test_file


def test_get_config_specific(base_reader, tmp_path, monkeypatch):
    # The config root is not resolved from the current directory.
    monkeypatch.chdir(tmp_path)
    config = base_reader.get_config("example_file.csv")
    assert config.reader.options["header"] is True
    assert config.source.name == "sample_config"


def test_get_config_default(base_reader):
    config = base_reader.get_config("file1.csv")
    assert config.source.name == "csv_config"
    assert config.reader.options["header"] is True


def test_read_file(base_reader, test_file):
    dummy_df = base_reader.spark.createDataFrame([("Alice", 1), ("Bob", 2)], ["name", "id"])

    dummy_reader = MagicMock()
    dummy_reader.read.return_value = dummy_df

    with patch(
        "pipeline.file_reader.reader_factory.ReaderFactory.get_reader", return_value=dummy_reader
    ):
        df = base_reader.read_file(str(test_file))
        assert df.count() == 2


def test_b1_read_file_resolves_format_from_default_config(spark, tmp_path):
    """
    Regression test for B1 (docs/audit/2026-11-05-baseline.md): a file with
    no dedicated config used to crash with `KeyError: 'source'` because
    `read_file()` only ever looked at `file_config["source"]["format"]`,
    while the `basic_reading_config.yaml` fallback had a flat `format` key
    — reproduced on a "random_data.csv"-style file.

    Since P2-1 the fallback (config/defaults/*.yaml) has the same validated
    shape as the dedicated configs: a file without a dedicated config falls
    back on the default for its extension, and its `source.format` reaches
    the factory.
    """
    config_dir = tmp_path / "config"
    (config_dir / "defaults").mkdir(parents=True)
    (config_dir / "defaults" / "csv.yaml").write_text(
        yaml.safe_dump(
            {
                "source": {"name": "default_csv", "format": "csv", "patterns": ["*.csv"]},
                "reader": {"options": {"header": True, "inferSchema": True, "delimiter": ","}},
            }
        )
    )
    reader = BaseFileReader(spark, config_path=config_dir)

    test_file = tmp_path / "random_data.csv"
    test_file.write_text("col1,col2\n1,2\n")

    dummy_df = spark.createDataFrame([(1, 2)], ["col1", "col2"])
    dummy_reader = MagicMock()
    dummy_reader.read.return_value = dummy_df

    with patch(
        "pipeline.file_reader.reader_factory.ReaderFactory.get_reader",
        return_value=dummy_reader,
    ) as mock_get_reader:
        df = reader.read_file(str(test_file))

    assert df.count() == 1
    # No KeyError was raised, and the default's format is passed on to the factory.
    assert mock_get_reader.call_args.args[0] == "csv"
    # The reader receives the validated default config.
    assert dummy_reader.read.call_args.args[1].source.name == "default_csv"
