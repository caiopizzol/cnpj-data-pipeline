"""Reject invalid benchmark requests before opening a database."""

from pathlib import Path
from unittest.mock import patch

import pytest

from scripts.benchmark_postgres import measure


@pytest.mark.parametrize(
    "batch,strategy,url,message",
    [
        (0, "upsert", "unused", "positive"),
        (1, "bad", "unused", "Strategy"),
        (1, "upsert", "", "BENCHMARK_DATABASE_URL"),
    ],
)
def test_invalid_arguments(tmp_path: Path, batch: int, strategy: str, url: str, message: str) -> None:
    with patch("psycopg2.connect") as connect:
        with pytest.raises(ValueError, match=message):
            measure(tmp_path / "missing", tmp_path / "output", batch, strategy, url)
    connect.assert_not_called()


def test_empty_source(tmp_path: Path) -> None:
    source = tmp_path / "CNAECSV.csv"
    source.touch()
    with patch("psycopg2.connect") as connect:
        with pytest.raises(ValueError, match="no rows"):
            measure(source, tmp_path / "output", 1, "upsert", "unused")
    connect.assert_not_called()


def test_unknown_source_rejected_before_connecting(tmp_path: Path) -> None:
    source = tmp_path / "unknown.csv"
    source.write_text("01;One\n")
    with patch("psycopg2.connect") as connect:
        with pytest.raises(ValueError, match="Unknown source"):
            measure(source, tmp_path / "output", 1, "upsert", "unused")
    connect.assert_not_called()
