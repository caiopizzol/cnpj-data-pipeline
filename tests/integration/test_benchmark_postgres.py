"""Exercise benchmark isolation, real writes and independent output verification."""

import json
import runpy
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

import polars as pl
import psycopg2
import pytest

from database import Database
from scripts.benchmark_postgres import measure, temporary_database
from tests.integration.support import FIXTURES_DIR

URL = "postgresql://postgres:postgres@localhost:5435/postgres"
pytestmark = pytest.mark.usefixtures("postgres_available")


def databases() -> set[str]:
    with closing(psycopg2.connect(URL)) as conn, conn.cursor() as cur:
        cur.execute("SELECT datname FROM pg_database")
        return {row[0] for row in cur.fetchall()}


@pytest.mark.parametrize("strategy", ["upsert", "replace"])
@pytest.mark.parametrize("name", ["EMPRECSV.csv", "SOCIOCSV.csv", "ESTABELE.csv"])
def test_measure_real_source(tmp_path: Path, strategy: str, name: str) -> None:
    before = databases()
    output = tmp_path / "result"
    result = measure(FIXTURES_DIR / name, output, 50, strategy, URL)
    report = json.loads((output / "measurement.json").read_text())
    assert report == result
    assert report["rows"] == report["stored_rows"]
    assert report["duplicate_rows"] == 0
    assert report["batches"] > 1
    assert report["writing_seconds"] > 0
    assert report["processing_seconds"] > 0
    assert report["total_seconds"] >= report["writing_seconds"] + report["processing_seconds"]
    assert report["peak_client_rss_bytes_before_verification"] > 0
    assert report["postgres_settings"]["fsync"]["value"] == "on"
    assert URL not in json.dumps(report)
    assert databases() == before


@pytest.mark.parametrize("strategy", ["upsert", "replace"])
def test_identical_duplicates_are_counted(tmp_path: Path, strategy: str) -> None:
    source = tmp_path / "CNAECSV.csv"
    source.write_text('"01";"Café"\n' * 10000, encoding="iso-8859-1")
    measure(source, tmp_path / "result", 50, strategy, URL)
    report = json.loads((tmp_path / "result/measurement.json").read_text())
    assert report["rows"] == 10000
    assert report["stored_rows"] == 1
    assert report["duplicate_rows"] == 9999
    assert report["batches"] > 1


@pytest.mark.parametrize("corrupt", ["drop", "change"])
def test_rejects_silent_loader_corruption(tmp_path: Path, corrupt: str) -> None:
    source = tmp_path / "CNAECSV.csv"
    source.write_text("01;One\n02;Two\n")
    before = databases()
    original = Database.bulk_upsert

    def broken(db: Database, frame: pl.DataFrame, table: str, columns: list[str]) -> None:
        original(db, frame, table, columns)
        with db.connect().cursor() as cur:
            cur.execute(
                "DELETE FROM cnaes WHERE codigo = '02'"
                if corrupt == "drop"
                else "UPDATE cnaes SET descricao = 'wrong' WHERE codigo = '02'"
            )
        db.connect().commit()

    with patch.object(Database, "bulk_upsert", broken):
        with pytest.raises(ValueError, match="Stored values differ"):
            measure(source, tmp_path / "result", 50, "upsert", URL)
    assert not (tmp_path / "result/measurement.json").exists()
    assert databases() == before


@pytest.mark.parametrize("copies", [1, 10000])
def test_conflicting_duplicates_fail_verification(tmp_path: Path, copies: int) -> None:
    source = tmp_path / "CNAECSV.csv"
    source.write_text("01;First\n" * copies + "01;Second\n" * copies)
    with pytest.raises(ValueError, match="conflicting duplicate"):
        measure(source, tmp_path / "result", 50, "upsert", URL)
    assert not (tmp_path / "result/measurement.json").exists()


def test_cleans_up_after_schema_failure() -> None:
    before = databases()
    with patch.object(Database, "ensure_schema", side_effect=RuntimeError("schema failed")):
        with pytest.raises(RuntimeError, match="schema failed"):
            with temporary_database(URL):
                pytest.fail("setup must fail")
    assert databases() == before


def test_cli_uses_explicit_benchmark_url(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "CNAECSV.csv"
    source.write_text("01;One\n")
    output = tmp_path / "result"
    monkeypatch.setenv("BENCHMARK_DATABASE_URL", URL)
    monkeypatch.setenv("DATABASE_URL", "invalid:must-not-use")
    monkeypatch.setattr("sys.argv", ["benchmark", str(source), str(output)])
    runpy.run_path(str(Path(__file__).resolve().parents[2] / "scripts/benchmark_postgres.py"), run_name="__main__")
    assert json.loads((output / "measurement.json").read_text())["stored_rows"] == 1
    with pytest.raises(FileExistsError):
        measure(source, output, 50, "upsert", URL)


def test_failed_load_removes_database_without_report(tmp_path: Path) -> None:
    source = tmp_path / "CNAECSV.csv"
    source.write_text("01;One\n")
    before = databases()
    with patch.object(Database, "bulk_upsert", side_effect=RuntimeError("write failed")):
        with pytest.raises(RuntimeError, match="write failed"):
            measure(source, tmp_path / "result", 50, "upsert", URL)
    assert not (tmp_path / "result/measurement.json").exists()
    assert databases() == before


def test_does_not_adopt_or_drop_existing_database() -> None:
    with temporary_database(URL) as owned:
        name = owned.connect().info.dbname
        with patch("scripts.benchmark_postgres.uuid4") as identifier:
            identifier.return_value.hex = name.removeprefix("cnpj_benchmark_")
            with pytest.raises(psycopg2.errors.DuplicateDatabase):
                with temporary_database(URL):
                    pytest.fail("collision must fail")
        assert name in databases()


@pytest.mark.parametrize("hashes", [["original", "changed"], ["original", "original", "changed"]])
def test_detects_source_changes_before_publishing(tmp_path: Path, hashes: list[str]) -> None:
    source = tmp_path / "CNAECSV.csv"
    source.write_text("01;One\n")
    with patch("scripts.benchmark_postgres.sha256", side_effect=hashes):
        with pytest.raises(ValueError, match="Source changed"):
            measure(source, tmp_path / "result", 50, "upsert", URL)
    assert not (tmp_path / "result/measurement.json").exists()


def test_detects_missing_processed_rows(tmp_path: Path) -> None:
    source = tmp_path / "CNAECSV.csv"
    source.write_text("01;One\n02;Two\n")

    def truncated(*_args: object, **_kwargs: object):
        yield pl.DataFrame({"codigo": ["01"], "descricao": ["One"]}), "cnaes", ["codigo", "descricao"]

    with patch("scripts.benchmark_postgres.process_file", truncated):
        with pytest.raises(ValueError, match="2 rows, processed 1"):
            measure(source, tmp_path / "result", 50, "upsert", URL)
    assert not (tmp_path / "result/measurement.json").exists()


def test_verification_uses_column_names_not_loader_order(tmp_path: Path) -> None:
    from processor import process_file

    source = tmp_path / "CNAECSV.csv"
    source.write_text("01;One\n02;Two\n")

    def reordered(path: Path, batch_size: int):
        for batch, table, columns in process_file(path, batch_size=batch_size):
            yield batch.select(list(reversed(batch.columns))), table, columns

    with patch("scripts.benchmark_postgres.process_file", reordered):
        with pytest.raises(ValueError, match="Stored values differ"):
            measure(source, tmp_path / "result", 50, "upsert", URL)
    assert not (tmp_path / "result/measurement.json").exists()
