"""Tests for Parquet writer."""

import json
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest

from parquet_writer import ParquetWriter


@pytest.fixture
def output_dir(tmp_path: Path):
    return tmp_path / "parquet_output"


@pytest.fixture
def writer(output_dir: Path):
    return ParquetWriter(output_dir)


@pytest.fixture
def sample_empresas():
    return pl.DataFrame(
        {
            "cnpj_basico": ["00000000", "11111111", "22222222"],
            "razao_social": ["EMPRESA A", "EMPRESA B", "EMPRESA C"],
            "capital_social": ["1000.00", "2000.00", "3000.00"],
        }
    )


@pytest.fixture
def sample_estabelecimentos():
    return pl.DataFrame(
        {
            "cnpj_basico": ["00000000", "11111111", "22222222", "33333333"],
            "cnpj_ordem": ["0001", "0001", "0001", "0001"],
            "uf": ["SP", "SP", "RJ", "MG"],
            "municipio": ["7107", "7107", "6001", "4123"],
        }
    )


class TestWriteBatch:
    def test_writes_single_file(self, writer: ParquetWriter, sample_empresas: pl.DataFrame, output_dir: Path) -> None:
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        writer.close()

        path = output_dir / "empresas.parquet"
        assert path.exists()
        assert pl.read_parquet(str(path)).height == 3

    def test_returns_row_count(self, writer: ParquetWriter, sample_empresas: pl.DataFrame) -> None:
        rows = writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        assert rows == 3

    def test_accumulates_rows_in_same_file(
        self, writer: ParquetWriter, sample_empresas: pl.DataFrame, output_dir: Path
    ) -> None:
        """Multiple batches go to the same file."""
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        writer.close()

        table = pl.read_parquet(str(output_dir / "empresas.parquet"))
        assert table.height == 6
        assert writer.stats["empresas"].rows == 6

    def test_estabelecimentos_writes_single_file(
        self, writer: ParquetWriter, sample_estabelecimentos: pl.DataFrame, output_dir: Path
    ) -> None:
        """Estabelecimentos goes to a single file (no UF partitioning)."""
        writer.write_batch(
            sample_estabelecimentos,
            "estabelecimentos",
            ["cnpj_basico", "cnpj_ordem", "uf", "municipio"],
        )
        writer.close()

        assert (output_dir / "estabelecimentos.parquet").exists()
        table = pl.read_parquet(str(output_dir / "estabelecimentos.parquet"))
        assert table.height == 4


class TestFlushTable:
    def test_returns_flushed_file_path(
        self, writer: ParquetWriter, sample_empresas: pl.DataFrame, output_dir: Path
    ) -> None:
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        path = writer.flush_table("empresas")

        assert path == output_dir / "empresas.parquet"
        assert path is not None
        assert path.exists()

    def test_returns_none_for_unknown_table(self, writer: ParquetWriter) -> None:
        assert writer.flush_table("nonexistent") is None

    def test_clears_writer_after_flush(self, writer: ParquetWriter, sample_empresas: pl.DataFrame) -> None:
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])

        assert writer.flush_table("empresas") is not None
        assert writer.flush_table("empresas") is None

    def test_tracks_file_size(self, writer: ParquetWriter, sample_empresas: pl.DataFrame) -> None:
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        writer.flush_table("empresas")

        assert writer.stats["empresas"].size_bytes > 0
        assert writer.stats["empresas"].file == "empresas.parquet"


class TestClose:
    def test_closes_all_writers(
        self, writer: ParquetWriter, sample_empresas: pl.DataFrame, sample_estabelecimentos: pl.DataFrame
    ) -> None:
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        writer.write_batch(
            sample_estabelecimentos, "estabelecimentos", ["cnpj_basico", "cnpj_ordem", "uf", "municipio"]
        )

        writer.close()
        assert writer.stats["empresas"].size_bytes > 0
        assert writer.stats["estabelecimentos"].size_bytes > 0
        assert writer.flush_table("empresas") is None
        assert writer.flush_table("estabelecimentos") is None

    def test_computes_file_sizes(self, writer: ParquetWriter, sample_empresas: pl.DataFrame) -> None:
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        writer.close()

        assert writer.stats["empresas"].size_bytes > 0


class TestWriteManifest:
    def test_writes_manifest_json(
        self,
        writer: ParquetWriter,
        sample_empresas: pl.DataFrame,
        sample_estabelecimentos: pl.DataFrame,
        output_dir: Path,
    ) -> None:
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        writer.write_batch(
            sample_estabelecimentos,
            "estabelecimentos",
            ["cnpj_basico", "cnpj_ordem", "uf", "municipio"],
        )
        writer.close()
        writer.write_manifest()

        manifest_path = output_dir / "manifest.json"
        assert manifest_path.exists()

        saved = json.loads(manifest_path.read_text())
        assert saved["totals"]["rows"] == 7
        assert saved["totals"]["files"] > 0
        assert saved["totals"]["sizeBytes"] > 0
        assert "empresas" in saved["tables"]
        assert "estabelecimentos" in saved["tables"]

    def test_manifest_has_exported_at(
        self, writer: ParquetWriter, sample_empresas: pl.DataFrame, output_dir: Path
    ) -> None:
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        writer.close()
        manifest = writer.write_manifest()

        assert "exportedAt" in manifest
        assert manifest["exportedAt"].endswith("Z")

    def test_manifest_has_pipeline_and_schema_versions(
        self, writer: ParquetWriter, sample_empresas: pl.DataFrame, output_dir: Path
    ) -> None:
        """Manifest metadata includes pipeline and layout versions, not a physical schema fingerprint."""
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        writer.close()
        manifest = writer.write_manifest()

        assert "pipelineVersion" in manifest
        assert manifest["pipelineVersion"]  # non-empty
        assert "schemaVersion" in manifest
        assert manifest["schemaVersion"] == "2"

    def test_manifest_records_source_month_when_provided(
        self, writer: ParquetWriter, sample_empresas: pl.DataFrame, output_dir: Path
    ) -> None:
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        writer.close()
        manifest = writer.write_manifest(source_month="2024-11")

        assert manifest["sourceMonth"] == "2024-11"

    def test_manifest_source_month_optional(
        self, writer: ParquetWriter, sample_empresas: pl.DataFrame, output_dir: Path
    ) -> None:
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        writer.close()
        manifest = writer.write_manifest()

        assert "sourceMonth" in manifest
        assert manifest["sourceMonth"] is None


class TestThreadSafety:
    def test_concurrent_writes_produce_correct_row_count(self, writer: ParquetWriter, output_dir: Path) -> None:
        """Multiple threads writing simultaneously should not lose data."""
        import threading

        errors: list[Exception] = []

        def write_batch(thread_id: int):
            try:
                df = pl.DataFrame(
                    {
                        "codigo": [f"{thread_id:03d}{i:04d}" for i in range(100)],
                        "descricao": [f"Thread {thread_id} item {i}" for i in range(100)],
                    }
                )
                writer.write_batch(df, "cnaes", ["codigo", "descricao"])
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=write_batch, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        writer.close()

        assert not errors, f"Errors during concurrent writes: {errors}"

        table = pl.read_parquet(str(output_dir / "cnaes.parquet"))
        assert table.height == 1000  # 10 threads x 100 rows
        assert writer.stats["cnaes"].rows == 1000


class TestZstdCompression:
    def test_output_uses_zstd(self, writer: ParquetWriter, sample_empresas: pl.DataFrame, output_dir: Path) -> None:
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        writer.close()

        meta = pq.ParquetFile(str(output_dir / "empresas.parquet")).metadata
        compression = meta.row_group(0).column(0).compression
        assert compression == "ZSTD"
