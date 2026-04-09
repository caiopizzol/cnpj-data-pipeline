"""Tests for Parquet writer."""

import json

import polars as pl
import pyarrow.parquet as pq
import pytest

from parquet_writer import ParquetWriter


@pytest.fixture
def output_dir(tmp_path):
    return tmp_path / "parquet_output"


@pytest.fixture
def writer(output_dir):
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
    def test_writes_single_file(self, writer, sample_empresas, output_dir):
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        writer.close()

        path = output_dir / "empresas.parquet"
        assert path.exists()
        assert pq.read_table(str(path)).num_rows == 3

    def test_returns_row_count(self, writer, sample_empresas):
        rows = writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        assert rows == 3

    def test_accumulates_rows_in_same_file(self, writer, sample_empresas, output_dir):
        """Multiple batches go to the same file."""
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        writer.close()

        table = pq.read_table(str(output_dir / "empresas.parquet"))
        assert table.num_rows == 6
        assert writer.stats["empresas"].rows == 6

    def test_estabelecimentos_writes_single_file(self, writer, sample_estabelecimentos, output_dir):
        """Estabelecimentos goes to a single file (no UF partitioning)."""
        writer.write_batch(
            sample_estabelecimentos,
            "estabelecimentos",
            ["cnpj_basico", "cnpj_ordem", "uf", "municipio"],
        )
        writer.close()

        assert (output_dir / "estabelecimentos.parquet").exists()
        table = pq.read_table(str(output_dir / "estabelecimentos.parquet"))
        assert table.num_rows == 4


class TestFlushTable:
    def test_returns_flushed_file_path(self, writer, sample_empresas, output_dir):
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        path = writer.flush_table("empresas")

        assert path == output_dir / "empresas.parquet"
        assert path.exists()

    def test_returns_none_for_unknown_table(self, writer):
        assert writer.flush_table("nonexistent") is None

    def test_clears_writer_after_flush(self, writer, sample_empresas):
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        assert "empresas" in writer._writers

        writer.flush_table("empresas")
        assert "empresas" not in writer._writers

    def test_tracks_file_size(self, writer, sample_empresas):
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        writer.flush_table("empresas")

        assert writer.stats["empresas"].size_bytes > 0
        assert writer.stats["empresas"].file == "empresas.parquet"


class TestClose:
    def test_closes_all_writers(self, writer, sample_empresas, sample_estabelecimentos):
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        writer.write_batch(
            sample_estabelecimentos, "estabelecimentos", ["cnpj_basico", "cnpj_ordem", "uf", "municipio"]
        )
        assert len(writer._writers) == 2

        writer.close()
        assert len(writer._writers) == 0

    def test_computes_file_sizes(self, writer, sample_empresas):
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        writer.close()

        assert writer.stats["empresas"].size_bytes > 0


class TestWriteManifest:
    def test_writes_manifest_json(self, writer, sample_empresas, sample_estabelecimentos, output_dir):
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

    def test_manifest_has_exported_at(self, writer, sample_empresas, output_dir):
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        writer.close()
        manifest = writer.write_manifest()

        assert "exportedAt" in manifest
        assert manifest["exportedAt"].endswith("Z")


class TestThreadSafety:
    def test_concurrent_writes_produce_correct_row_count(self, writer, output_dir):
        """Multiple threads writing simultaneously should not lose data."""
        import threading

        errors = []

        def write_batch(thread_id):
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

        table = pq.read_table(str(output_dir / "cnaes.parquet"))
        assert table.num_rows == 1000  # 10 threads x 100 rows
        assert writer.stats["cnaes"].rows == 1000


class TestZstdCompression:
    def test_output_uses_zstd(self, writer, sample_empresas, output_dir):
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        writer.close()

        meta = pq.read_metadata(str(output_dir / "empresas.parquet"))
        compression = meta.row_group(0).column(0).compression
        assert compression == "ZSTD"
