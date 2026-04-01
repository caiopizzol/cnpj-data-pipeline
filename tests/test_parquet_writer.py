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
    def test_writes_single_file_for_regular_table(self, writer, sample_empresas, output_dir):
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        writer.close()

        path = output_dir / "empresas.parquet"
        assert path.exists()

        table = pq.read_table(str(path))
        assert table.num_rows == 3

    def test_returns_row_count(self, writer, sample_empresas):
        rows = writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        assert rows == 3

    def test_accumulates_rows_across_batches(self, writer, sample_empresas, output_dir):
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        writer.close()

        table = pq.read_table(str(output_dir / "empresas.parquet"))
        assert table.num_rows == 6
        assert writer.stats["empresas"].rows == 6

    def test_partitions_estabelecimentos_by_uf(self, writer, sample_estabelecimentos, output_dir):
        writer.write_batch(
            sample_estabelecimentos,
            "estabelecimentos",
            ["cnpj_basico", "cnpj_ordem", "uf", "municipio"],
        )
        writer.close()

        estab_dir = output_dir / "estabelecimentos"
        assert (estab_dir / "uf=SP.parquet").exists()
        assert (estab_dir / "uf=RJ.parquet").exists()
        assert (estab_dir / "uf=MG.parquet").exists()

        sp_table = pq.read_table(str(estab_dir / "uf=SP.parquet"))
        assert sp_table.num_rows == 2

        rj_table = pq.read_table(str(estab_dir / "uf=RJ.parquet"))
        assert rj_table.num_rows == 1

    def test_does_not_partition_if_uf_not_in_columns(self, writer, output_dir):
        df = pl.DataFrame({"cnpj_basico": ["00000000"], "nome_fantasia": ["TEST"]})
        writer.write_batch(df, "estabelecimentos", ["cnpj_basico", "nome_fantasia"])
        writer.close()

        assert (output_dir / "estabelecimentos.parquet").exists()
        assert not (output_dir / "estabelecimentos").is_dir()


class TestFlush:
    def test_returns_flushed_file_paths(self, writer, sample_empresas, output_dir):
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        flushed = writer.flush()

        assert len(flushed) == 1
        assert flushed[0] == output_dir / "empresas.parquet"
        assert flushed[0].exists()

    def test_clears_writers_after_flush(self, writer, sample_empresas):
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        assert len(writer._writers) > 0

        writer.flush()
        assert len(writer._writers) == 0

    def test_can_write_after_flush(self, writer, sample_empresas, output_dir):
        """After flush, new writes create new files (separate row groups)."""
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        flushed1 = writer.flush()

        # Write again — this reopens the same file
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        flushed2 = writer.flush()

        assert len(flushed1) == 1
        assert len(flushed2) == 1
        # Both flushes wrote to the same path
        assert flushed1[0] == flushed2[0]

    def test_flush_partitioned_returns_all_files(self, writer, sample_estabelecimentos, output_dir):
        writer.write_batch(
            sample_estabelecimentos,
            "estabelecimentos",
            ["cnpj_basico", "cnpj_ordem", "uf", "municipio"],
        )
        flushed = writer.flush()

        # Should have 3 files: SP, RJ, MG
        assert len(flushed) == 3
        flushed_names = {f.name for f in flushed}
        assert "uf=SP.parquet" in flushed_names
        assert "uf=RJ.parquet" in flushed_names
        assert "uf=MG.parquet" in flushed_names


class TestClose:
    def test_computes_file_sizes(self, writer, sample_empresas):
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        writer.close()

        assert writer.stats["empresas"].size_bytes > 0
        assert len(writer.stats["empresas"].files) == 1

    def test_clears_writers(self, writer, sample_empresas):
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        assert len(writer._writers) > 0

        writer.close()
        assert len(writer._writers) == 0


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


class TestZstdCompression:
    def test_output_uses_zstd(self, writer, sample_empresas, output_dir):
        writer.write_batch(sample_empresas, "empresas", ["cnpj_basico", "razao_social", "capital_social"])
        writer.close()

        meta = pq.read_metadata(str(output_dir / "empresas.parquet"))
        compression = meta.row_group(0).column(0).compression
        assert compression == "ZSTD"
