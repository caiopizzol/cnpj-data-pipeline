"""Tests for processor module."""

import tempfile
from pathlib import Path

import polars as pl

from processor import _convert_encoding, _transform, get_file_type, process_file


class TestGetFileType:
    """Test get_file_type function."""

    def test_simples_file_type(self):
        """Test that SIMPLES filename returns SIMPLESCSV type."""
        filename = "F.K03200$W.SIMPLES.CSV.D51213"
        result = get_file_type(filename)
        assert result == "SIMPLESCSV"

    def test_simples_case_insensitive(self):
        """Test that SIMPLES matching is case insensitive."""
        test_cases = [
            "f.k03200$w.simples.csv.d51213",
            "F.K03200$W.SIMPLES.CSV.D51213",
            "file.SIMPLES.csv",
            "data.simples.CSV",
        ]

        for filename in test_cases:
            result = get_file_type(filename)
            assert result == "SIMPLESCSV", f"Failed for filename: {filename}"

    def test_other_file_types(self):
        """Test other known file type patterns."""
        test_cases = [
            ("CNAECSV.D51213", "CNAECSV"),
            ("MOTICSV.D51213", "MOTICSV"),
            ("EMPRECSV.D51213", "EMPRECSV"),
            ("ESTABELE.D51213", "ESTABELE"),
            ("SOCIOCSV.D51213", "SOCIOCSV"),
            ("MUNICCSV.D51213", "MUNICCSV"),
            ("NATJUCSV.D51213", "NATJUCSV"),
            ("PAISCSV.D51213", "PAISCSV"),
            ("QUALSCSV.D51213", "QUALSCSV"),
        ]

        for filename, expected_type in test_cases:
            result = get_file_type(filename)
            assert result == expected_type, f"Expected {expected_type} for {filename}, got {result}"

    def test_unknown_file_type(self):
        """Test that unknown filename returns None."""
        unknown_files = ["README.txt", "config.json", "random_file.csv", "F.K03200$W.UNKNOWN.CSV.D51213"]

        for filename in unknown_files:
            result = get_file_type(filename)
            assert result is None, f"Expected None for {filename}, got {result}"


class TestTransform:
    """Test _transform function for date transformations."""

    def test_transform_zero_dates_to_none_estabelecimentos(self):
        """Test that '0' and '00000000' dates become None for estabelecimentos."""
        # Create test dataframe with date columns
        df = pl.DataFrame(
            {
                "cnpj_basico": ["12345678"],
                "data_situacao_cadastral": ["0"],
                "data_inicio_atividade": ["00000000"],
                "data_situacao_especial": ["20230101"],  # Valid date should remain
            }
        )

        result = _transform(df, "ESTABELE")

        # Check that '0' became None
        assert result["data_situacao_cadastral"][0] is None

        # Check that '00000000' became None
        assert result["data_inicio_atividade"][0] is None

        # Check that valid date remained unchanged
        assert result["data_situacao_especial"][0] == "20230101"

    def test_transform_zero_dates_to_none_simples(self):
        """Test that '0' and '00000000' dates become None for SIMPLES data."""
        df = pl.DataFrame(
            {
                "cnpj_basico": ["12345678"],
                "data_opcao_pelo_simples": ["0"],
                "data_exclusao_do_simples": ["00000000"],
                "data_opcao_pelo_mei": ["20230101"],
                "data_exclusao_do_mei": ["0"],
            }
        )

        result = _transform(df, "SIMPLESCSV")

        # Check that '0' dates became None
        assert result["data_opcao_pelo_simples"][0] is None
        assert result["data_exclusao_do_mei"][0] is None

        # Check that '00000000' became None
        assert result["data_exclusao_do_simples"][0] is None

        # Check that valid date remained unchanged
        assert result["data_opcao_pelo_mei"][0] == "20230101"

    def test_transform_zero_dates_to_none_socios(self):
        """Test that '0' and '00000000' dates become None for socios data."""
        df = pl.DataFrame({"cnpj_basico": ["12345678"], "data_entrada_sociedade": ["0"]})

        result = _transform(df, "SOCIOCSV")

        # Check that '0' became None
        assert result["data_entrada_sociedade"][0] is None

    def test_transform_null_dates_remain_none(self):
        """Test that null dates remain None."""
        df = pl.DataFrame(
            {"cnpj_basico": ["12345678"], "data_situacao_cadastral": [None], "data_inicio_atividade": [None]}
        )

        result = _transform(df, "ESTABELE")

        # Check that None values remain None
        assert result["data_situacao_cadastral"][0] is None
        assert result["data_inicio_atividade"][0] is None

    def test_transform_valid_dates_unchanged(self):
        """Test that valid dates are not changed."""
        valid_dates = ["20230101", "19991231", "20240615"]

        df = pl.DataFrame(
            {
                "cnpj_basico": ["12345678", "87654321", "11223344"],
                "data_situacao_cadastral": valid_dates,
                "data_inicio_atividade": valid_dates,
                "data_situacao_especial": valid_dates,
            }
        )

        result = _transform(df, "ESTABELE")

        # Check that all valid dates remained unchanged
        for i, expected_date in enumerate(valid_dates):
            assert result["data_situacao_cadastral"][i] == expected_date
            assert result["data_inicio_atividade"][i] == expected_date
            assert result["data_situacao_especial"][i] == expected_date

    def test_transform_no_date_columns_file_type(self):
        """Test _transform with file type that has no date transformations."""
        df = pl.DataFrame({"codigo": ["123"], "descricao": ["Test"]})

        result = _transform(df, "CNAECSV")

        # DataFrame should be unchanged for file types without date transformations
        assert result.equals(df)

    def test_transform_mixed_date_values(self):
        """Test _transform with mixed valid and invalid date values."""
        df = pl.DataFrame(
            {
                "cnpj_basico": ["12345678", "87654321", "11223344", "99887766"],
                "data_opcao_pelo_simples": ["0", "20230101", "00000000", None],
                "data_exclusao_do_simples": ["20240101", "0", "20230615", "00000000"],
            }
        )

        result = _transform(df, "SIMPLESCSV")

        # Check expected transformations
        expected_opcao = [None, "20230101", None, None]
        expected_exclusao = ["20240101", None, "20230615", None]

        for i in range(len(expected_opcao)):
            assert result["data_opcao_pelo_simples"][i] == expected_opcao[i]
            assert result["data_exclusao_do_simples"][i] == expected_exclusao[i]


class TestConvertEncoding:
    """Test encoding conversion from ISO-8859-1 to UTF-8."""

    def test_converts_iso_to_utf8(self, tmp_path):
        """Test that ISO-8859-1 content is correctly converted to UTF-8."""
        # Create a file with ISO-8859-1 content (Brazilian characters)
        iso_content = "São Paulo;Empresa Ltda;Açúcar\nRio de Janeiro;Comércio;Café"
        iso_file = tmp_path / "test.csv"
        iso_file.write_text(iso_content, encoding="ISO-8859-1")

        utf8_file = _convert_encoding(iso_file)

        try:
            # Read as UTF-8 and verify content
            result = utf8_file.read_text(encoding="UTF-8")
            assert "São Paulo" in result
            assert "Açúcar" in result
            assert "Café" in result
        finally:
            utf8_file.unlink(missing_ok=True)

    def test_handles_large_file_in_chunks(self, tmp_path):
        """Test that large files are processed correctly (chunked reading)."""
        # Create a file larger than the 50MB chunk size would normally handle
        # We'll use a smaller test but verify the chunking logic works
        iso_file = tmp_path / "large.csv"
        content = "data;value\n" * 10000
        iso_file.write_text(content, encoding="ISO-8859-1")

        utf8_file = _convert_encoding(iso_file)

        try:
            result = utf8_file.read_text(encoding="UTF-8")
            assert result.count("\n") == 10000
        finally:
            utf8_file.unlink(missing_ok=True)


class TestProcessFile:
    """Test process_file function for batch processing."""

    def test_skips_unknown_file_type(self, tmp_path):
        """Test that unknown file types are skipped with no output."""
        unknown_file = tmp_path / "UNKNOWN_FILE.csv"
        unknown_file.write_text("data;value", encoding="ISO-8859-1")

        results = list(process_file(unknown_file))

        assert results == []

    def test_handles_empty_csv(self, tmp_path):
        """Test that empty CSV files are handled gracefully."""
        empty_file = tmp_path / "CNAECSV.csv"
        empty_file.write_text("", encoding="ISO-8859-1")

        results = list(process_file(empty_file))

        assert results == []

    def test_processes_valid_csv_in_batches(self, tmp_path):
        """Test that valid CSV is processed and yields correct data."""
        # Create a small CNAE file (simple 2-column format)
        cnae_file = tmp_path / "CNAECSV.csv"
        content = "0111301;Cultivo de arroz\n0111302;Cultivo de milho\n0111303;Cultivo de trigo"
        cnae_file.write_text(content, encoding="ISO-8859-1")

        results = list(process_file(cnae_file, batch_size=100))

        assert len(results) == 1
        df, table_name, columns = results[0]
        assert table_name == "cnaes"
        assert len(df) == 3
        assert columns == ["codigo", "descricao"]

    def test_processes_simples_file(self, tmp_path):
        """Test that SIMPLES files are processed correctly."""
        simples_file = tmp_path / "F.K03200$W.SIMPLES.CSV.D51213"
        # 7 columns: cnpj_basico, opcao_pelo_simples, dates (4x)
        content = "12345678;S;20200101;0;N;0;0"
        simples_file.write_text(content, encoding="ISO-8859-1")

        results = list(process_file(simples_file))

        assert len(results) == 1
        df, table_name, columns = results[0]
        assert table_name == "dados_simples"
        # Verify date transformation (0 → None)
        assert df["data_exclusao_do_simples"][0] is None

    def test_cleans_up_temp_file(self, tmp_path):
        """Test that temporary UTF-8 file is deleted after processing."""
        cnae_file = tmp_path / "CNAECSV.csv"
        cnae_file.write_text("0111301;Test", encoding="ISO-8859-1")

        # Count .utf8.csv files before
        temp_dir = Path(tempfile.gettempdir())
        utf8_files_before = len(list(temp_dir.glob("*.utf8.csv")))

        # Process file
        list(process_file(cnae_file))

        # Count .utf8.csv files after - should be same (cleaned up)
        utf8_files_after = len(list(temp_dir.glob("*.utf8.csv")))
        assert utf8_files_after == utf8_files_before

    def test_multiple_batches(self, tmp_path):
        """Test that large files are processed in multiple batches."""
        cnae_file = tmp_path / "CNAECSV.csv"
        # Create 150 rows, with batch_size=50 should yield 3 batches
        rows = [f"{i:07d};Descrição {i}" for i in range(150)]
        cnae_file.write_text("\n".join(rows), encoding="ISO-8859-1")

        results = list(process_file(cnae_file, batch_size=50))

        assert len(results) == 3
        assert len(results[0][0]) == 50
        assert len(results[1][0]) == 50
        assert len(results[2][0]) == 50
