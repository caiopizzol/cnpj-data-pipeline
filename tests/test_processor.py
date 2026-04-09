"""Tests for processor module."""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from processor import _convert_encoding, _transform, _validate, get_file_type, process_file


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

    def test_transform_capital_social(self):
        """Test that capital social is converted from Brazilian to standard decimal."""
        df = pl.DataFrame(
            {
                "cnpj_basico": ["12345678"],
                "capital_social": ["1.234.567,89"],
            }
        )

        result = _transform(df, "EMPRECSV")

        assert result["capital_social"][0] == "1234567.89"

    def test_transform_negative_capital_social(self):
        """Test that negative capital social becomes null."""
        df = pl.DataFrame(
            {
                "cnpj_basico": ["12345678", "87654321"],
                "capital_social": ["-1.000,00", "5.000,00"],
            }
        )

        result = _transform(df, "EMPRECSV")

        assert result["capital_social"][0] is None
        assert result["capital_social"][1] == "5000.00"

    def test_transform_country_code_padding(self):
        """Test that country codes are zero-padded to 3 digits."""
        df = pl.DataFrame(
            {
                "cnpj_basico": ["12345678"],
                "pais": ["1"],
            }
        )

        result = _transform(df, "ESTABELE")

        assert result["pais"][0] == "001"

    def test_transform_null_cpf_fill(self):
        """Test that null cnpj_cpf_do_socio is filled with zeros."""
        df = pl.DataFrame(
            {
                "cnpj_basico": ["12345678"],
                "cnpj_cpf_do_socio": [None],
            }
        )

        result = _transform(df, "SOCIOCSV")

        assert result["cnpj_cpf_do_socio"][0] == "00000000000000"


class TestValidate:
    """Test _validate function for format validation."""

    def test_validate_cnpj_basico_format(self):
        """Test that cnpj_basico must be exactly 8 digits."""
        df = pl.DataFrame({"cnpj_basico": ["12345678", "1234", "ABCDEFGH", None]})

        result = _validate(df, "EMPRECSV")

        # Validation logs but doesn't nullify format errors (keeps raw values)
        assert result["cnpj_basico"][0] == "12345678"
        assert result["cnpj_basico"][1] == "1234"

    def test_validate_situacao_cadastral(self):
        """Test that situacao_cadastral must be 01, 02, 03, 04, or 08."""
        df = pl.DataFrame({"situacao_cadastral": ["02", "08", "99", None]})

        result = _validate(df, "ESTABELE")

        # Logs the invalid "99" but keeps it
        assert result["situacao_cadastral"][2] == "99"

    def test_validate_uf(self):
        """Test that UF must be a valid Brazilian state code."""
        df = pl.DataFrame({"uf": ["SP", "RJ", "XX", None]})

        result = _validate(df, "ESTABELE")

        # Logs invalid "XX" but keeps it
        assert result["uf"][2] == "XX"

    def test_validate_opcao_simples(self):
        """Test that opcao_pelo_simples must be S or N."""
        df = pl.DataFrame({"opcao_pelo_simples": ["S", "N", "X", None]})

        result = _validate(df, "SIMPLESCSV")

        assert result["opcao_pelo_simples"][2] == "X"

    def test_validate_identificador_socio(self):
        """Test that identificador_de_socio must be 1, 2, or 3."""
        df = pl.DataFrame({"identificador_de_socio": ["1", "2", "3", "9"]})

        result = _validate(df, "SOCIOCSV")

        assert result["identificador_de_socio"][3] == "9"

    def test_validate_invalid_date_format_nullified(self):
        """Test that dates with invalid format (not YYYYMMDD) are nullified."""
        df = pl.DataFrame({"data_situacao_cadastral": ["20230101", "2023-01-01", "INVALID", None]})

        result = _validate(df, "ESTABELE")

        assert result["data_situacao_cadastral"][0] == "20230101"
        assert result["data_situacao_cadastral"][1] is None
        assert result["data_situacao_cadastral"][2] is None
        assert result["data_situacao_cadastral"][3] is None

    def test_validate_date_invalid_month_day(self):
        """Test that dates with invalid month/day are nullified."""
        df = pl.DataFrame({"data_situacao_cadastral": ["20231301", "20230132", "20230615"]})

        result = _validate(df, "ESTABELE")

        assert result["data_situacao_cadastral"][0] is None  # month 13
        assert result["data_situacao_cadastral"][1] is None  # day 32
        assert result["data_situacao_cadastral"][2] == "20230615"

    def test_validate_date_impossible_calendar_dates(self):
        """Test that impossible calendar dates (Feb 30, Apr 31) are nullified."""
        df = pl.DataFrame({"data_situacao_cadastral": ["20230229", "20230431", "20240230", "20240229", "20230115"]})

        result = _validate(df, "ESTABELE")

        assert result["data_situacao_cadastral"][0] is None  # Feb 29 non-leap
        assert result["data_situacao_cadastral"][1] is None  # Apr 31
        assert result["data_situacao_cadastral"][2] is None  # Feb 30
        assert result["data_situacao_cadastral"][3] == "20240229"  # Feb 29 leap year — valid
        assert result["data_situacao_cadastral"][4] == "20230115"  # normal date

    def test_validate_date_future_nullified(self):
        """Test that future dates are nullified."""
        df = pl.DataFrame({"data_situacao_cadastral": ["29991231", "20230101"]})

        result = _validate(df, "ESTABELE")

        assert result["data_situacao_cadastral"][0] is None
        assert result["data_situacao_cadastral"][1] == "20230101"

    def test_validate_date_before_1900_nullified(self):
        """Test that dates before 1900 are nullified."""
        df = pl.DataFrame({"data_situacao_cadastral": ["18501231", "19000101"]})

        result = _validate(df, "ESTABELE")

        assert result["data_situacao_cadastral"][0] is None
        assert result["data_situacao_cadastral"][1] == "19000101"

    def test_validate_valid_data_passes(self):
        """Test that valid data passes through unchanged."""
        df = pl.DataFrame(
            {
                "cnpj_basico": ["12345678"],
                "natureza_juridica": ["2135"],
                "qualificacao_responsavel": ["50"],
                "capital_social": ["1000.00"],
                "porte": ["01"],
                "ente_federativo_responsavel": [None],
            }
        )

        result = _validate(df, "EMPRECSV")

        assert result.equals(df)


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

    def test_cleans_up_temp_file_on_read_error(self, tmp_path):
        """Temp file should be deleted if the source file can't be read."""
        missing_file = tmp_path / "nonexistent.csv"
        temp_file = tmp_path / "leaked.utf8.csv"

        with patch(
            "processor.tempfile.mkstemp",
            return_value=(os.open(str(temp_file), os.O_CREAT | os.O_WRONLY), str(temp_file)),
        ):
            with pytest.raises(FileNotFoundError):
                _convert_encoding(missing_file)

        assert not temp_file.exists()


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
        """Test that all rows are processed across batches."""
        cnae_file = tmp_path / "CNAECSV.csv"
        rows = [f"{i:07d};Descrição {i}" for i in range(150)]
        cnae_file.write_text("\n".join(rows), encoding="ISO-8859-1")

        results = list(process_file(cnae_file, batch_size=50))

        total_rows = sum(len(df) for df, _, _ in results)
        assert total_rows == 150
        assert len(results) >= 1
