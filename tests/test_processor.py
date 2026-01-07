"""Tests for processor module."""

import polars as pl

from processor import _transform, get_file_type


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
