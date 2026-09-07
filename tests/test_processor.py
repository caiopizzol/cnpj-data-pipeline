"""Tests for processor module."""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from processor import (
    LayoutDriftError,
    apply_typed_casts,
    check_layout,
    convert_encoding,
    get_file_type,
    process_file,
    transform,
    validate,
)


class TestGetFileType:
    """Test get_file_type function."""

    def test_simples_file_type(self) -> None:
        """Test that SIMPLES filename returns SIMPLESCSV type."""
        filename = "F.K03200$W.SIMPLES.CSV.D51213"
        result = get_file_type(filename)
        assert result == "SIMPLESCSV"

    def test_simples_case_insensitive(self) -> None:
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

    def test_other_file_types(self) -> None:
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

    def test_unknown_file_type(self) -> None:
        """Test that unknown filename returns None."""
        unknown_files = ["README.txt", "config.json", "random_file.csv", "F.K03200$W.UNKNOWN.CSV.D51213"]

        for filename in unknown_files:
            result = get_file_type(filename)
            assert result is None, f"Expected None for {filename}, got {result}"


class TestTransform:
    """Test transform function for date transformations."""

    def test_transform_zero_dates_to_none_estabelecimentos(self) -> None:
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

        result = transform(df, "ESTABELE")

        assert result["data_situacao_cadastral"][0] is None

        assert result["data_inicio_atividade"][0] is None

        assert result["data_situacao_especial"][0] == "20230101"

    def test_transform_zero_dates_to_none_simples(self) -> None:
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

        result = transform(df, "SIMPLESCSV")

        assert result["data_opcao_pelo_simples"][0] is None
        assert result["data_exclusao_do_mei"][0] is None

        assert result["data_exclusao_do_simples"][0] is None

        assert result["data_opcao_pelo_mei"][0] == "20230101"

    def test_transform_zero_dates_to_none_socios(self) -> None:
        """Test that '0' and '00000000' dates become None for socios data."""
        df = pl.DataFrame({"cnpj_basico": ["12345678"], "data_entrada_sociedade": ["0"]})

        result = transform(df, "SOCIOCSV")

        assert result["data_entrada_sociedade"][0] is None

    def test_transform_null_dates_remain_none(self) -> None:
        """Test that null dates remain None."""
        df = pl.DataFrame(
            {"cnpj_basico": ["12345678"], "data_situacao_cadastral": [None], "data_inicio_atividade": [None]}
        )

        result = transform(df, "ESTABELE")

        assert result["data_situacao_cadastral"][0] is None
        assert result["data_inicio_atividade"][0] is None

    def test_transform_valid_dates_unchanged(self) -> None:
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

        result = transform(df, "ESTABELE")

        for i, expected_date in enumerate(valid_dates):
            assert result["data_situacao_cadastral"][i] == expected_date
            assert result["data_inicio_atividade"][i] == expected_date
            assert result["data_situacao_especial"][i] == expected_date

    def test_transform_no_date_columns_file_type(self) -> None:
        """Test transform with file type that has no date transformations."""
        df = pl.DataFrame({"codigo": ["123"], "descricao": ["Test"]})

        result = transform(df, "CNAECSV")

        # DataFrame should be unchanged for file types without date transformations
        assert result.equals(df)

    def test_transform_mixed_date_values(self) -> None:
        """Test transform with mixed valid and invalid date values."""
        df = pl.DataFrame(
            {
                "cnpj_basico": ["12345678", "87654321", "11223344", "99887766"],
                "data_opcao_pelo_simples": ["0", "20230101", "00000000", None],
                "data_exclusao_do_simples": ["20240101", "0", "20230615", "00000000"],
            }
        )

        result = transform(df, "SIMPLESCSV")

        # Check expected transformations
        expected_opcao = [None, "20230101", None, None]
        expected_exclusao = ["20240101", None, "20230615", None]

        for i in range(len(expected_opcao)):
            assert result["data_opcao_pelo_simples"][i] == expected_opcao[i]
            assert result["data_exclusao_do_simples"][i] == expected_exclusao[i]

    def test_transform_capital_social(self) -> None:
        """Test that capital social is converted from Brazilian to standard decimal."""
        df = pl.DataFrame(
            {
                "cnpj_basico": ["12345678"],
                "capital_social": ["1.234.567,89"],
            }
        )

        result = transform(df, "EMPRECSV")

        assert result["capital_social"][0] == "1234567.89"

    def test_transform_negative_capital_social(self) -> None:
        """Test that negative capital social becomes null."""
        df = pl.DataFrame(
            {
                "cnpj_basico": ["12345678", "87654321"],
                "capital_social": ["-1.000,00", "5.000,00"],
            }
        )

        result = transform(df, "EMPRECSV")

        assert result["capital_social"][0] is None
        assert result["capital_social"][1] == "5000.00"

    def test_transform_country_code_padding(self) -> None:
        """Test that country codes are zero-padded to 3 digits."""
        df = pl.DataFrame(
            {
                "cnpj_basico": ["12345678"],
                "pais": ["1"],
            }
        )

        result = transform(df, "ESTABELE")

        assert result["pais"][0] == "001"

    def test_transform_null_cpf_fill(self) -> None:
        """Test that null cnpj_cpf_do_socio is filled with zeros."""
        df = pl.DataFrame(
            {
                "cnpj_basico": ["12345678"],
                "cnpj_cpf_do_socio": [None],
            }
        )

        result = transform(df, "SOCIOCSV")

        assert result["cnpj_cpf_do_socio"][0] == "00000000000000"

    def test_transform_cep_pads_seven_digit_numeric(self) -> None:
        """7-digit all-numeric CEPs get the missing leading zero restored.
        Junk values must be left untouched so the report keeps surfacing them."""
        df = pl.DataFrame(
            {
                "cnpj_basico": ["1", "2", "3", "4", "5", "6", "7"],
                "cep": [
                    "1005010",  # 7-digit numeric → padded
                    "01005010",  # already 8 digits → unchanged
                    "13045000",  # 8-digit, no leading zero → unchanged
                    "0",  # single-char sentinel → unchanged
                    "       0",  # whitespace sentinel → unchanged
                    "0000ABCD",  # 8-char non-digit → unchanged
                    None,  # null → unchanged
                ],
            }
        )

        result = transform(df, "ESTABELE")

        assert result["cep"][0] == "01005010"
        assert result["cep"][1] == "01005010"
        assert result["cep"][2] == "13045000"
        assert result["cep"][3] == "0"
        assert result["cep"][4] == "       0"
        assert result["cep"][5] == "0000ABCD"
        assert result["cep"][6] is None


class TestSocioId:
    """socio_id is a deterministic UUID over the canonical identity tuple.

    The masked CPF in cnpj_cpf_do_socio is not unique inside a company
    (issue #78), so the table PK is socio_id, not the old triple.
    """

    @staticmethod
    def _make_df(rows: list[tuple[str, str, str | None, str | None, str]]) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "cnpj_basico": [r[0] for r in rows],
                "identificador_de_socio": [r[1] for r in rows],
                "nome_socio": [r[2] for r in rows],
                "cnpj_cpf_do_socio": [r[3] for r in rows],
                "qualificacao_do_socio": ["22"] * len(rows),
                "data_entrada_sociedade": [r[4] for r in rows],
                "pais": [None] * len(rows),
                "representante_legal": [None] * len(rows),
                "nome_do_representante": [None] * len(rows),
                "qualificacao_do_representante_legal": [None] * len(rows),
                "faixa_etaria": ["0"] * len(rows),
            }
        )

    def test_old_triple_collision_yields_distinct_socio_ids(self) -> None:
        """Two partners sharing the masked-CPF triple but with different names
        must produce different socio_id. This is the 2026-05 case from #78."""
        df = self._make_df(
            [
                ("01654767", "2", "ALICE SILVA", "***909016**", "20200101"),
                ("01654767", "2", "BOB SOUZA", "***909016**", "20200101"),
            ]
        )

        result = transform(df, "SOCIOCSV")

        assert result["socio_id"][0] != result["socio_id"][1]

    def test_name_casing_and_whitespace_canonicalize(self) -> None:
        """RFB cosmetic name jitter (case, double spaces) must not churn the key."""
        df = self._make_df(
            [
                ("12345678", "2", "ALICE  SILVA", "***123456**", "20200101"),
                ("12345678", "2", "alice silva", "***123456**", "20200101"),
            ]
        )

        result = transform(df, "SOCIOCSV")

        assert result["socio_id"][0] == result["socio_id"][1]

    def test_qualificacao_change_keeps_socio_id_stable(self) -> None:
        """qualificacao_do_socio is an updateable attribute, not identity.
        A partner whose qualification changes between months must upsert,
        not create a ghost row."""
        df = self._make_df(
            [
                ("12345678", "2", "ALICE SILVA", "***123456**", "20200101"),
                ("12345678", "2", "ALICE SILVA", "***123456**", "20200101"),
            ]
        )
        df = df.with_columns(pl.Series("qualificacao_do_socio", ["22", "49"]))

        result = transform(df, "SOCIOCSV")

        assert result["socio_id"][0] == result["socio_id"][1]

    def test_null_name_is_distinct_from_other_partners(self) -> None:
        """A row with NULL nome_socio must get a valid socio_id and not
        collide with a sibling partner under the same masked CPF."""
        df = self._make_df(
            [
                ("12345678", "2", None, "***123456**", "20200101"),
                ("12345678", "2", "ALICE SILVA", "***123456**", "20200101"),
            ]
        )

        result = transform(df, "SOCIOCSV")

        assert result["socio_id"][0] is not None
        assert result["socio_id"][0] != result["socio_id"][1]

    def test_raw_nome_socio_unchanged(self) -> None:
        """Canonicalization runs only against the hash input; the raw column
        must not be mutated."""
        df = self._make_df([("12345678", "2", "  ALICE   SILVA  ", "***123456**", "20200101")])

        result = transform(df, "SOCIOCSV")

        assert result["nome_socio"][0] == "  ALICE   SILVA  "

    def test_socio_id_is_uuid_string(self) -> None:
        df = self._make_df([("12345678", "2", "ALICE SILVA", "***123456**", "20200101")])

        result = transform(df, "SOCIOCSV")

        import uuid

        uuid.UUID(result["socio_id"][0])  # raises if not a valid UUID string


class TestValidate:
    """Test validate function for format validation."""

    def test_validate_cnpj_basico_format(self, caplog: pytest.LogCaptureFixture) -> None:
        """cnpj_basico is 8 uppercase alphanumeric chars (0-9, A-Z) from the
        2026-07 alphanumeric CNPJ. Validation logs malformed values but keeps
        raw data (no nullify)."""
        df = pl.DataFrame({"cnpj_basico": ["12345678", "12ABC678", "ABCDEFGH", "1234", "abcd1234", None]})

        with caplog.at_level("WARNING"):
            result = validate(df, "EMPRECSV")

        # Numeric and uppercase-alphanumeric stems are valid and kept as-is.
        assert result["cnpj_basico"][0] == "12345678"
        assert result["cnpj_basico"][1] == "12ABC678"
        assert result["cnpj_basico"][2] == "ABCDEFGH"
        # Too-short "1234" and lowercase "abcd1234" are flagged but still kept.
        assert "cnpj_basico: 2 invalid" in caplog.text
        assert result["cnpj_basico"][3] == "1234"

    def test_validate_cnpj_ordem_alphanumeric(self, caplog: pytest.LogCaptureFixture) -> None:
        """cnpj_ordem is 4 uppercase alphanumeric chars; lowercase/short warn."""
        df = pl.DataFrame({"cnpj_ordem": ["0001", "01DE", "abcd", "12", None]})

        with caplog.at_level("WARNING"):
            result = validate(df, "ESTABELE")

        assert result["cnpj_ordem"][1] == "01DE"
        assert "cnpj_ordem: 2 invalid" in caplog.text

    def test_validate_cnpj_dv_stays_numeric(self, caplog: pytest.LogCaptureFixture) -> None:
        """cnpj_dv remains 2 numeric digits even under alphanumeric CNPJ;
        an alphabetic dv is flagged."""
        df = pl.DataFrame({"cnpj_dv": ["91", "3X", None]})

        with caplog.at_level("WARNING"):
            validate(df, "ESTABELE")

        assert "cnpj_dv: 1 invalid" in caplog.text

    def test_validate_situacao_cadastral(self, caplog: pytest.LogCaptureFixture) -> None:
        """Report unknown situacao_cadastral codes without replacing them."""
        df = pl.DataFrame({"situacao_cadastral": ["02", "08", "99", None]})

        result = validate(df, "ESTABELE")

        # Logs the invalid "99" but keeps it
        assert result["situacao_cadastral"][2] == "99"
        assert "situacao_cadastral: 1 invalid" in caplog.text

    def test_validate_uf(self, caplog: pytest.LogCaptureFixture) -> None:
        """Report unknown UF codes without replacing them."""
        df = pl.DataFrame({"uf": ["SP", "RJ", "XX", None]})

        result = validate(df, "ESTABELE")

        # Logs invalid "XX" but keeps it
        assert result["uf"][2] == "XX"
        assert "uf: 1 invalid" in caplog.text

    def test_validate_opcao_simples(self, caplog: pytest.LogCaptureFixture) -> None:
        """Report unknown Simples option codes without replacing them."""
        df = pl.DataFrame({"opcao_pelo_simples": ["S", "N", "X", None]})

        result = validate(df, "SIMPLESCSV")

        assert result["opcao_pelo_simples"][2] == "X"
        assert "opcao_pelo_simples: 1 invalid" in caplog.text

    def test_validate_identificador_socio(self, caplog: pytest.LogCaptureFixture) -> None:
        """Report unknown socio identifiers without replacing them."""
        df = pl.DataFrame({"identificador_de_socio": ["1", "2", "3", "9"]})

        result = validate(df, "SOCIOCSV")

        assert result["identificador_de_socio"][3] == "9"
        assert "identificador_de_socio: 1 invalid" in caplog.text

    def test_validate_invalid_date_format_nullified(self) -> None:
        """Test that dates with invalid format (not YYYYMMDD) are nullified."""
        df = pl.DataFrame({"data_situacao_cadastral": ["20230101", "2023-01-01", "INVALID", None]})

        result = validate(df, "ESTABELE")

        assert result["data_situacao_cadastral"][0] == "20230101"
        assert result["data_situacao_cadastral"][1] is None
        assert result["data_situacao_cadastral"][2] is None
        assert result["data_situacao_cadastral"][3] is None

    def test_validate_date_invalid_month_day(self) -> None:
        """Test that dates with invalid month/day are nullified."""
        df = pl.DataFrame({"data_situacao_cadastral": ["20231301", "20230132", "20230615"]})

        result = validate(df, "ESTABELE")

        assert result["data_situacao_cadastral"][0] is None  # month 13
        assert result["data_situacao_cadastral"][1] is None  # day 32
        assert result["data_situacao_cadastral"][2] == "20230615"

    def test_validate_date_impossible_calendar_dates(self) -> None:
        """Test that impossible calendar dates (Feb 30, Apr 31) are nullified."""
        df = pl.DataFrame({"data_situacao_cadastral": ["20230229", "20230431", "20240230", "20240229", "20230115"]})

        result = validate(df, "ESTABELE")

        assert result["data_situacao_cadastral"][0] is None  # Feb 29 non-leap
        assert result["data_situacao_cadastral"][1] is None  # Apr 31
        assert result["data_situacao_cadastral"][2] is None  # Feb 30
        assert result["data_situacao_cadastral"][3] == "20240229"  # Feb 29 leap year — valid
        assert result["data_situacao_cadastral"][4] == "20230115"  # normal date

    def test_validate_date_future_nullified(self) -> None:
        """Test that future dates are nullified."""
        df = pl.DataFrame({"data_situacao_cadastral": ["29991231", "20230101"]})

        result = validate(df, "ESTABELE")

        assert result["data_situacao_cadastral"][0] is None
        assert result["data_situacao_cadastral"][1] == "20230101"

    def test_validate_date_before_1900_nullified(self) -> None:
        """Test that dates before 1900 are nullified."""
        df = pl.DataFrame({"data_situacao_cadastral": ["18501231", "19000101"]})

        result = validate(df, "ESTABELE")

        assert result["data_situacao_cadastral"][0] is None
        assert result["data_situacao_cadastral"][1] == "19000101"

    def test_validate_valid_data_passes(self) -> None:
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

        result = validate(df, "EMPRECSV")

        assert result.equals(df)


class TestConvertEncoding:
    """Test encoding conversion from ISO-8859-1 to UTF-8."""

    def test_converts_iso_to_utf8(self, tmp_path: Path) -> None:
        """Test that ISO-8859-1 content is correctly converted to UTF-8."""
        # Create a file with ISO-8859-1 content (Brazilian characters)
        iso_content = "São Paulo;Empresa Ltda;Açúcar\nRio de Janeiro;Comércio;Café"
        iso_file = tmp_path / "test.csv"
        iso_file.write_text(iso_content, encoding="ISO-8859-1")

        utf8_file = convert_encoding(iso_file)

        try:
            # Read as UTF-8 and verify content
            result = utf8_file.read_text(encoding="UTF-8")
            assert "São Paulo" in result
            assert "Açúcar" in result
            assert "Café" in result
        finally:
            utf8_file.unlink(missing_ok=True)

    def test_preserves_all_lines_during_encoding_conversion(self, tmp_path: Path) -> None:
        """Encoding conversion preserves every line of a multi-line file."""
        iso_file = tmp_path / "large.csv"
        content = "data;value\n" * 10000
        iso_file.write_text(content, encoding="ISO-8859-1")

        utf8_file = convert_encoding(iso_file)

        try:
            result = utf8_file.read_text(encoding="UTF-8")
            assert result.count("\n") == 10000
        finally:
            utf8_file.unlink(missing_ok=True)

    def test_cleans_up_temp_file_on_read_error(self, tmp_path: Path) -> None:
        """Temp file should be deleted if the source file can't be read."""
        missing_file = tmp_path / "nonexistent.csv"
        temp_file = tmp_path / "leaked.utf8.csv"

        with patch(
            "processor.tempfile.mkstemp",
            return_value=(os.open(str(temp_file), os.O_CREAT | os.O_WRONLY), str(temp_file)),
        ):
            with pytest.raises(FileNotFoundError):
                convert_encoding(missing_file)

        assert not temp_file.exists()


class TestProcessFile:
    """Test process_file function for batch processing."""

    def test_skips_unknown_file_type(self, tmp_path: Path) -> None:
        """Test that unknown file types are skipped with no output."""
        unknown_file = tmp_path / "UNKNOWN_FILE.csv"
        unknown_file.write_text("data;value", encoding="ISO-8859-1")

        results = list(process_file(unknown_file))

        assert results == []

    def test_handles_empty_csv(self, tmp_path: Path) -> None:
        """Test that empty CSV files are handled gracefully."""
        empty_file = tmp_path / "CNAECSV.csv"
        empty_file.write_text("", encoding="ISO-8859-1")

        results = list(process_file(empty_file))

        assert results == []

    def test_processes_valid_csv_in_batches(self, tmp_path: Path) -> None:
        """Test that valid CSV is processed and yields correct data."""
        # Create a small CNAE file (simple 2-column format)
        cnae_file = tmp_path / "CNAECSV.csv"
        content = "0111301;Cultivo de arroz\n0111302;Cultivo de milho\n0111303;Cultivo de trigo"
        cnae_file.write_text(content, encoding="ISO-8859-1")

        results = list(process_file(cnae_file, batch_size=100))

        assert len(results) == 1
        df, table_name, _columns = results[0]
        assert table_name == "cnaes"
        assert len(df) == 3
        assert _columns == ["codigo", "descricao"]

    def test_processes_simples_file(self, tmp_path: Path) -> None:
        """Test that SIMPLES files are processed correctly."""
        simples_file = tmp_path / "F.K03200$W.SIMPLES.CSV.D51213"
        # 7 columns: cnpj_basico, opcao_pelo_simples, dates (4x)
        content = "12345678;S;20200101;0;N;0;0"
        simples_file.write_text(content, encoding="ISO-8859-1")

        results = list(process_file(simples_file))

        assert len(results) == 1
        df, table_name, _columns = results[0]
        assert table_name == "dados_simples"
        # Verify date transformation (0 → None)
        assert df["data_exclusao_do_simples"][0] is None

    def test_cleans_up_temp_file(self, tmp_path: Path) -> None:
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

    def test_preserves_rows_with_small_requested_batch_size(self, tmp_path: Path) -> None:
        """All rows survive regardless of how Polars groups the requested batches."""
        cnae_file = tmp_path / "CNAECSV.csv"
        rows = [f"{i:07d};Descrição {i}" for i in range(150)]
        cnae_file.write_text("\n".join(rows), encoding="ISO-8859-1")

        results = list(process_file(cnae_file, batch_size=50))

        total_rows = sum(len(df) for df, _, _ in results)
        assert total_rows == 150
        assert len(results) >= 1


class TestTypedCasts:
    """apply_typed_casts brings Parquet output to type-parity with Postgres.

    Date cleanup and decimal normalization precede these casts in the
    pipeline. Numeric casts do not enforce domain membership.
    """

    def test_dates_cast_to_polars_date_for_estabele(self) -> None:
        df = pl.DataFrame(
            {
                "data_situacao_cadastral": ["20240315", None],
                "data_inicio_atividade": ["20200101", "19991231"],
                "data_situacao_especial": [None, None],
            }
        )
        out = apply_typed_casts(df, "ESTABELE")
        assert out["data_situacao_cadastral"].dtype == pl.Date
        assert out["data_inicio_atividade"].dtype == pl.Date
        assert out["data_situacao_especial"].dtype == pl.Date
        # Null preserved
        assert out["data_situacao_cadastral"][1] is None

    def test_dates_cast_for_simples_and_socios(self) -> None:
        simples = pl.DataFrame(
            {
                "data_opcao_pelo_simples": ["20200101"],
                "data_exclusao_do_simples": [None],
                "data_opcao_pelo_mei": ["20210601"],
                "data_exclusao_do_mei": [None],
            }
        )
        socios = pl.DataFrame({"data_entrada_sociedade": ["20180515"]})
        assert apply_typed_casts(simples, "SIMPLESCSV")["data_opcao_pelo_simples"].dtype == pl.Date
        assert apply_typed_casts(socios, "SOCIOCSV")["data_entrada_sociedade"].dtype == pl.Date

    def test_capital_social_cast_to_float64(self) -> None:
        df = pl.DataFrame({"capital_social": ["1234567.89", "0", None]})
        out = apply_typed_casts(df, "EMPRECSV")
        assert out["capital_social"].dtype == pl.Float64
        assert out["capital_social"][0] == 1234567.89
        assert out["capital_social"][1] == 0.0
        assert out["capital_social"][2] is None

    def test_identificador_matriz_filial_cast_to_int32(self) -> None:
        df = pl.DataFrame({"identificador_matriz_filial": ["1", "2", None]})
        out = apply_typed_casts(df, "ESTABELE")
        assert out["identificador_matriz_filial"].dtype == pl.Int32
        assert out["identificador_matriz_filial"][0] == 1
        assert out["identificador_matriz_filial"][1] == 2
        assert out["identificador_matriz_filial"][2] is None

    def test_noop_on_reference_table(self) -> None:
        """Reference tables have no date or numeric columns to cast."""
        df = pl.DataFrame({"codigo": ["0111301"], "descricao": ["Cultivo de arroz"]})
        out = apply_typed_casts(df, "CNAECSV")
        # Both columns stay Utf8 (no transformations apply to reference tables)
        assert out["codigo"].dtype == pl.Utf8
        assert out["descricao"].dtype == pl.Utf8

    def test_handles_all_null_date_column(self) -> None:
        """All-null columns supplied as pl.Null must cast without string operations."""
        df = pl.DataFrame(
            {
                "data_situacao_cadastral": ["20240101"],
                "data_inicio_atividade": ["20200101"],
                "data_situacao_especial": pl.Series([None], dtype=pl.Null),
            }
        )
        out = apply_typed_casts(df, "ESTABELE")
        # All three columns end up as Date for schema stability across batches
        assert out["data_situacao_cadastral"].dtype == pl.Date
        assert out["data_inicio_atividade"].dtype == pl.Date
        assert out["data_situacao_especial"].dtype == pl.Date
        assert out["data_situacao_especial"][0] is None


class TestProcessFileTypedFlag:
    """typed=True end-to-end through process_file."""

    def test_default_keeps_strings(self, tmp_path: Path) -> None:
        """Default behavior (typed=False) preserves backward compat for v1.x
        Parquet consumers who already wrote queries against string columns."""
        simples_file = tmp_path / "F.K03200$W.SIMPLES.CSV.D51213"
        simples_file.write_text("12345678;S;20200101;0;N;0;0", encoding="ISO-8859-1")

        results = list(process_file(simples_file))
        df, _, _ = results[0]
        assert df["data_opcao_pelo_simples"].dtype == pl.Utf8

    def test_typed_flag_casts_dates(self, tmp_path: Path) -> None:
        simples_file = tmp_path / "F.K03200$W.SIMPLES.CSV.D51213"
        simples_file.write_text("12345678;S;20200101;0;N;0;0", encoding="ISO-8859-1")

        results = list(process_file(simples_file, typed=True))
        df, _, _ = results[0]
        assert df["data_opcao_pelo_simples"].dtype == pl.Date


class TestLayoutDriftDetection:
    """check_layout rejects an unexpected field count in the first CSV record."""

    def test_matching_layout_passes(self, tmp_path: Path) -> None:
        """A CSV with the expected column count must validate cleanly."""
        # SIMPLESCSV expects 7 columns
        f = tmp_path / "good.utf8.csv"
        f.write_text("12345678;S;20200101;0;N;0;0\n", encoding="utf-8")
        # Should not raise
        check_layout(f, "SIMPLESCSV")

    def test_extra_column_raises(self, tmp_path: Path) -> None:
        """If RFB adds a column, we must fail loudly."""
        f = tmp_path / "drift.utf8.csv"
        # 8 fields instead of expected 7
        f.write_text("12345678;S;20200101;0;N;0;0;EXTRA\n", encoding="utf-8")
        with pytest.raises(LayoutDriftError) as exc:
            check_layout(f, "SIMPLESCSV")
        assert "expected 7" in str(exc.value)
        assert "got 8" in str(exc.value)

    def test_missing_column_raises(self, tmp_path: Path) -> None:
        f = tmp_path / "short.utf8.csv"
        # 6 fields instead of expected 7
        f.write_text("12345678;S;20200101;0;N;0\n", encoding="utf-8")
        with pytest.raises(LayoutDriftError) as exc:
            check_layout(f, "SIMPLESCSV")
        assert "expected 7" in str(exc.value)

    def test_empty_file_does_not_raise(self, tmp_path: Path) -> None:
        """Empty CSV: let Polars' NoDataError handle it, don't claim drift."""
        f = tmp_path / "empty.utf8.csv"
        f.write_text("", encoding="utf-8")
        check_layout(f, "SIMPLESCSV")  # should not raise

    def test_process_file_aborts_on_drift(self, tmp_path: Path) -> None:
        """End-to-end: process_file must fail BEFORE yielding any batch
        when the source layout doesn't match."""
        bad = tmp_path / "F.K03200$W.SIMPLES.CSV.D51213"
        # 8 fields (one extra) - drift
        bad.write_text("12345678;S;20200101;0;N;0;0;BOGUS\n", encoding="ISO-8859-1")
        with pytest.raises(LayoutDriftError):
            list(process_file(bad))
