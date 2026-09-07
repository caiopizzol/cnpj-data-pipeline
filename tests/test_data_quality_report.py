"""Tests for scripts/data_quality_report.py."""

import argparse
import sys
from pathlib import Path

import pytest

# scripts/ isn't a package; add it to sys.path so we can import directly.
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from data_quality_report import cnpj_expected_dv, format_report, sample_pct  # noqa: E402


def _base_measurements(enriched_orphans):
    """Minimal measurement dict for format_report, parameterized on the
    enriched-coverage section under test."""
    return {
        "cnpj_check_digits": {"total": 0, "valid": 0, "invalid": 0, "examples": [], "scan_mode": "sample 0.1%"},
        "orphan_fks": [{"label": "estabelecimentos.pais ∉ paises", "orphans": 3}],
        "enriched_orphans": enriched_orphans,
        "exterior_uf": {"total": 10, "exterior": 1},
        "capital_sentinel": {"total": 10, "sentinel": 0, "nulls": 0},
        "representante_sentinel": {"total": 10, "sentinel": 0},
        "cep_validity": {"total": 10, "nulls": 0, "zero_sentinel": 0, "malformed": 0},
    }


class TestCnpjExpectedDV:
    """The check-digit algorithm is deterministic and well-known. Test
    against published real CNPJs whose check digits are public knowledge.
    """

    @pytest.mark.parametrize(
        "first_12,expected",
        [
            # Banco do Brasil S.A. matriz: CNPJ 00.000.000/0001-91
            ("000000000001", "91"),
            # Petrobras matriz: 33.000.167/0001-01
            ("330001670001", "01"),
            # Receita Federal as a known publicly-listed entity:
            # 00.394.460/0058-87 (Ministério da Fazenda - SP)
            ("003944600058", "87"),
        ],
    )
    def test_known_real_cnpjs(self, first_12, expected):
        assert cnpj_expected_dv(first_12) == expected

    def test_alphanumeric_official_example(self):
        # Receita Federal's published alphanumeric example: 12.ABC.345/01DE-35.
        # Stem 12ABC34501DE -> check digits 35 under the ord(c)-48 valuation.
        assert cnpj_expected_dv("12ABC34501DE") == "35"

    def test_rejects_lowercase_and_symbols(self):
        # The stem alphabet is uppercase 0-9/A-Z only; lowercase and
        # punctuation are not valid characters.
        with pytest.raises(ValueError):
            cnpj_expected_dv("0000000000a1")
        with pytest.raises(ValueError):
            cnpj_expected_dv("000000000.01")

    def test_rejects_wrong_length(self):
        with pytest.raises(ValueError):
            cnpj_expected_dv("12345")
        with pytest.raises(ValueError):
            cnpj_expected_dv("00000000000012")  # 14 not 12

    @pytest.mark.parametrize(
        "stem,expected",
        [
            ("000000000000", "00"),  # Both weighted sums have remainder 0.
            ("000000000006", "04"),  # First weighted sum has remainder 1.
            ("000000000018", "30"),  # Second weighted sum has remainder 1.
        ],
    )
    def test_zero_digits_for_remainders_zero_and_one(self, stem, expected):
        """Synthetic stems exercise both zero-DV boundaries for each digit."""
        assert cnpj_expected_dv(stem) == expected


class TestSamplePct:
    def test_accepts_positive_percentage_up_to_100(self):
        assert sample_pct("0.1") == 0.1
        assert sample_pct("100") == 100.0

    @pytest.mark.parametrize("value", ["0", "-1", "101", "nan", "inf", "abc"])
    def test_rejects_invalid_percentages(self, value):
        with pytest.raises(argparse.ArgumentTypeError):
            sample_pct(value)


class TestFormatReportEnrichedSection:
    """Render-level coverage for the enriched-domain coverage section."""

    def test_renders_monthly_vs_enriched_table(self):
        report = format_report(
            _base_measurements(
                {
                    "available": True,
                    "rows": [
                        {
                            "label": "estabelecimentos.motivo_situacao_cadastral",
                            "monthly_orphans": 5,
                            "enriched_orphans": 1,
                        }
                    ],
                }
            ),
            scope={"scope_str": "Bernoulli sample 0.1%"},
        )
        assert "## Enriched-domain coverage" in report
        assert "Monthly orphans" in report and "After enrichment" in report
        assert "estabelecimentos.motivo_situacao_cadastral" in report

    def test_renders_unavailable_note(self):
        report = format_report(
            _base_measurements({"available": False, "rows": []}),
            scope={"scope_str": "Bernoulli sample 0.1%"},
        )
        assert "## Enriched-domain coverage" in report
        assert "reference_domains_enriched.sql" in report
