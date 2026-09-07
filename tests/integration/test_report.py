import pytest

from database import Database
from scripts import data_quality_report


class TestDataQualityReportMeasurements:
    """Verify report against prepared PostgreSQL fixtures."""

    def test_new_orphan_checks_present_and_detect_code_36(self, test_db: Database) -> None:
        """The orphan report now covers qualificacao_responsavel and the socios
        relationships. Code 36 (unverified) is a real orphan and must surface."""
        results = {r["label"]: r["orphans"] for r in data_quality_report.measure_orphan_fks(test_db.connect())}

        for label in (
            "empresas.qualificacao_responsavel ∉ qualificacoes_socios",
            "socios.pais ∉ paises",
            "socios.qualificacao_do_socio ∉ qualificacoes_socios",
            "socios.qualificacao_do_representante_legal ∉ qualificacoes_socios (≠ '00')",
        ):
            assert label in results, f"missing orphan check: {label}"

        # crafted empresa 99000004 carries qualificacao_responsavel '36'
        assert results["empresas.qualificacao_responsavel ∉ qualificacoes_socios"] >= 1

    @pytest.mark.usefixtures("reference_domains_enriched")
    def test_enriched_coverage_shows_gap_closed(self, test_db: Database) -> None:
        """Enriched coverage reports monthly vs enriched orphans. Supplemental
        codes close the gap: motivo 32, pais 150/994, and qualificacao 36
        (the legacy Gerente-Delegado code, carried by fixture empresa 99000004)."""
        result = data_quality_report.measure_enriched_orphans(test_db.connect())
        assert result["available"] is True
        rows = {r["label"]: r for r in result["rows"]}

        motivo = rows["estabelecimentos.motivo_situacao_cadastral"]
        assert motivo["monthly_orphans"] > motivo["enriched_orphans"], "motivo 32 should close the gap"

        pais = rows["estabelecimentos.pais"]
        assert pais["monthly_orphans"] > pais["enriched_orphans"], "pais 150/994 should close the gap"
        assert pais["enriched_orphans"] >= 1, "spurious pais 008 must remain unresolved"

        qual = rows["empresas.qualificacao_responsavel"]
        assert qual["monthly_orphans"] > qual["enriched_orphans"], "legacy code 36 should close the gap"

    def test_enriched_coverage_absent_when_tables_missing(self, test_db: Database) -> None:
        """When the enriched tables do not exist, the measurement degrades to
        available=False instead of erroring."""
        with test_db.connect().cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS motivos_enriched")
            cur.execute("DROP TABLE IF EXISTS paises_enriched")
            cur.execute("DROP TABLE IF EXISTS qualificacoes_socios_enriched")
        test_db.connect().commit()

        result = data_quality_report.measure_enriched_orphans(test_db.connect())
        assert result == {"available": False, "rows": []}
