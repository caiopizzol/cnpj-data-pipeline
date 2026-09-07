import pytest

from database import Database
from tests.integration.support import (
    ENRICHED_SUPPLEMENTAL_MOTIVO_32,
    RECIPES_DIR,
    count_rows,
    fetch_row,
)


@pytest.mark.usefixtures("reference_domains_enriched")
class TestRecipeReferenceDomainsEnriched:
    """Verify reference domains enriched against prepared PostgreSQL fixtures."""

    RECIPE_PATH = RECIPES_DIR / "reference_domains_enriched.sql"

    ENRICHED_TABLES = ("motivos_enriched", "paises_enriched", "qualificacoes_socios_enriched")
    EXPECTED_COLUMNS = {
        "codigo",
        "descricao",
        "source_kind",
        "source_url",
        "is_supplemental",
        "confidence",
        "notes",
    }

    def test_recipe_executes(self, test_db: Database) -> None:
        """The recipe SQL should parse and execute, creating all three tables."""

        for table in self.ENRICHED_TABLES:
            with test_db.connect().cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name = %s",
                    (table,),
                )
                assert cur.fetchone() is not None, f"{table} not created"

    def test_provenance_schema(self, test_db: Database) -> None:
        """Every enriched table exposes the same provenance columns."""
        for table in self.ENRICHED_TABLES:
            with test_db.connect().cursor() as cur:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
                    (table,),
                )
                cols = {row[0] for row in cur.fetchall()}
            assert cols == self.EXPECTED_COLUMNS, f"{table} columns: {cols}"

    def test_monthly_rows_preserved_and_win(self, test_db: Database) -> None:
        """Each enriched table is a superset of its monthly lookup: every
        monthly (codigo, descricao) pair is present as a non-supplemental row,
        and supplemental rows never override a monthly codigo."""
        for monthly, enriched in (
            ("motivos", "motivos_enriched"),
            ("paises", "paises_enriched"),
            ("qualificacoes_socios", "qualificacoes_socios_enriched"),
        ):
            with test_db.connect().cursor() as cur:
                cur.execute(
                    f"""
                    SELECT COUNT(*) FROM {monthly} mo
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {enriched} en
                        WHERE en.codigo = mo.codigo
                          AND en.descricao IS NOT DISTINCT FROM mo.descricao
                          AND en.is_supplemental = false
                          AND en.source_kind = 'receita_monthly'
                    )
                    """
                )
                missing = fetch_row(cur)[0]
                assert missing == 0, f"{enriched} dropped/altered {missing} monthly rows"

                # No codigo appears more than once (anti-join + PK guarantee).
                cur.execute(f"SELECT codigo, COUNT(*) FROM {enriched} GROUP BY codigo HAVING COUNT(*) > 1")
                assert cur.fetchall() == [], f"{enriched} has duplicate codigo"

                # Supplemental rows must not collide with a monthly codigo.
                cur.execute(
                    f"""
                    SELECT COUNT(*) FROM {enriched} en
                    WHERE en.is_supplemental
                      AND EXISTS (SELECT 1 FROM {monthly} mo WHERE mo.codigo = en.codigo)
                    """
                )
                assert fetch_row(cur)[0] == 0, f"{enriched} supplemental row overrides a monthly codigo"

    def test_motivo_32_supplemental(self, test_db: Database) -> None:
        """motivo 32 is absent from the monthly Motivos delivery but resolves via
        the SERPRO supplemental row, verbatim and flagged supplemental."""
        with test_db.connect().cursor() as cur:
            cur.execute(
                "SELECT descricao, source_kind, is_supplemental, source_url, confidence "
                "FROM motivos_enriched WHERE codigo = '32'"
            )
            row = fetch_row(cur)
        assert row is not None, "motivo 32 missing from motivos_enriched"
        descricao, source_kind, is_supplemental, source_url, confidence = row
        assert descricao == ENRICHED_SUPPLEMENTAL_MOTIVO_32, repr(descricao)
        assert source_kind == "serpro_dominio"
        assert is_supplemental is True
        assert source_url and source_url.startswith("https://bcadastros.serpro.gov.br/")
        assert confidence == "high"

    def test_pais_supplemental_codes(self, test_db: Database) -> None:
        """The SERPRO-confirmed orphan country codes resolve via supplemental
        rows with their official labels; codes absent from every official table
        stay unresolved (no row)."""
        expected = {
            # 015/042 exercise the zero-padding match: SERPRO stores them as
            # "15"/"42", the pipeline pads pais to "015"/"042".
            "015": "ALAND, ILHAS",
            "042": "ANTÁRTICA",
            "150": "JERSEY, ILHA DO CANAL",
            "151": "CANÁRIAS, ILHAS",
            "200": "CURACAO",
            "321": "GUERNSEY",
            "359": "MAN, ILHA DE",
            "367": "INGLATERRA",
            "393": "JERSEY",
            "449": "MACEDÔNIA, ANT.REP.IUGOSLAVA",
            "498": "MONTENEGRO",
            "578": "PALESTINA",
            "678": "SAINT KITTS E NEVIS",
            "693": "SAO BARTOLOMEU",
            "699": "SÃO MARTINHO, ILHA DE (PARTE HOLANDESA)",
            "737": "SERVIA",
            "755": "SVALBARD E JAN MAYEN",
            "994": "A DESIGNAR",
        }
        with test_db.connect().cursor() as cur:
            for codigo, descricao in expected.items():
                cur.execute(
                    "SELECT descricao, is_supplemental, source_kind FROM paises_enriched WHERE codigo = %s",
                    (codigo,),
                )
                row = fetch_row(cur)
                assert row is not None, f"pais {codigo} missing from paises_enriched"
                assert row[0] == descricao, f"pais {codigo}: {row[0]!r}"
                assert row[1] is True and row[2] == "serpro_dominio"

            # Absent from both supplemental sources -> intentionally unresolved.
            for codigo in ("008", "009", "452"):
                cur.execute("SELECT 1 FROM paises_enriched WHERE codigo = %s", (codigo,))
                assert cur.fetchone() is None, f"pais {codigo} should stay unresolved"

    def test_qualificacao_36_legacy_supplement(self, test_db: Database) -> None:
        """Code 36 (Gerente-Delegado) is a legacy qualification - documented in
        Receita's open-data table but no longer collected, so it is absent from
        the monthly delivery and resolved via a receita_ods supplemental row. It
        is the only supplemental qualification (nothing else is invented)."""
        with test_db.connect().cursor() as cur:
            cur.execute(
                "SELECT descricao, is_supplemental, source_kind, confidence "
                "FROM qualificacoes_socios_enriched WHERE codigo = '36'"
            )
            row = fetch_row(cur)
            assert row is not None, "qualificacao 36 missing from qualificacoes_socios_enriched"
            assert row[0] == "Gerente-Delegado", repr(row[0])
            assert row[1] is True and row[2] == "receita_ods" and row[3] == "high"

            cur.execute("SELECT codigo FROM qualificacoes_socios_enriched WHERE is_supplemental ORDER BY codigo")
            assert [r[0] for r in cur.fetchall()] == ["36"], "only code 36 may be supplemented"

    def test_idempotent(self, test_db: Database) -> None:
        """Re-running the recipe drops+recreates without error, same counts."""
        sql = self.RECIPE_PATH.read_text()
        counts_before = {t: count_rows(test_db, t) for t in self.ENRICHED_TABLES}
        with test_db.connect().cursor() as cur:
            cur.execute(sql)
        test_db.connect().commit()
        for table, before in counts_before.items():
            assert count_rows(test_db, table) == before, f"{table} row count changed on re-run"
