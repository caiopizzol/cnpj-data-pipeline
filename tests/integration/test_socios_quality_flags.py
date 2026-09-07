import pytest

from database import Database
from tests.integration.support import (
    RECIPES_DIR,
    apply_recipe,
    count_rows,
    fetch_row,
)


@pytest.mark.usefixtures("socios_quality_flags")
class TestRecipeSociosQualityFlags:
    """Verify socios quality flags against prepared PostgreSQL fixtures."""

    RECIPE_PATH = RECIPES_DIR / "socios_quality_flags.sql"

    def test_recipe_executes(self, test_db: Database) -> None:
        """The recipe SQL should parse and execute without errors. The enriched
        flags depend on the enriched lookups, so apply those first."""

        with test_db.connect().cursor() as cur:
            cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'socios_quality_flags'")
            assert cur.fetchone() is not None, "socios_quality_flags table not created"

    def test_row_count_matches_socios(self, test_db: Database) -> None:
        """The recipe is one row per socios row."""
        socios = count_rows(test_db, "socios")
        flags = count_rows(test_db, "socios_quality_flags")
        assert flags > 0, "socios_quality_flags is empty"
        assert flags == socios, f"socios_quality_flags ({flags}) != socios ({socios})"

    def test_schema_is_narrow_flags_only(self, test_db: Database) -> None:
        """The recipe should carry the source key plus quality signals only."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'socios_quality_flags'
            """)
            cols = {row[0] for row in cur.fetchall()}

        assert cols == {
            "socio_id",
            "cnpj_basico",
            "identificador_de_socio",
            "cnpj_cpf_do_socio",
            "representante_is_placeholder",
            "pais_lookup_missing",
            "qualificacao_socio_lookup_missing",
            "qualificacao_representante_lookup_missing",
            "pais_enriched_lookup_missing",
            "qualificacao_socio_enriched_lookup_missing",
            "qualificacao_representante_enriched_lookup_missing",
            "faixa_etaria_nao_se_aplica",
        }

    def test_flags_match_source_predicates(self, test_db: Database) -> None:
        """Flags should mirror direct predicates over socios and reference
        tables. No interpretation should be duplicated elsewhere."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM socios_quality_flags f
                JOIN socios s USING (socio_id)
                WHERE f.representante_is_placeholder IS DISTINCT FROM (
                    s.representante_legal = '***000000**'
                    AND s.qualificacao_do_representante_legal = '00'
                )
                   OR f.pais_lookup_missing IS DISTINCT FROM (
                    s.pais IS NOT NULL
                    AND NOT EXISTS (SELECT 1 FROM paises p WHERE p.codigo = s.pais)
                )
                   OR f.qualificacao_socio_lookup_missing IS DISTINCT FROM (
                    s.qualificacao_do_socio IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM qualificacoes_socios q
                        WHERE q.codigo = s.qualificacao_do_socio
                    )
                )
                   OR f.qualificacao_representante_lookup_missing IS DISTINCT FROM (
                    s.qualificacao_do_representante_legal IS NOT NULL
                    AND s.qualificacao_do_representante_legal <> '00'
                    AND NOT EXISTS (
                        SELECT 1 FROM qualificacoes_socios q
                        WHERE q.codigo = s.qualificacao_do_representante_legal
                    )
                )
                   OR f.faixa_etaria_nao_se_aplica IS DISTINCT FROM (s.faixa_etaria = '0')
            """)
            mismatches = fetch_row(cur)[0]

            cur.execute("""
                SELECT COUNT(*)
                FROM socios_quality_flags f
                JOIN socios s USING (socio_id)
                WHERE f.pais_enriched_lookup_missing IS DISTINCT FROM (
                    s.pais IS NOT NULL
                    AND NOT EXISTS (SELECT 1 FROM paises_enriched p WHERE p.codigo = s.pais)
                )
                   OR f.qualificacao_socio_enriched_lookup_missing IS DISTINCT FROM (
                    s.qualificacao_do_socio IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM qualificacoes_socios_enriched q
                        WHERE q.codigo = s.qualificacao_do_socio
                    )
                )
                   OR f.qualificacao_representante_enriched_lookup_missing IS DISTINCT FROM (
                    s.qualificacao_do_representante_legal IS NOT NULL
                    AND s.qualificacao_do_representante_legal <> '00'
                    AND NOT EXISTS (
                        SELECT 1 FROM qualificacoes_socios_enriched q
                        WHERE q.codigo = s.qualificacao_do_representante_legal
                    )
                )
            """)
            enriched_mismatches = fetch_row(cur)[0]

        assert mismatches == 0, f"socios_quality_flags predicate mismatches for {mismatches} rows"
        assert enriched_mismatches == 0, f"socios enriched predicate mismatches for {enriched_mismatches} rows"

    def test_no_raw_or_clean_columns(self, test_db: Database) -> None:
        """This is a flags table, not socios_clean or socios_detalhe."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'socios_quality_flags'
            """)
            cols = {row[0] for row in cur.fetchall()}

        forbidden = {
            "nome_socio",
            "representante_legal",
            "representante_legal_raw",
            "representante_legal_clean",
            "nome_do_representante",
            "qualificacao_do_socio_descricao",
            "qualificacao_do_representante_legal_descricao",
            "pais_descricao",
            "faixa_etaria_descricao",
        }
        leaked = cols & forbidden
        assert not leaked, f"socios_quality_flags leaked non-flag columns: {leaked}"

    def test_idempotent(self, test_db: Database) -> None:
        """Re-running the recipe should drop+recreate without error and
        produce the same row count."""
        sql = self.RECIPE_PATH.read_text()
        count_before = count_rows(test_db, "socios_quality_flags")

        with test_db.connect().cursor() as cur:
            cur.execute(sql)
        test_db.connect().commit()

        count_after = count_rows(test_db, "socios_quality_flags")
        assert count_before == count_after, f"Re-running recipe changed row count: {count_before} -> {count_after}"


@pytest.mark.parametrize("country, enriched_missing", [("150", False), ("008", True)])
def test_socios_supplemental_flags(
    test_db: Database, reference_domains_enriched: None, country: str, enriched_missing: bool
) -> None:
    with test_db.connect().cursor() as cur:
        cur.execute(
            """
            UPDATE socios SET pais = %s, qualificacao_do_socio = '36',
                qualificacao_do_representante_legal = '36'
            WHERE socio_id = (SELECT socio_id FROM socios ORDER BY socio_id LIMIT 1)
            RETURNING socio_id
        """,
            (country,),
        )
        socio_id = fetch_row(cur)[0]
    test_db.connect().commit()
    apply_recipe(test_db, "socios_quality_flags")
    with test_db.connect().cursor() as cur:
        cur.execute(
            """
            SELECT pais_lookup_missing, pais_enriched_lookup_missing,
                qualificacao_socio_lookup_missing, qualificacao_socio_enriched_lookup_missing,
                qualificacao_representante_lookup_missing, qualificacao_representante_enriched_lookup_missing
            FROM socios_quality_flags WHERE socio_id = %s
        """,
            (socio_id,),
        )
        assert fetch_row(cur) == (True, enriched_missing, True, False, True, False)
