import pytest

from database import Database
from tests.integration.support import (
    RECIPES_DIR,
    count_rows,
    fetch_row,
)


@pytest.mark.usefixtures("data_quality_flags")
class TestRecipeDataQualityFlags:
    """Verify data quality flags against prepared PostgreSQL fixtures."""

    RECIPE_PATH = RECIPES_DIR / "data_quality_flags.sql"

    def test_recipe_executes(self, test_db: Database) -> None:
        """The recipe SQL should parse and execute without errors. The enriched
        flags depend on the enriched lookups, so apply those first."""

        with test_db.connect().cursor() as cur:
            cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'data_quality_flags'")
            assert cur.fetchone() is not None, "data_quality_flags table not created"

    def test_row_count_matches_empresa_estabelecimento_join(self, test_db: Database) -> None:
        """The recipe joins estabelecimentos with empresas, so the output row
        count must match that base join. Reference-table checks happen through
        NOT EXISTS predicates and must not add or drop rows."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM empresas emp
                JOIN estabelecimentos e USING (cnpj_basico)
            """)
            expected = fetch_row(cur)[0]

        actual = count_rows(test_db, "data_quality_flags")
        assert actual > 0, "data_quality_flags is empty"
        assert actual == expected, (
            f"data_quality_flags ({actual}) != empresas⋈estabelecimentos ({expected}) - "
            "flags must preserve the base join cardinality"
        )

    def test_schema_is_narrow_flags_only(self, test_db: Database) -> None:
        """The recipe should not duplicate raw source columns. It carries the
        estabelecimento key, cnpj concat, and quality signals only."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'data_quality_flags'
            """)
            cols = {row[0] for row in cur.fetchall()}

        assert cols == {
            "cnpj_basico",
            "cnpj_ordem",
            "cnpj_dv",
            "cnpj",
            "cep_status",
            "is_exterior",
            "pais_lookup_missing",
            "motivo_lookup_missing",
            "pais_enriched_lookup_missing",
            "motivo_enriched_lookup_missing",
            "capital_social_is_suspicious_sentinel",
        }

    def test_cnpj_column_is_concatenation(self, test_db: Database) -> None:
        """cnpj column = cnpj_basico || cnpj_ordem || cnpj_dv."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT cnpj, cnpj_basico, cnpj_ordem, cnpj_dv
                FROM data_quality_flags
                LIMIT 10
            """)
            for cnpj, basico, ordem, dv in cur.fetchall():
                assert cnpj == basico + ordem + dv, f"{cnpj} != {basico}+{ordem}+{dv}"
                assert len(cnpj) == 14, f"cnpj wrong length: {cnpj}"

    def test_cep_status_matches_source_predicate(self, test_db: Database) -> None:
        """cep_status should be a direct predicate over estabelecimentos.cep."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM data_quality_flags dq
                JOIN estabelecimentos e USING (cnpj_basico, cnpj_ordem, cnpj_dv)
                WHERE dq.cep_status IS DISTINCT FROM (
                    CASE
                        WHEN e.cep IS NULL THEN 'missing'
                        WHEN e.cep = '00000000' THEN 'zero_sentinel'
                        WHEN e.cep ~ '^\\d{8}$' THEN 'valid_shape'
                        ELSE 'malformed'
                    END
                )
            """)
            mismatches = fetch_row(cur)[0]
        assert mismatches == 0, f"cep_status mismatches source predicate for {mismatches} rows"

    def test_lookup_flags_match_reference_predicates(self, test_db: Database) -> None:
        """Lookup flags should mirror missing-reference predicates."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM data_quality_flags dq
                JOIN estabelecimentos e USING (cnpj_basico, cnpj_ordem, cnpj_dv)
                WHERE dq.pais_lookup_missing IS DISTINCT FROM (
                    e.pais IS NOT NULL
                    AND NOT EXISTS (SELECT 1 FROM paises p WHERE p.codigo = e.pais)
                )
            """)
            pais_mismatches = fetch_row(cur)[0]

            cur.execute("""
                SELECT COUNT(*)
                FROM data_quality_flags dq
                JOIN estabelecimentos e USING (cnpj_basico, cnpj_ordem, cnpj_dv)
                WHERE dq.motivo_lookup_missing IS DISTINCT FROM (
                    e.motivo_situacao_cadastral IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM motivos m
                        WHERE m.codigo = e.motivo_situacao_cadastral
                    )
                )
            """)
            motivo_mismatches = fetch_row(cur)[0]

            cur.execute("""
                SELECT COUNT(*)
                FROM data_quality_flags dq
                JOIN estabelecimentos e USING (cnpj_basico, cnpj_ordem, cnpj_dv)
                WHERE dq.pais_enriched_lookup_missing IS DISTINCT FROM (
                    e.pais IS NOT NULL
                    AND NOT EXISTS (SELECT 1 FROM paises_enriched p WHERE p.codigo = e.pais)
                )
                   OR dq.motivo_enriched_lookup_missing IS DISTINCT FROM (
                    e.motivo_situacao_cadastral IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM motivos_enriched m
                        WHERE m.codigo = e.motivo_situacao_cadastral
                    )
                )
            """)
            enriched_mismatches = fetch_row(cur)[0]

        assert pais_mismatches == 0, f"pais_lookup_missing mismatches for {pais_mismatches} rows"
        assert motivo_mismatches == 0, f"motivo_lookup_missing mismatches for {motivo_mismatches} rows"
        assert enriched_mismatches == 0, f"enriched lookup-missing flags mismatch for {enriched_mismatches} rows"

    def test_monthly_and_enriched_flags_diverge_on_supplemental(self, test_db: Database) -> None:
        """The whole point: a supplemental code (motivo 32, pais 150/994) is
        monthly-missing but enriched-resolved; a spurious code (pais 008) is
        missing under both."""
        with test_db.connect().cursor() as cur:
            # crafted motivo-32 estabelecimento: monthly-missing, enriched-resolved
            cur.execute(
                "SELECT motivo_lookup_missing, motivo_enriched_lookup_missing "
                "FROM data_quality_flags WHERE cnpj_basico = '99000001'"
            )
            assert fetch_row(cur) == (True, False), "motivo 32 should be monthly-missing, enriched-resolved"

            # pais 150 and 994: monthly-missing, enriched-resolved
            cur.execute(
                "SELECT pais_lookup_missing, pais_enriched_lookup_missing "
                "FROM data_quality_flags WHERE cnpj_basico IN ('99000002', '99000004') "
                "ORDER BY cnpj_basico"
            )
            assert cur.fetchall() == [(True, False), (True, False)]

            # pais 008: missing under both (spurious, intentionally unresolved)
            cur.execute(
                "SELECT pais_lookup_missing, pais_enriched_lookup_missing "
                "FROM data_quality_flags WHERE cnpj_basico = '99000003'"
            )
            assert fetch_row(cur) == (True, True), "spurious pais 008 must stay missing under both"

    def test_company_level_flag_matches_empresas_predicate(self, test_db: Database) -> None:
        """capital_social flag should be derived from empresas, not guessed
        from estabelecimento-level columns."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM data_quality_flags dq
                JOIN empresas emp USING (cnpj_basico)
                WHERE dq.capital_social_is_suspicious_sentinel
                    IS DISTINCT FROM (emp.capital_social = 999999999999)
            """)
            mismatches = fetch_row(cur)[0]

        assert mismatches == 0, f"capital sentinel flag mismatches for {mismatches} rows"

    def test_idempotent(self, test_db: Database) -> None:
        """Re-running the recipe should drop+recreate without error and
        produce the same row count."""
        sql = self.RECIPE_PATH.read_text()
        count_before = count_rows(test_db, "data_quality_flags")

        with test_db.connect().cursor() as cur:
            cur.execute(sql)
        test_db.connect().commit()

        count_after = count_rows(test_db, "data_quality_flags")
        assert count_before == count_after, f"Re-running recipe changed row count: {count_before} -> {count_after}"
