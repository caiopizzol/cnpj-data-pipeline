import pytest

from database import Database
from tests.integration.support import (
    RECIPES_DIR,
    count_rows,
    fetch_row,
)


@pytest.mark.usefixtures("estabelecimentos_clean")
class TestRecipeEstabelecimentosClean:
    """Verify estabelecimentos clean against prepared PostgreSQL fixtures."""

    RECIPE_PATH = RECIPES_DIR / "estabelecimentos_clean.sql"

    def test_recipe_executes(self, test_db: Database) -> None:
        """The recipe SQL should parse and execute after data_quality_flags."""

        with test_db.connect().cursor() as cur:
            cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'estabelecimentos_clean'")
            assert cur.fetchone() is not None, "estabelecimentos_clean table not created"

    def test_row_count_matches_flags(self, test_db: Database) -> None:
        """The recipe is one row per data_quality_flags row."""
        flags = count_rows(test_db, "data_quality_flags")
        clean = count_rows(test_db, "estabelecimentos_clean")
        assert clean > 0, "estabelecimentos_clean is empty"
        assert clean == flags, f"estabelecimentos_clean ({clean}) != data_quality_flags ({flags})"

    def test_schema_preserves_raw_and_clean_pairs(self, test_db: Database) -> None:
        """The table exposes raw + clean values and passes through flags,
        without becoming an empresa_detalhe replacement."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'estabelecimentos_clean'
            """)
            cols = {row[0] for row in cur.fetchall()}

        assert cols == {
            "cnpj_basico",
            "cnpj_ordem",
            "cnpj_dv",
            "cnpj",
            "cep_raw",
            "cep_clean",
            "capital_social_raw",
            "capital_social_clean",
            "cep_status",
            "capital_social_is_suspicious_sentinel",
            "pais_lookup_missing",
            "motivo_lookup_missing",
            "is_exterior",
        }

    def test_flags_are_passed_through_verbatim(self, test_db: Database) -> None:
        """Flag columns should be direct copies from data_quality_flags."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM estabelecimentos_clean ec
                JOIN data_quality_flags f USING (cnpj_basico, cnpj_ordem, cnpj_dv)
                WHERE ec.cep_status IS DISTINCT FROM f.cep_status
                   OR ec.capital_social_is_suspicious_sentinel IS DISTINCT FROM
                        f.capital_social_is_suspicious_sentinel
                   OR ec.pais_lookup_missing IS DISTINCT FROM f.pais_lookup_missing
                   OR ec.motivo_lookup_missing IS DISTINCT FROM f.motivo_lookup_missing
                   OR ec.is_exterior IS DISTINCT FROM f.is_exterior
            """)
            mismatches = fetch_row(cur)[0]

        assert mismatches == 0, f"flag pass-through mismatches for {mismatches} rows"

    def test_clean_values_use_flags_only(self, test_db: Database) -> None:
        """Clean columns should use the materialized flags rather than
        duplicating source predicates in a second place."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM estabelecimentos_clean ec
                JOIN estabelecimentos e USING (cnpj_basico, cnpj_ordem, cnpj_dv)
                JOIN empresas emp USING (cnpj_basico)
                JOIN data_quality_flags f USING (cnpj_basico, cnpj_ordem, cnpj_dv)
                WHERE ec.cep_raw IS DISTINCT FROM e.cep
                   OR ec.capital_social_raw IS DISTINCT FROM emp.capital_social
                   OR ec.cep_clean IS DISTINCT FROM (
                        CASE WHEN f.cep_status = 'valid_shape' THEN e.cep ELSE NULL END
                   )
                   OR ec.capital_social_clean IS DISTINCT FROM (
                        CASE
                            WHEN f.capital_social_is_suspicious_sentinel THEN NULL
                            ELSE emp.capital_social
                        END
                   )
            """)
            mismatches = fetch_row(cur)[0]

        assert mismatches == 0, f"clean values mismatch flag predicates for {mismatches} rows"

    def test_no_reference_or_label_columns(self, test_db: Database) -> None:
        """This recipe should not grow into empresa_detalhe. Reference-table
        descriptions and labels belong in other recipes."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'estabelecimentos_clean'
            """)
            cols = {row[0] for row in cur.fetchall()}

        forbidden = {
            "razao_social",
            "nome_fantasia",
            "cnae_fiscal_principal_descricao",
            "municipio_nome",
            "situacao_cadastral_descricao",
            "porte_descricao",
            "endereco_completo",
            "is_ativa",
            "is_matriz",
        }
        leaked = cols & forbidden
        assert not leaked, f"estabelecimentos_clean leaked unrelated columns: {leaked}"

    def test_idempotent(self, test_db: Database) -> None:
        """Re-running the recipe should drop+recreate without error and
        produce the same row count."""
        sql = self.RECIPE_PATH.read_text()
        count_before = count_rows(test_db, "estabelecimentos_clean")

        with test_db.connect().cursor() as cur:
            cur.execute(sql)
        test_db.connect().commit()

        count_after = count_rows(test_db, "estabelecimentos_clean")
        assert count_before == count_after, f"Re-running recipe changed row count: {count_before} -> {count_after}"
