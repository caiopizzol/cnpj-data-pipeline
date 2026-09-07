import pytest

from database import Database
from tests.integration.support import (
    RECIPES_DIR,
    count_rows,
    fetch_row,
)


@pytest.mark.usefixtures("socios_clean")
class TestRecipeSociosClean:
    """Verify socios clean against prepared PostgreSQL fixtures."""

    RECIPE_PATH = RECIPES_DIR / "socios_clean.sql"

    def test_recipe_executes(self, test_db: Database) -> None:
        """The recipe SQL should parse and execute after socios_quality_flags."""

        with test_db.connect().cursor() as cur:
            cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'socios_clean'")
            assert cur.fetchone() is not None, "socios_clean table not created"

    def test_row_count_matches_flags(self, test_db: Database) -> None:
        """The recipe is one row per socios_quality_flags row."""
        flags = count_rows(test_db, "socios_quality_flags")
        clean = count_rows(test_db, "socios_clean")
        assert clean > 0, "socios_clean is empty"
        assert clean == flags, f"socios_clean ({clean}) != socios_quality_flags ({flags})"

    def test_schema_preserves_raw_and_clean_pairs(self, test_db: Database) -> None:
        """The table exposes raw + clean values and passes through flags,
        without becoming socios_detalhe."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'socios_clean'
            """)
            cols = {row[0] for row in cur.fetchall()}

        assert cols == {
            "socio_id",
            "cnpj_basico",
            "identificador_de_socio",
            "cnpj_cpf_do_socio",
            "representante_legal_raw",
            "representante_legal_clean",
            "nome_do_representante_raw",
            "nome_do_representante_clean",
            "qualificacao_do_representante_legal_raw",
            "qualificacao_do_representante_legal_clean",
            "faixa_etaria_raw",
            "faixa_etaria_clean",
            "representante_is_placeholder",
            "pais_lookup_missing",
            "qualificacao_socio_lookup_missing",
            "qualificacao_representante_lookup_missing",
            "faixa_etaria_nao_se_aplica",
        }

    def test_flags_are_passed_through_verbatim(self, test_db: Database) -> None:
        """Flag columns should be direct copies from socios_quality_flags."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM socios_clean sc
                JOIN socios_quality_flags f USING (socio_id)
                WHERE sc.representante_is_placeholder IS DISTINCT FROM f.representante_is_placeholder
                   OR sc.pais_lookup_missing IS DISTINCT FROM f.pais_lookup_missing
                   OR sc.qualificacao_socio_lookup_missing IS DISTINCT FROM f.qualificacao_socio_lookup_missing
                   OR sc.qualificacao_representante_lookup_missing IS DISTINCT FROM
                        f.qualificacao_representante_lookup_missing
                   OR sc.faixa_etaria_nao_se_aplica IS DISTINCT FROM f.faixa_etaria_nao_se_aplica
            """)
            mismatches = fetch_row(cur)[0]

        assert mismatches == 0, f"flag pass-through mismatches for {mismatches} rows"

    def test_clean_values_use_flags_only(self, test_db: Database) -> None:
        """Clean columns should use the materialized flags rather than
        duplicating source predicates in a second place."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM socios_clean sc
                JOIN socios s USING (socio_id)
                JOIN socios_quality_flags f USING (socio_id)
                WHERE sc.representante_legal_raw IS DISTINCT FROM s.representante_legal
                   OR sc.nome_do_representante_raw IS DISTINCT FROM s.nome_do_representante
                   OR sc.qualificacao_do_representante_legal_raw IS DISTINCT FROM
                        s.qualificacao_do_representante_legal
                   OR sc.faixa_etaria_raw IS DISTINCT FROM s.faixa_etaria
                   OR sc.representante_legal_clean IS DISTINCT FROM (
                        CASE WHEN f.representante_is_placeholder THEN NULL ELSE s.representante_legal END
                   )
                   OR sc.nome_do_representante_clean IS DISTINCT FROM (
                        CASE WHEN f.representante_is_placeholder THEN NULL ELSE s.nome_do_representante END
                   )
                   OR sc.qualificacao_do_representante_legal_clean IS DISTINCT FROM (
                        CASE
                            WHEN f.representante_is_placeholder THEN NULL
                            ELSE s.qualificacao_do_representante_legal
                        END
                   )
                   OR sc.faixa_etaria_clean IS DISTINCT FROM (
                        CASE WHEN f.faixa_etaria_nao_se_aplica THEN NULL ELSE s.faixa_etaria END
                   )
            """)
            mismatches = fetch_row(cur)[0]

        assert mismatches == 0, f"clean values mismatch flag predicates for {mismatches} rows"

    def test_no_reference_or_label_columns(self, test_db: Database) -> None:
        """This recipe should not grow into socios_detalhe. Reference-table
        descriptions and labels belong in other recipes."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'socios_clean'
            """)
            cols = {row[0] for row in cur.fetchall()}

        forbidden = {
            "nome_socio",
            "identificador_de_socio_descricao",
            "qualificacao_do_socio_descricao",
            "qualificacao_do_representante_legal_descricao",
            "pais_descricao",
            "faixa_etaria_descricao",
            "socio_type",
            "is_representante_pj",
        }
        leaked = cols & forbidden
        assert not leaked, f"socios_clean leaked unrelated columns: {leaked}"

    def test_idempotent(self, test_db: Database) -> None:
        """Re-running the recipe should drop+recreate without error and
        produce the same row count."""
        sql = self.RECIPE_PATH.read_text()
        count_before = count_rows(test_db, "socios_clean")

        with test_db.connect().cursor() as cur:
            cur.execute(sql)
        test_db.connect().commit()

        count_after = count_rows(test_db, "socios_clean")
        assert count_before == count_after, f"Re-running recipe changed row count: {count_before} -> {count_after}"
