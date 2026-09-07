import pytest

from database import Database
from tests.integration.support import (
    RECIPES_DIR,
    count_rows,
    fetch_row,
)


@pytest.mark.usefixtures("empresas_busca_nome")
class TestRecipeEmpresasBuscaNome:
    """Verify empresas busca nome against prepared PostgreSQL fixtures."""

    RECIPE_PATH = RECIPES_DIR / "empresas_busca_nome.sql"

    def test_recipe_executes(self, test_db: Database) -> None:
        """The recipe SQL should parse and execute without errors."""

        with test_db.connect().cursor() as cur:
            cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'empresas_busca_nome'")
            assert cur.fetchone() is not None, "empresas_busca_nome table not created"

    def test_row_count_matches_active_matriz_join(self, test_db: Database) -> None:
        """Row count must equal empresas JOIN estabelecimentos USING (cnpj_basico)
        filtered to active matriz. LEFT JOINs on cnaes and municipios must
        not add or drop rows."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM empresas e
                JOIN estabelecimentos est USING (cnpj_basico)
                WHERE est.situacao_cadastral = '02'
                  AND est.identificador_matriz_filial = 1
            """)
            expected = fetch_row(cur)[0]

        actual = count_rows(test_db, "empresas_busca_nome")
        assert actual > 0, "empresas_busca_nome is empty - fixture has no active-matriz overlap"
        assert actual == expected, (
            f"empresas_busca_nome ({actual}) != active-matriz join ({expected}) - "
            "reference LEFT JOINs on cnaes/municipios must preserve all rows"
        )

    def test_only_active_matriz_rows(self, test_db: Database) -> None:
        """Every row must satisfy the filter predicate. Guards against the
        WHERE clause being weakened or against the type comparison drifting
        (identificador_matriz_filial is INTEGER, situacao_cadastral is text)."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM empresas_busca_nome
                WHERE situacao_cadastral <> '02'
                   OR identificador_matriz_filial <> 1
            """)
            violations = fetch_row(cur)[0]
        assert violations == 0, f"{violations} rows violate the active-matriz predicate"

    def test_cnpj_column_is_concatenation(self, test_db: Database) -> None:
        """cnpj column = cnpj_basico || cnpj_ordem || cnpj_dv, same convention
        as empresa_detalhe."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT cnpj, cnpj_basico, cnpj_ordem, cnpj_dv
                FROM empresas_busca_nome
                LIMIT 10
            """)
            rows = cur.fetchall()
            assert rows, "no rows to verify cnpj concatenation"
            for cnpj, basico, ordem, dv in rows:
                assert cnpj == basico + ordem + dv, f"{cnpj} != {basico}+{ordem}+{dv}"
                assert len(cnpj) == 14, f"cnpj wrong length: {cnpj}"

    def test_reference_descriptions_joined(self, test_db: Database) -> None:
        """When estabelecimento codes have matching reference rows the
        denormalized description columns should be populated."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM empresas_busca_nome
                WHERE cnae_descricao IS NOT NULL
                  AND municipio_nome IS NOT NULL
            """)
            count = fetch_row(cur)[0]
            assert count > 0, "no rows have both cnae_descricao and municipio_nome joined"

    def test_expected_indexes_exist(self, test_db: Database) -> None:
        """The expected search indexes must exist; verify the UF-prefix definition too."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT indexname, indexdef FROM pg_indexes
                WHERE tablename = 'empresas_busca_nome'
            """)
            definitions = dict(cur.fetchall())
            indexes = set(definitions)

        expected = {
            "pk_empresas_busca_nome",
            "idx_empresas_busca_nome_cnpj",
            "idx_empresas_busca_nome_razao_prefix",
            "idx_empresas_busca_nome_uf_razao",
            "idx_empresas_busca_nome_uf_razao_prefix",
            "idx_empresas_busca_nome_uf_municipio_razao",
            "idx_empresas_busca_nome_uf_cnae_razao",
        }
        missing = expected - indexes
        assert not missing, f"missing expected indexes: {missing}"
        definition = definitions["idx_empresas_busca_nome_uf_razao_prefix"]
        assert "USING btree (uf, razao_social text_pattern_ops, cnpj_basico, cnpj_ordem)" in definition

    def test_no_derived_columns_leaked(self, test_db: Database) -> None:
        """The recipe filters rows but does not synthesize labels or
        booleans. Source codes stay; no is_ativa, no situacao_cadastral_descricao."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'empresas_busca_nome'
            """)
            cols = {row[0] for row in cur.fetchall()}

        forbidden = {
            "is_ativa",
            "is_matriz",
            "situacao_cadastral_descricao",
            "identificador_matriz_filial_descricao",
        }
        leaked = cols & forbidden
        assert not leaked, f"recipe leaked opinionated columns: {leaked}"

    def test_idempotent(self, test_db: Database) -> None:
        """Re-running the recipe should drop+recreate without error and
        produce the same row count."""
        sql = self.RECIPE_PATH.read_text()
        count_before = count_rows(test_db, "empresas_busca_nome")

        with test_db.connect().cursor() as cur:
            cur.execute(sql)
        test_db.connect().commit()

        count_after = count_rows(test_db, "empresas_busca_nome")
        assert count_before == count_after, f"re-running recipe changed row count: {count_before} -> {count_after}"
