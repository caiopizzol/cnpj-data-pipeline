import pytest

from database import Database
from tests.integration.support import (
    RECIPES_DIR,
    count_rows,
    fetch_row,
)


@pytest.mark.usefixtures("cnae_secundaria_exploded")
class TestRecipeCnaeSecundariaExploded:
    """Verify cnae secundaria exploded against prepared PostgreSQL fixtures."""

    RECIPE_PATH = RECIPES_DIR / "cnae_secundaria_exploded.sql"

    def test_recipe_executes(self, test_db: Database) -> None:
        """The recipe SQL should parse and execute without errors."""

        with test_db.connect().cursor() as cur:
            cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'cnae_secundaria_exploded'")
            assert cur.fetchone() is not None, "cnae_secundaria_exploded table not created"

    def test_row_count_matches_string_split_cardinality(self, test_db: Database) -> None:
        """Output cardinality should match the trimmed non-empty entries from
        cnae_fiscal_secundaria. No rows are gained through joins."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM estabelecimentos e
                CROSS JOIN LATERAL unnest(string_to_array(e.cnae_fiscal_secundaria, ',')) AS code(raw_code)
                WHERE e.cnae_fiscal_secundaria IS NOT NULL
                  AND trim(code.raw_code) <> ''
            """)
            expected = fetch_row(cur)[0]

        actual = count_rows(test_db, "cnae_secundaria_exploded")
        assert actual > 0, "cnae_secundaria_exploded is empty"
        assert actual == expected, f"cnae_secundaria_exploded ({actual}) != split cardinality ({expected})"

    def test_schema_is_side_table_only(self, test_db: Database) -> None:
        """The recipe should stay as key + cnae_codigo. No descriptions,
        validation flags, or position column in v1."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'cnae_secundaria_exploded'
            """)
            cols = {row[0] for row in cur.fetchall()}

        assert cols == {
            "cnpj_basico",
            "cnpj_ordem",
            "cnpj_dv",
            "cnpj",
            "cnae_codigo",
        }

    def test_cnpj_column_is_concatenation(self, test_db: Database) -> None:
        """cnpj column = cnpj_basico || cnpj_ordem || cnpj_dv."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT cnpj, cnpj_basico, cnpj_ordem, cnpj_dv
                FROM cnae_secundaria_exploded
                LIMIT 10
            """)
            for cnpj, basico, ordem, dv in cur.fetchall():
                assert cnpj == basico + ordem + dv, f"{cnpj} != {basico}+{ordem}+{dv}"
                assert len(cnpj) == 14, f"cnpj wrong length: {cnpj}"

    def test_codes_are_trimmed_and_non_empty(self, test_db: Database) -> None:
        """The only normalization in the recipe is trimming whitespace and
        dropping empty split entries."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM cnae_secundaria_exploded
                WHERE cnae_codigo IS NULL
                   OR cnae_codigo = ''
                   OR cnae_codigo <> trim(cnae_codigo)
            """)
            bad_rows = fetch_row(cur)[0]

        assert bad_rows == 0, f"Found {bad_rows} untrimmed or empty CNAE rows"

    def test_no_reference_join_or_description_columns(self, test_db: Database) -> None:
        """Consumers LEFT JOIN cnaes themselves when they want descriptions.
        The recipe should preserve codes only."""
        with test_db.connect().cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'cnae_secundaria_exploded'
            """)
            cols = {row[0] for row in cur.fetchall()}

        forbidden = {
            "descricao",
            "cnae_descricao",
            "cnae_fiscal_secundaria_descricao",
            "cnae_lookup_missing",
            "position",
            "ordinality",
        }
        leaked = cols & forbidden
        assert not leaked, f"cnae_secundaria_exploded leaked interpretation columns: {leaked}"

    def test_idempotent(self, test_db: Database) -> None:
        """Re-running the recipe should drop+recreate without error and
        produce the same row count."""
        sql = self.RECIPE_PATH.read_text()
        count_before = count_rows(test_db, "cnae_secundaria_exploded")

        with test_db.connect().cursor() as cur:
            cur.execute(sql)
        test_db.connect().commit()

        count_after = count_rows(test_db, "cnae_secundaria_exploded")
        assert count_before == count_after, f"Re-running recipe changed row count: {count_before} -> {count_after}"
