import pytest

from database import Database
from tests.integration.support import (
    RECIPES_DIR,
    count_rows,
    fetch_row,
)


@pytest.mark.usefixtures("empresas_busca_nome_counts")
class TestRecipeEmpresasBuscaNomeCounts:
    """Verify empresas busca nome counts against prepared PostgreSQL fixtures."""

    RECIPE_PATH = RECIPES_DIR / "empresas_busca_nome_counts.sql"

    def test_recipe_executes(self, test_db: Database) -> None:
        """The recipe SQL should parse and execute without errors."""

        with test_db.connect().cursor() as cur:
            cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'empresas_busca_nome_counts'")
            assert cur.fetchone() is not None, "empresas_busca_nome_counts table not created"

    def test_three_kinds_present(self, test_db: Database) -> None:
        """The rollup carries exactly three kinds: uf, uf_municipio, uf_cnae."""
        with test_db.connect().cursor() as cur:
            cur.execute("SELECT DISTINCT kind FROM empresas_busca_nome_counts ORDER BY kind")
            kinds = [row[0] for row in cur.fetchall()]
        assert kinds == ["uf", "uf_cnae", "uf_municipio"], kinds

    def test_sums_equal_main_table_per_kind(self, test_db: Database) -> None:
        """Each kind's totals must sum to the row count of the source
        table. If they diverge, the GROUP BY missed a row or the source
        changed between the build of the main recipe and the counts."""
        with test_db.connect().cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM empresas_busca_nome")
            base = fetch_row(cur)[0]

            cur.execute("SELECT kind, SUM(total) FROM empresas_busca_nome_counts GROUP BY kind")
            per_kind = dict(cur.fetchall())

        # SUM is returned as Decimal by psycopg2; coerce to int for the
        # equality comparison.
        for kind in ("uf", "uf_municipio", "uf_cnae"):
            assert int(per_kind[kind]) == base, f"{kind} sums to {per_kind[kind]}, expected {base}"

    def test_uf_municipio_has_both_codigo_and_nome(self, test_db: Database) -> None:
        """kind='uf_municipio' rows carry both municipio_codigo and
        municipio_nome. Consumers using either as the join key should get
        the same row."""
        with test_db.connect().cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM empresas_busca_nome_counts
                WHERE kind = 'uf_municipio'
                  AND (municipio_codigo IS NULL OR municipio_nome IS NULL)
                """
            )
            missing = fetch_row(cur)[0]
        assert missing == 0, f"{missing} uf_municipio rows missing codigo or nome"

    def test_lookup_by_name_and_by_codigo_agree(self, test_db: Database) -> None:
        """Looking up the same bucket via municipio_nome or municipio_codigo
        must return the same total."""
        with test_db.connect().cursor() as cur:
            cur.execute(
                """
                SELECT uf, municipio_codigo, municipio_nome, total
                FROM empresas_busca_nome_counts
                WHERE kind = 'uf_municipio'
                ORDER BY total DESC
                LIMIT 5
                """
            )
            samples = cur.fetchall()
            assert samples, "no uf_municipio rows to verify"

            for uf, codigo, nome, total in samples:
                cur.execute(
                    "SELECT total FROM empresas_busca_nome_counts "
                    "WHERE kind='uf_municipio' AND uf=%s AND municipio_codigo=%s",
                    (uf, codigo),
                )
                by_codigo = fetch_row(cur)[0]
                cur.execute(
                    "SELECT total FROM empresas_busca_nome_counts "
                    "WHERE kind='uf_municipio' AND uf=%s AND municipio_nome=%s",
                    (uf, nome),
                )
                by_nome = fetch_row(cur)[0]
                assert by_codigo == by_nome == total, (
                    f"divergent totals for ({uf}, {codigo}, {nome}): "
                    f"by_codigo={by_codigo} by_nome={by_nome} stored={total}"
                )

    def test_partial_unique_indexes_present(self, test_db: Database) -> None:
        """Every documented lookup pattern needs its partial unique index.
        These provide the intended lookup paths; planner choices depend
        on the query, statistics and table size."""
        with test_db.connect().cursor() as cur:
            cur.execute(
                """
                SELECT indexname FROM pg_indexes
                WHERE tablename = 'empresas_busca_nome_counts'
                """
            )
            indexes = {row[0] for row in cur.fetchall()}

        expected = {
            "idx_empresas_busca_nome_counts_uf",
            "idx_empresas_busca_nome_counts_uf_municipio",
            "idx_empresas_busca_nome_counts_uf_municipio_codigo",
            "idx_empresas_busca_nome_counts_uf_cnae",
        }
        missing = expected - indexes
        assert not missing, f"missing expected indexes: {missing}"

    def test_idempotent(self, test_db: Database) -> None:
        """Re-running the recipe should drop+recreate without error and
        produce the same row count."""
        sql = self.RECIPE_PATH.read_text()
        count_before = count_rows(test_db, "empresas_busca_nome_counts")

        with test_db.connect().cursor() as cur:
            cur.execute(sql)
        test_db.connect().commit()

        count_after = count_rows(test_db, "empresas_busca_nome_counts")
        assert count_before == count_after, f"re-running recipe changed row count: {count_before} -> {count_after}"
