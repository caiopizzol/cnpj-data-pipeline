"""Integration tests using real data fixtures against PostgreSQL.

Requires a running PostgreSQL instance (docker compose up -d postgres).
Skipped automatically in CI if DATABASE_URL is not set.
"""

from pathlib import Path

import psycopg2
import pytest

from database import Database
from processor import process_file

FIXTURES_DIR = Path(__file__).parent / "fixtures"
DATABASE_URL = "postgresql://postgres:postgres@localhost:5435/cnpj_test"

# Processing order (same as main.py — respects FK dependencies)
PROCESSING_ORDER = [
    "CNAECSV.csv",
    "MOTICSV.csv",
    "MUNICCSV.csv",
    "NATJUCSV.csv",
    "PAISCSV.csv",
    "QUALSCSV.csv",
    "EMPRECSV.csv",
    "ESTABELE.csv",
    "SOCIOCSV.csv",
    "SIMPLESCSV.csv",
]

EXPECTED_COUNTS = {
    "cnaes": 1359,
    "motivos": 63,
    "municipios": 5572,
    "naturezas_juridicas": 91,
    "paises": 255,
    "qualificacoes_socios": 68,
    "empresas": 2000,
    "estabelecimentos": 2000,
    "socios": 2000,
    "dados_simples": 2000,
}


def _pg_available() -> bool:
    """Check if PostgreSQL is reachable."""
    try:
        conn = psycopg2.connect(host="localhost", port=5435, user="postgres", password="postgres", dbname="postgres")
        conn.close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _pg_available(), reason="PostgreSQL not available")


@pytest.fixture(scope="module")
def test_db():
    """Create a test database, run schema, yield Database, then drop it."""
    # Connect to default db to create test db
    conn = psycopg2.connect(host="localhost", port=5435, user="postgres", password="postgres", dbname="postgres")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP DATABASE IF EXISTS cnpj_test")
        cur.execute("CREATE DATABASE cnpj_test")
    conn.close()

    # Run schema
    conn = psycopg2.connect(host="localhost", port=5435, user="postgres", password="postgres", dbname="cnpj_test")
    conn.autocommit = True
    schema = (Path(__file__).parent.parent / "initial.sql").read_text()
    with conn.cursor() as cur:
        cur.execute(schema)
    conn.close()

    db = Database(DATABASE_URL)

    yield db

    db.disconnect()

    # Drop test db
    conn = psycopg2.connect(host="localhost", port=5435, user="postgres", password="postgres", dbname="postgres")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP DATABASE IF EXISTS cnpj_test")
    conn.close()


def _count_rows(db: Database, table: str) -> int:
    """Count rows in a table."""
    with db.conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table}")
        return cur.fetchone()[0]


class TestFullPipeline:
    """Test processing all fixture files into PostgreSQL."""

    def test_load_all_fixtures(self, test_db):
        """Process all fixtures in order and verify row counts."""
        for fixture_name in PROCESSING_ORDER:
            fixture_path = FIXTURES_DIR / fixture_name
            assert fixture_path.exists(), f"Missing fixture: {fixture_name}"

            for batch, table_name, columns in process_file(fixture_path, batch_size=500000):
                test_db.bulk_upsert(batch, table_name, columns)

        # Verify row counts (may be less than fixture lines due to PK dedup)
        for table, expected in EXPECTED_COUNTS.items():
            actual = _count_rows(test_db, table)
            assert actual > 0, f"{table} is empty"
            assert actual <= expected, f"{table} has more rows ({actual}) than fixture ({expected})"

    def test_upsert_idempotency(self, test_db):
        """Loading the same data twice should not create duplicates."""
        # Get counts after first load
        counts_before = {table: _count_rows(test_db, table) for table in EXPECTED_COUNTS}

        # Load again
        for fixture_name in PROCESSING_ORDER:
            fixture_path = FIXTURES_DIR / fixture_name
            for batch, table_name, columns in process_file(fixture_path, batch_size=500000):
                test_db.bulk_upsert(batch, table_name, columns)

        # Counts should be identical
        for table, before in counts_before.items():
            after = _count_rows(test_db, table)
            assert after == before, f"{table}: {before} rows before, {after} after (duplicates created)"

    def test_data_integrity(self, test_db):
        """Verify data was loaded correctly — spot check key fields."""
        with test_db.conn.cursor() as cur:
            # CNAE codes should be 7 chars
            cur.execute("SELECT codigo FROM cnaes LIMIT 1")
            codigo = cur.fetchone()[0]
            assert len(codigo) == 7, f"CNAE code wrong length: {codigo}"

            # Country codes should be 3 chars (padded)
            cur.execute("SELECT DISTINCT pais FROM estabelecimentos WHERE pais IS NOT NULL LIMIT 5")
            for (pais,) in cur.fetchall():
                assert len(pais) == 3, f"Country code not padded: {pais}"

            # Capital social should be numeric (not Brazilian format)
            cur.execute("SELECT capital_social FROM empresas WHERE capital_social IS NOT NULL LIMIT 1")
            capital = cur.fetchone()[0]
            assert isinstance(capital, float), f"Capital social not float: {capital}"

            # No '0' or '00000000' dates should exist
            cur.execute("""
                SELECT count(*) FROM estabelecimentos
                WHERE data_situacao_cadastral::text IN ('0', '00000000')
            """)
            assert cur.fetchone()[0] == 0, "Found invalid dates in estabelecimentos"

    def test_replace_strategy(self, test_db):
        """Loading with bulk_insert should truncate and reload cleanly."""
        # Load with replace strategy
        test_db._truncated_tables.clear()
        for fixture_name in PROCESSING_ORDER:
            fixture_path = FIXTURES_DIR / fixture_name
            for batch, table_name, columns in process_file(fixture_path, batch_size=500000):
                test_db.bulk_insert(batch, table_name, columns)

        # Verify data is still there (not empty after truncate)
        for table, expected in EXPECTED_COUNTS.items():
            actual = _count_rows(test_db, table)
            assert actual > 0, f"{table} is empty after replace"
            assert actual <= expected, f"{table} has more rows ({actual}) than fixture ({expected})"

    def test_replace_handles_cross_batch_pk_overlap(self, test_db):
        """bulk_insert must handle PK overlap across batches of the same table.

        RFB occasionally ships the same (cnpj_basico, cnpj_ordem, cnpj_dv)
        across two sharded ZIPs of the same source table. Before the fix,
        the second batch's direct COPY into the target crashed on the PK
        constraint. The fix routes subsequent batches through a temp table
        + ON CONFLICT path so the load completes.

        Simulating cross-batch overlap by calling bulk_insert TWICE on the
        same estabelecimentos fixture - second call must not raise.
        """
        test_db._truncated_tables.clear()
        fixture_path = FIXTURES_DIR / "ESTABELE.csv"
        all_batches = list(process_file(fixture_path, batch_size=500000))
        assert len(all_batches) > 0, "fixture produced no batches"

        # First pass: truncate + load (fast path)
        for batch, table_name, columns in all_batches:
            test_db.bulk_insert(batch, table_name, columns)
        count_after_first = _count_rows(test_db, "estabelecimentos")
        assert count_after_first > 0

        # Second pass: same data, should hit the overlap path. The fix
        # makes this complete without raising; the row count stays stable
        # because every (basico, ordem, dv) already exists.
        for batch, table_name, columns in all_batches:
            test_db.bulk_insert(batch, table_name, columns)
        count_after_second = _count_rows(test_db, "estabelecimentos")
        assert count_after_second == count_after_first, (
            f"cross-batch overlap path changed row count: "
            f"{count_after_first} -> {count_after_second}"
        )


class TestRecipeEmpresaDetalhe:
    """Apply recipes/postgres/empresa_detalhe.sql against the fixture-loaded
    database and verify the derived table has the expected shape.

    Depends on test_db being populated by TestFullPipeline; pytest runs
    classes in file order so the fixture state from earlier tests is
    available here. We don't reload fixtures - the database fixture is
    module-scoped and TestFullPipeline.test_load_all_fixtures already
    populated it.
    """

    RECIPE_PATH = Path(__file__).parent.parent / "recipes" / "postgres" / "empresa_detalhe.sql"

    def test_recipe_executes(self, test_db):
        """The recipe SQL should parse and execute without errors."""
        sql = self.RECIPE_PATH.read_text()
        with test_db.conn.cursor() as cur:
            cur.execute(sql)
        test_db.conn.commit()

        # Table exists
        with test_db.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'empresa_detalhe'")
            assert cur.fetchone() is not None, "empresa_detalhe table not created"

    def test_row_count_matches_empresa_estabelecimento_join(self, test_db):
        """The recipe INNER-JOINs empresas with estabelecimentos and then
        LEFT-JOINs reference tables + dados_simples. So the output row count
        must equal the cardinality of empresas JOIN estabelecimentos USING
        (cnpj_basico) - no rows lost to the LEFT JOINs, no rows gained.

        In real prod data this is also = COUNT(*) FROM estabelecimentos
        because every estabelecimento has a parent empresa. The test
        fixtures sample independently so overlap is partial, which is why
        we assert against the JOIN cardinality, not the raw count."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM empresas e
                JOIN estabelecimentos s USING (cnpj_basico)
            """)
            expected = cur.fetchone()[0]
        ed = _count_rows(test_db, "empresa_detalhe")
        assert ed > 0, "empresa_detalhe is empty"
        assert ed == expected, (
            f"empresa_detalhe ({ed}) != empresas⋈estabelecimentos ({expected}) - "
            f"LEFT JOINs on reference tables or dados_simples must be preserving "
            f"all rows from the base inner join"
        )

    def test_cnpj_column_is_concatenation(self, test_db):
        """cnpj column = cnpj_basico || cnpj_ordem || cnpj_dv."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT cnpj, cnpj_basico, cnpj_ordem, cnpj_dv
                FROM empresa_detalhe
                LIMIT 10
            """)
            for cnpj, basico, ordem, dv in cur.fetchall():
                assert cnpj == basico + ordem + dv, f"{cnpj} != {basico}+{ordem}+{dv}"
                assert len(cnpj) == 14, f"cnpj wrong length: {cnpj}"

    def test_reference_descriptions_joined(self, test_db):
        """When a code has a matching reference-table row, the description
        column should be populated."""
        with test_db.conn.cursor() as cur:
            # At least some rows should have all reference descriptions
            cur.execute("""
                SELECT COUNT(*) FROM empresa_detalhe
                WHERE cnae_fiscal_principal_descricao IS NOT NULL
                  AND municipio_nome IS NOT NULL
                  AND natureza_juridica_descricao IS NOT NULL
            """)
            count = cur.fetchone()[0]
            assert count > 0, "No rows have all reference descriptions joined"

    def test_dados_simples_columns_present(self, test_db):
        """dados_simples LEFT JOIN should expose raw columns. Some rows may
        have NULL Simples (no record), but the columns must exist."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'empresa_detalhe'
                  AND column_name IN (
                    'opcao_pelo_simples', 'data_opcao_pelo_simples',
                    'data_exclusao_do_simples', 'opcao_pelo_mei',
                    'data_opcao_pelo_mei', 'data_exclusao_do_mei'
                  )
            """)
            cols = {row[0] for row in cur.fetchall()}
            assert cols == {
                "opcao_pelo_simples",
                "data_opcao_pelo_simples",
                "data_exclusao_do_simples",
                "opcao_pelo_mei",
                "data_opcao_pelo_mei",
                "data_exclusao_do_mei",
            }, f"Missing dados_simples columns: {cols}"

    def test_no_derived_columns_leaked(self, test_db):
        """The recipe should NOT add opinionated columns like is_ativa,
        is_matriz, or label-substituted enums (situacao_cadastral stays
        as the source code, not 'Ativa'). Sanity check against scope creep."""
        with test_db.conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'empresa_detalhe'
            """)
            cols = {row[0] for row in cur.fetchall()}
            forbidden = {
                "is_ativa",
                "is_matriz",
                "is_optante_simples",
                "is_mei",
                "situacao_cadastral_descricao",
                "porte_descricao",
                "cnpj_formatado",
                "endereco_completo",
            }
            leaked = cols & forbidden
            assert not leaked, f"Recipe leaked opinionated columns: {leaked}"

    def test_idempotent(self, test_db):
        """Re-running the recipe should drop+recreate without error and
        produce the same row count."""
        sql = self.RECIPE_PATH.read_text()
        count_before = _count_rows(test_db, "empresa_detalhe")

        with test_db.conn.cursor() as cur:
            cur.execute(sql)
        test_db.conn.commit()

        count_after = _count_rows(test_db, "empresa_detalhe")
        assert count_before == count_after, f"Re-running recipe changed row count: {count_before} -> {count_after}"
