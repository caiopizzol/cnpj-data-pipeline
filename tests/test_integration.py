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
