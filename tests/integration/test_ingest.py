from datetime import date

from database import Database
from processor import process_file
from tests.integration.support import (
    EXPECTED_COUNTS,
    FIXTURES_DIR,
    PROCESSING_ORDER,
    count_rows,
    fetch_row,
)


class TestFullPipeline:
    """Verify ingest against prepared PostgreSQL fixtures."""

    def test_load_all_fixtures(self, empty_db: Database) -> None:
        """Process all fixtures in order and verify row counts."""
        for fixture_name in PROCESSING_ORDER:
            fixture_path = FIXTURES_DIR / fixture_name
            assert fixture_path.exists(), f"Missing fixture: {fixture_name}"

            for batch, table_name, columns in process_file(fixture_path, batch_size=500000):
                empty_db.bulk_upsert(batch, table_name, columns)

        for table, expected in EXPECTED_COUNTS.items():
            actual = count_rows(empty_db, table)
            assert actual == expected, f"{table}: expected {expected} rows, got {actual}"

    def test_upsert_idempotency(self, test_db: Database) -> None:
        """Loading the same data twice should not create duplicates."""
        # Get counts after first load
        counts_before = {table: count_rows(test_db, table) for table in EXPECTED_COUNTS}

        # Load again
        for fixture_name in PROCESSING_ORDER:
            fixture_path = FIXTURES_DIR / fixture_name
            for batch, table_name, columns in process_file(fixture_path, batch_size=500000):
                test_db.bulk_upsert(batch, table_name, columns)

        # Counts should be identical
        for table, before in counts_before.items():
            after = count_rows(test_db, table)
            assert after == before, f"{table}: {before} rows before, {after} after (duplicates created)"

    def test_data_integrity(self, test_db: Database) -> None:
        """Verify data was loaded correctly — spot check key fields."""
        with test_db.connect().cursor() as cur:
            # CNAE codes should be 7 chars
            cur.execute("SELECT codigo FROM cnaes LIMIT 1")
            codigo = fetch_row(cur)[0]
            assert len(codigo) == 7, f"CNAE code wrong length: {codigo}"

            # Country codes should be 3 chars (padded)
            cur.execute("SELECT DISTINCT pais FROM estabelecimentos WHERE pais IS NOT NULL LIMIT 5")
            for (pais,) in cur.fetchall():
                assert len(pais) == 3, f"Country code not padded: {pais}"

            # Capital social should be numeric (not Brazilian format)
            cur.execute("SELECT capital_social FROM empresas WHERE capital_social IS NOT NULL LIMIT 1")
            capital = fetch_row(cur)[0]
            assert isinstance(capital, float), f"Capital social not float: {capital}"

            cur.execute("""
                SELECT cnpj_basico, data_situacao_cadastral
                FROM estabelecimentos
                WHERE cnpj_basico IN ('07163346', '13810056')
                  AND cnpj_ordem = '0001'
                ORDER BY cnpj_basico
            """)
            assert cur.fetchall() == [("07163346", None), ("13810056", date(2016, 11, 25))]

    def test_replace_strategy(self, test_db: Database) -> None:
        """Loading with bulk_insert should truncate and reload cleanly."""
        # Load with replace strategy
        for fixture_name in PROCESSING_ORDER:
            fixture_path = FIXTURES_DIR / fixture_name
            for batch, table_name, columns in process_file(fixture_path, batch_size=500000):
                test_db.bulk_insert(batch, table_name, columns)

        # Verify data is still there (not empty after truncate)
        for table, expected in EXPECTED_COUNTS.items():
            actual = count_rows(test_db, table)
            assert actual == expected, f"{table}: expected {expected} rows after replace, got {actual}"

    def test_replace_handles_cross_batch_pk_overlap(self, test_db: Database) -> None:
        """bulk_insert must handle PK overlap across batches of the same table.

        RFB occasionally ships the same (cnpj_basico, cnpj_ordem, cnpj_dv)
        across two sharded ZIPs of the same source table. Before the fix,
        the second batch's direct COPY into the target crashed on the PK
        constraint. The fix routes subsequent batches through a temp table
        + ON CONFLICT path so the load completes.

        Simulating cross-batch overlap by calling bulk_insert TWICE on the
        same estabelecimentos fixture - second call must not raise.
        """
        fixture_path = FIXTURES_DIR / "ESTABELE.csv"
        all_batches = list(process_file(fixture_path, batch_size=500000))
        assert len(all_batches) > 0, "fixture produced no batches"

        # First pass: truncate + load (fast path)
        for batch, table_name, columns in all_batches:
            test_db.bulk_insert(batch, table_name, columns)
        count_after_first = count_rows(test_db, "estabelecimentos")
        assert count_after_first > 0

        # Second pass: same data, should hit the overlap path. The fix
        # makes this complete without raising; the row count stays stable
        # because every (basico, ordem, dv) already exists.
        for batch, table_name, columns in all_batches:
            test_db.bulk_insert(batch, table_name, columns)
        count_after_second = count_rows(test_db, "estabelecimentos")
        assert count_after_second == count_after_first, (
            f"cross-batch overlap path changed row count: {count_after_first} -> {count_after_second}"
        )
