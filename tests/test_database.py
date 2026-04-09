"""Tests for database module."""

from unittest.mock import MagicMock, patch

import polars as pl
import psycopg2
import pytest

from database import Database


@pytest.fixture
def db():
    """Create a Database instance with a test URL."""
    return Database("postgresql://user:pass@localhost:5432/testdb")


@pytest.fixture
def connected_db(db):
    """Create a Database instance with a mocked connection."""
    db.conn = MagicMock()
    return db


class TestParseUrl:
    """Test DATABASE_URL parsing."""

    def test_parses_full_url(self, db):
        params = db._parse_url()
        assert params["host"] == "localhost"
        assert params["port"] == 5432
        assert params["database"] == "testdb"
        assert params["user"] == "user"
        assert params["password"] == "pass"

    def test_default_port(self):
        db = Database("postgresql://user:pass@localhost/testdb")
        params = db._parse_url()
        assert params["port"] == 5432


class TestConnect:
    """Test connection with retry logic."""

    @patch("database.psycopg2.connect")
    def test_connects_successfully(self, mock_connect, db):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        db.connect()

        assert db.conn is mock_conn
        assert mock_conn.autocommit is False

    @patch("database.psycopg2.connect")
    def test_noop_when_already_connected(self, mock_connect, db):
        db.conn = MagicMock()

        db.connect()

        mock_connect.assert_not_called()

    @patch("database.time.sleep")
    @patch("database.psycopg2.connect")
    def test_retries_on_operational_error(self, mock_connect, mock_sleep, db):
        mock_conn = MagicMock()
        mock_connect.side_effect = [
            psycopg2.OperationalError("fail"),
            psycopg2.OperationalError("fail"),
            mock_conn,
        ]

        db.connect()

        assert mock_connect.call_count == 3
        assert db.conn is mock_conn

    @patch("database.time.sleep")
    @patch("database.psycopg2.connect")
    def test_raises_after_max_retries(self, mock_connect, mock_sleep, db):
        mock_connect.side_effect = psycopg2.OperationalError("fail")

        with pytest.raises(psycopg2.OperationalError):
            db.connect()

        assert mock_connect.call_count == 3  # default retry_attempts


class TestDisconnect:
    """Test connection cleanup."""

    def test_closes_connection(self, connected_db):
        conn = connected_db.conn

        connected_db.disconnect()

        conn.close.assert_called_once()
        assert connected_db.conn is None

    def test_noop_when_not_connected(self, db):
        db.disconnect()  # Should not raise


class TestGetProcessedFiles:
    """Test processed file tracking."""

    def test_returns_set_of_filenames(self, connected_db):
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [("file1.zip",), ("file2.zip",)]
        connected_db.conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        connected_db.conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = connected_db.get_processed_files("2024-01")

        assert result == {"file1.zip", "file2.zip"}

    def test_raises_on_error(self, connected_db):
        mock_cur = MagicMock()
        mock_cur.execute.side_effect = psycopg2.OperationalError("connection lost")
        connected_db.conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        connected_db.conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with pytest.raises(psycopg2.OperationalError):
            connected_db.get_processed_files("2024-01")


class TestMarkProcessed:
    """Test marking files as processed."""

    def test_inserts_and_commits(self, connected_db):
        mock_cur = MagicMock()
        connected_db.conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        connected_db.conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        connected_db.mark_processed("2024-01", "file.zip")

        mock_cur.execute.assert_called_once()
        assert "INSERT INTO processed_files" in mock_cur.execute.call_args[0][0]
        connected_db.conn.commit.assert_called_once()


class TestClearProcessedFiles:
    """Test clearing processed file records."""

    def test_deletes_and_commits(self, connected_db):
        mock_cur = MagicMock()
        connected_db.conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        connected_db.conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        connected_db.clear_processed_files("2024-01")

        mock_cur.execute.assert_called_once()
        assert "DELETE FROM processed_files" in mock_cur.execute.call_args[0][0]
        connected_db.conn.commit.assert_called_once()


class TestTruncateTable:
    """Test explicit table truncation for parallel processing."""

    def test_truncates_and_tracks(self, connected_db):
        mock_cur = MagicMock()
        connected_db.conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        connected_db.conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        connected_db.truncate_table("empresas")

        mock_cur.execute.assert_called_once()
        assert "TRUNCATE TABLE empresas CASCADE" in mock_cur.execute.call_args[0][0]
        connected_db.conn.commit.assert_called_once()
        assert "empresas" in connected_db._truncated_tables

    def test_pre_truncated_constructor_param(self):
        """Database should accept pre_truncated tables via constructor."""
        db = Database("postgresql://test", pre_truncated={"empresas", "socios"})
        assert db._truncated_tables == {"empresas", "socios"}


class TestBulkUpsert:
    """Test bulk upsert with temp table strategy."""

    def test_skips_empty_dataframe(self, connected_db):
        df = pl.DataFrame({"col": []})

        connected_db.bulk_upsert(df, "test_table", ["col"])

        connected_db.conn.cursor.assert_not_called()

    def test_happy_path_sequence(self, connected_db):
        """Verify: create temp → copy → get PKs → upsert → commit."""
        df = pl.DataFrame({"codigo": ["001"], "descricao": ["Test"]})
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [("codigo",)]
        connected_db.conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        connected_db.conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        connected_db.bulk_upsert(df, "cnaes", ["codigo", "descricao"])

        calls = mock_cur.execute.call_args_list
        # 1. CREATE TEMP TABLE
        assert "CREATE TEMP TABLE" in calls[0][0][0]
        # 2. COPY (via copy_expert)
        mock_cur.copy_expert.assert_called_once()
        assert "COPY" in mock_cur.copy_expert.call_args[0][0]
        # 3. PK lookup
        assert "pg_index" in calls[1][0][0]
        # 4. INSERT ... ON CONFLICT
        assert "INSERT INTO cnaes" in calls[2][0][0]
        assert "ON CONFLICT" in calls[2][0][0]
        # 5. Commit
        connected_db.conn.commit.assert_called_once()

    def test_rollback_on_error(self, connected_db):
        df = pl.DataFrame({"codigo": ["001"]})
        mock_cur = MagicMock()
        mock_cur.execute.side_effect = Exception("DB error")
        connected_db.conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        connected_db.conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with pytest.raises(Exception, match="DB error"):
            connected_db.bulk_upsert(df, "cnaes", ["codigo"])

        connected_db.conn.rollback.assert_called_once()
        connected_db.conn.commit.assert_not_called()


class TestBulkInsert:
    """Test bulk insert with TRUNCATE + COPY strategy."""

    def test_skips_empty_dataframe(self, connected_db):
        df = pl.DataFrame({"col": []})

        connected_db.bulk_insert(df, "test_table", ["col"])

        connected_db.conn.cursor.assert_not_called()

    def test_truncates_on_first_batch(self, connected_db):
        df = pl.DataFrame({"codigo": ["001"]})
        mock_cur = MagicMock()
        connected_db.conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        connected_db.conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        connected_db.bulk_insert(df, "cnaes", ["codigo"])

        calls = [c[0][0] for c in mock_cur.execute.call_args_list]
        assert any("TRUNCATE" in c for c in calls)
        mock_cur.copy_expert.assert_called_once()
        connected_db.conn.commit.assert_called_once()

    def test_does_not_truncate_on_second_batch(self, connected_db):
        df = pl.DataFrame({"codigo": ["001"]})
        mock_cur = MagicMock()
        connected_db.conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        connected_db.conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        connected_db.bulk_insert(df, "cnaes", ["codigo"])
        mock_cur.reset_mock()
        connected_db.conn.reset_mock()

        connected_db.bulk_insert(df, "cnaes", ["codigo"])

        calls = [c[0][0] for c in mock_cur.execute.call_args_list]
        assert not any("TRUNCATE" in c for c in calls)

    def test_rollback_on_error(self, connected_db):
        df = pl.DataFrame({"codigo": ["001"]})
        mock_cur = MagicMock()
        mock_cur.execute.side_effect = Exception("DB error")
        connected_db.conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        connected_db.conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with pytest.raises(Exception, match="DB error"):
            connected_db.bulk_insert(df, "cnaes", ["codigo"])

        connected_db.conn.rollback.assert_called_once()


class TestGetPrimaryKeys:
    """Test primary key lookup with caching."""

    def test_queries_and_caches(self, connected_db):
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [("cnpj_basico",), ("cnpj_ordem",)]

        result1 = connected_db._get_primary_keys(mock_cur, "estabelecimentos")
        result2 = connected_db._get_primary_keys(mock_cur, "estabelecimentos")

        assert result1 == ["cnpj_basico", "cnpj_ordem"]
        assert result2 == ["cnpj_basico", "cnpj_ordem"]
        # SQL only called once — second call uses cache
        assert mock_cur.execute.call_count == 1


class TestCopyToTemp:
    """Test COPY to temp table."""

    def test_strips_null_bytes(self, connected_db):
        df = pl.DataFrame({"col": ["hello\x00world"]})
        mock_cur = MagicMock()

        connected_db._copy_to_temp(mock_cur, df, "temp_table", ["col"])

        copy_call = mock_cur.copy_expert.call_args
        csv_buffer = copy_call[0][1]
        content = csv_buffer.read()
        assert b"\x00" not in content
        assert b"helloworld" in content


class TestUpsertFromTemp:
    """Test SQL generation for upsert."""

    def test_generates_update_clause(self, connected_db):
        mock_cur = MagicMock()

        connected_db._upsert_from_temp(mock_cur, "temp_tbl", "cnaes", ["codigo", "descricao"], ["codigo"])

        sql = mock_cur.execute.call_args[0][0]
        assert 'ON CONFLICT ("codigo")' in sql
        assert '"descricao" = EXCLUDED."descricao"' in sql
        assert "data_atualizacao = CURRENT_TIMESTAMP" in sql

    def test_do_nothing_when_all_columns_are_pks(self, connected_db):
        mock_cur = MagicMock()

        connected_db._upsert_from_temp(mock_cur, "temp_tbl", "test", ["id"], ["id"])

        sql = mock_cur.execute.call_args[0][0]
        assert "DO NOTHING" in sql
        assert "DO UPDATE" not in sql
