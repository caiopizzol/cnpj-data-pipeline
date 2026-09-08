"""PostgreSQL database operations with Polars for fast bulk loading."""

import io
import logging
import os
import time
from pathlib import Path

import polars as pl
import psycopg2
from psycopg2.extensions import connection, cursor

logger = logging.getLogger(__name__)


class Database:
    """PostgreSQL database handler with temp table upsert."""

    def __init__(self, database_url: str, pre_truncated: set[str] | None = None, retry_attempts: int = 3):
        self.database_url = database_url
        self.retry_attempts = retry_attempts
        self._pk_cache: dict[str, list[str]] = {}
        self._truncated_tables: set[str] = set(pre_truncated) if pre_truncated else set()
        self.conn: connection | None = None

    def connect(self) -> connection:
        """Establish database connection with retry.

        Passes DATABASE_URL through to libpq verbatim so query-string
        parameters (sslmode, options, application_name, connect_timeout,
        multi-host URIs, etc.) reach the driver.
        """
        if self.conn is not None:
            return self.conn

        for attempt in range(self.retry_attempts):
            try:
                self.conn = psycopg2.connect(self.database_url)
                self.conn.autocommit = False
                return self.conn
            except psycopg2.OperationalError:
                if attempt == self.retry_attempts - 1:
                    raise
                time.sleep(2**attempt)

        raise ValueError("retry_attempts must be positive")

    def disconnect(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def ensure_schema(self):
        """Apply initial.sql if the processed_files sentinel table is absent.

        Lets the published Docker image target a fresh managed Postgres
        (Railway, RDS, etc.) without a separate init step.
        """
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('processed_files')")
                row = cur.fetchone()
                if row is None:
                    raise RuntimeError("Schema query returned no row")
                if row[0] is not None:
                    return

                sql_path = Path(__file__).parent / "initial.sql"
                cur.execute(sql_path.read_text())
                conn.commit()
                logger.info("Applied schema from initial.sql")
        except Exception:
            conn.rollback()
            raise

    def get_processed_files(self, directory: str) -> set[str]:
        """Get all processed filenames for a directory."""
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT filename FROM processed_files WHERE directory = %s",
                    (directory,),
                )
                return {row[0] for row in cur.fetchall()}
        except Exception as e:
            logger.error(f"Failed to get processed files: {e}")
            raise

    def mark_processed(self, directory: str, filename: str):
        """Mark a file as processed."""
        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO processed_files (directory, filename)
                   VALUES (%s, %s)
                   ON CONFLICT (directory, filename) DO NOTHING""",
                (directory, filename),
            )
            conn.commit()

    def clear_processed_files(self, directory: str):
        """Clear all processed file records for a directory (for force re-processing)."""
        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM processed_files WHERE directory = %s",
                (directory,),
            )
            conn.commit()

    def truncate_table(self, table_name: str):
        """Truncate a table. Used before parallel processing with replace strategy."""
        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {table_name} CASCADE")
            conn.commit()
        self._truncated_tables.add(table_name)

    def preserve_tables(self, tables: set[str]) -> None:
        """Keep completed shards when resuming a replace load."""
        self._truncated_tables.update(tables)

    def bulk_upsert(self, df: pl.DataFrame, table_name: str, columns: list[str]):
        """Bulk upsert using temp table + COPY."""
        if df.is_empty():
            return

        conn = self.connect()
        temp_table = f"temp_{table_name}_{id(df)}"

        try:
            with conn.cursor() as cur:
                # 1. Create temp table
                cur.execute(
                    f"CREATE TEMP TABLE {temp_table} "
                    f"(LIKE {table_name} INCLUDING DEFAULTS INCLUDING STORAGE) ON COMMIT DROP"
                )

                # 2. COPY to temp
                self._copy_dataframe(cur, df, temp_table, columns)

                # 3. Upsert from temp to main
                primary_keys = self._get_primary_keys(cur, table_name)
                self._upsert_from_temp(cur, temp_table, table_name, columns, primary_keys)

                conn.commit()

        except Exception as e:
            conn.rollback()
            logger.error(f"Error: {table_name}: {e}")
            raise

    def bulk_insert(self, df: pl.DataFrame, table_name: str, columns: list[str]):
        """Bulk insert under LOADING_STRATEGY=replace.

        First batch per table, unless pre-truncated: TRUNCATE then COPY
        directly into the target. This requires unique keys within that batch.

        Subsequent batches and pre-truncated workers: COPY into a temp
        table and merge via the existing upsert helper. RFB occasionally ships
        the same (cnpj_basico, cnpj_ordem, cnpj_dv) across two sharded ZIPs of the
        same source table (e.g. Estabelecimentos0.zip and Estabelecimentos5.zip);
        without the temp-table path the second batch's direct COPY crashes
        on the PK constraint.
        """
        if df.is_empty():
            return

        conn = self.connect()

        try:
            with conn.cursor() as cur:
                first_batch = table_name not in self._truncated_tables
                if first_batch:
                    cur.execute(f"TRUNCATE TABLE {table_name} CASCADE")
                    self._truncated_tables.add(table_name)
                    logger.info(f"Truncated {table_name}")
                    # No existing rows can conflict; this batch must have unique keys.
                    self._copy_dataframe(cur, df, table_name, columns)
                else:
                    # Cross-batch PK overlap path. Same temp-then-upsert
                    # pattern as bulk_upsert.
                    temp_table = f"{table_name}_tmp_{os.getpid()}"
                    cur.execute(
                        f"CREATE TEMP TABLE IF NOT EXISTS {temp_table} "
                        f"(LIKE {table_name} INCLUDING DEFAULTS INCLUDING STORAGE) ON COMMIT DROP"
                    )
                    cur.execute(f"TRUNCATE {temp_table}")
                    self._copy_dataframe(cur, df, temp_table, columns)
                    primary_keys = self._get_primary_keys(cur, table_name)
                    self._upsert_from_temp(cur, temp_table, table_name, columns, primary_keys)

                conn.commit()

        except Exception as e:
            conn.rollback()
            logger.error(f"Error: {table_name}: {e}")
            raise

    def _copy_dataframe(self, cur: cursor, df: pl.DataFrame, table_name: str, columns: list[str]):
        """COPY a DataFrame to the supplied destination table using Polars CSV."""
        columns_str = ", ".join([f'"{col}"' for col in columns])
        csv_bytes = df.write_csv(include_header=False).encode("utf-8", errors="replace")
        csv_bytes = csv_bytes.replace(b"\x00", b"")

        cur.copy_expert(
            f"COPY {table_name} ({columns_str}) FROM STDIN WITH CSV ENCODING 'UTF8'",
            io.BytesIO(csv_bytes),
        )

    def _get_primary_keys(self, cur: cursor, table_name: str) -> list[str]:
        """Get primary key columns for a table with caching."""
        if table_name in self._pk_cache:
            return self._pk_cache[table_name]

        cur.execute(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary
            ORDER BY array_position(i.indkey, a.attnum)
            """,
            (table_name,),
        )

        primary_keys = [row[0] for row in cur.fetchall()]
        self._pk_cache[table_name] = primary_keys
        return primary_keys

    def _upsert_from_temp(
        self, cur: cursor, temp_table: str, target_table: str, columns: list[str], primary_keys: list[str]
    ):
        """Upsert from temp to target table."""
        columns_str = ", ".join([f'"{col}"' for col in columns])
        pk_str = ", ".join([f'"{pk}"' for pk in primary_keys])

        update_cols = [c for c in columns if c not in primary_keys]
        update_clause = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
        if update_clause:
            update_clause += ", data_atualizacao = CURRENT_TIMESTAMP"

        sql = f"""
            INSERT INTO {target_table} ({columns_str})
            SELECT DISTINCT ON ({pk_str}) {columns_str} FROM {temp_table} ORDER BY {pk_str}
            ON CONFLICT ({pk_str}) {"DO UPDATE SET " + update_clause if update_clause else "DO NOTHING"}
        """
        cur.execute(sql)
