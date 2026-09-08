"""Measure one CSV sample in a fresh PostgreSQL database, then verify and remove it.

Run from the repo root: uv run --frozen python -m scripts.benchmark_postgres --help.
Use a fresh process and an explicit BENCHMARK_DATABASE_URL for a disposable server.
"""

import argparse
import csv
import json
import os
import platform
import resource
import sys
import tomllib
from collections.abc import Generator
from contextlib import closing, contextmanager
from importlib.metadata import version
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import polars as pl
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import make_dsn

from database import Database
from file_types import get_file_type
from processor import process_file
from scripts.benchmark_pipeline import sha256


@contextmanager
def temporary_database(admin_url: str) -> Generator[Database, None, None]:
    name = f"cnpj_benchmark_{uuid4().hex}"
    with closing(psycopg2.connect(admin_url)) as admin:
        admin.autocommit = True
        with admin.cursor() as cur:
            cur.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(sql.Identifier(name)))
        db = Database(make_dsn(admin_url, dbname=name))
        try:
            db.ensure_schema()
            yield db
        finally:
            db.disconnect()
            with admin.cursor() as cur:
                cur.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(name)))


def verify(db: Database, source: Path, table: str, columns: list[str], batch_size: int) -> int:
    """Compare stored values with normalized input using a separate SQL transport."""
    names = sql.SQL(", ").join(map(sql.Identifier, columns))
    with db.connect().cursor() as cur:
        cur.execute(sql.SQL("CREATE TEMP TABLE expected (LIKE {} INCLUDING DEFAULTS)").format(sql.Identifier(table)))
        insert = sql.SQL("INSERT INTO expected ({}) VALUES ({})").format(
            names, sql.SQL(", ").join(sql.Placeholder() for _ in columns)
        )
        with closing(process_file(source, batch_size=batch_size)) as batches:
            for batch, _table, _columns in batches:
                cur.executemany(insert, batch.select(columns).rows())
        cur.execute(
            sql.SQL(
                "SELECT EXISTS ((SELECT {cols} FROM expected EXCEPT SELECT {cols} FROM {table}) "
                "UNION ALL (SELECT {cols} FROM {table} EXCEPT SELECT {cols} FROM expected))"
            ).format(cols=names, table=sql.Identifier(table))
        )
        difference = cur.fetchone()
        assert difference is not None
        if difference[0]:
            raise ValueError("Stored values differ from the sample (conflicting duplicate keys are unsupported)")
        cur.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(table)))
        count = cur.fetchone()
        assert count is not None
        return int(count[0])


def measure(source: Path, output: Path, batch_size: int, strategy: str, admin_url: str) -> dict[str, object]:
    if not admin_url:
        raise ValueError("Set BENCHMARK_DATABASE_URL to a disposable PostgreSQL server")
    if batch_size < 1:
        raise ValueError("Batch size must be positive")
    if strategy not in ("upsert", "replace"):
        raise ValueError("Strategy must be upsert or replace")
    if get_file_type(source.name) is None:
        raise ValueError("Unknown source file type; keep the Receita filename suffix")
    input_hash = sha256(source)
    with source.open(encoding="iso-8859-1", newline="") as file:
        expected_rows = sum(1 for _ in csv.reader(file, delimiter=";"))
    if expected_rows == 0:
        raise ValueError("Source has no rows")
    output.mkdir(parents=True, exist_ok=False)
    with temporary_database(admin_url) as db:
        with db.connect().cursor() as cur:
            cur.execute(
                "SELECT name, setting, unit FROM pg_settings WHERE name IN "
                "('server_version', 'shared_buffers', 'work_mem', 'fsync', 'synchronous_commit', "
                "'full_page_writes', 'max_wal_size', 'checkpoint_timeout') ORDER BY name"
            )
            settings = {name: {"value": value, "unit": unit} for name, value, unit in cur.fetchall()}
        db.connect().commit()
        load = db.bulk_upsert if strategy == "upsert" else db.bulk_insert
        processing = writing = 0.0
        rows = batch_count = 0
        table = ""
        columns: list[str] = []
        cpu_before = resource.getrusage(resource.RUSAGE_SELF)
        started = perf_counter()
        with closing(process_file(source, batch_size=batch_size)) as batches:
            while True:
                stage = perf_counter()
                item = next(batches, None)
                processing += perf_counter() - stage
                if item is None:
                    break
                batch, table, columns = item
                stage = perf_counter()
                load(batch, table, columns)
                writing += perf_counter() - stage
                rows += batch.height
                batch_count += 1
        elapsed = perf_counter() - started
        usage = resource.getrusage(resource.RUSAGE_SELF)
        if rows != expected_rows:
            raise ValueError(f"Source has {expected_rows} rows, processed {rows}")
        if sha256(source) != input_hash:
            raise ValueError("Source changed during measurement")
        stored_rows = verify(db, source, table, columns, batch_size)
        if sha256(source) != input_hash:
            raise ValueError("Source changed during measurement")
        result: dict[str, object] = {
            "input": str(source),
            "input_sha256": input_hash,
            "input_bytes": source.stat().st_size,
            "rows": rows,
            "stored_rows": stored_rows,
            "duplicate_rows": rows - stored_rows,
            "table": table,
            "strategy": strategy,
            "batch_size": batch_size,
            "batches": batch_count,
            "processing_seconds": processing,
            "writing_seconds": writing,
            "total_seconds": elapsed,
            "rows_per_second": rows / elapsed,
            "cpu_seconds": usage.ru_utime + usage.ru_stime - cpu_before.ru_utime - cpu_before.ru_stime,
            "peak_client_rss_bytes_before_verification": int(
                usage.ru_maxrss if sys.platform == "darwin" else usage.ru_maxrss * 1024
            ),
            "postgres_settings": settings,
            "environment": {
                "platform": platform.platform(),
                "python": platform.python_version(),
                "polars": pl.__version__,
                "psycopg2": version("psycopg2-binary"),
                "pipeline_version": tomllib.loads((Path(__file__).resolve().parents[1] / "pyproject.toml").read_text())[
                    "project"
                ]["version"],
                "cpu_count": os.cpu_count(),
                "polars_threads": pl.thread_pool_size(),
            },
        }
    (output / "measurement.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--batch-size", type=int, default=500_000)
    parser.add_argument("--strategy", choices=("upsert", "replace"), default="upsert")
    args = parser.parse_args()
    result = measure(
        args.source, args.output, args.batch_size, args.strategy, os.environ.get("BENCHMARK_DATABASE_URL", "")
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
