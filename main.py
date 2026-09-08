#!/usr/bin/env python3
"""
CNPJ Data Pipeline - Download and process Brazilian company data from Receita Federal.

Usage:
    python main.py                    # Process latest month once
    python main.py --list             # List available months
    python main.py --month 2024-11    # Process specific month
    python main.py --month 2024-11 --force   # Force PostgreSQL re-processing
    docker compose up                 # Run once with Docker
"""

import argparse
import logging
import subprocess
import sys
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING

from tqdm import tqdm

from config import Config
from downloader import Downloader
from file_types import FILE_MAPPINGS, get_zip_file_type
from file_types import get_file_priority as get_file_priority
from file_types import group_files_by_dependency as group_files_by_dependency
from processor import process_file

if TYPE_CHECKING:
    from database import Database
    from parquet_writer import ParquetWriter


logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="CNPJ Data Pipeline - Download and process Brazilian company data")
    parser.add_argument("--list", "-l", action="store_true", help="List available months without processing")
    parser.add_argument("--month", "-m", type=str, help="Specific month to process (format: YYYY-MM, e.g., 2024-11)")
    parser.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="Force PostgreSQL re-processing even if already processed",
    )
    return parser.parse_args()


def pg_worker(
    zip_filename: str, directory: str, downloader: Downloader, cfg: Config, pre_truncated: set[str] | None = None
) -> None:
    """Worker: download, process, and load one file to PostgreSQL."""
    from database import Database

    db = Database(cfg.database_url, pre_truncated=pre_truncated, retry_attempts=cfg.retry_attempts)
    try:
        for csv_path in downloader.download_file(directory, zip_filename):
            load_postgres_csv(csv_path, zip_filename, directory, db, cfg)
    except Exception as e:
        logger.error(f"Error processing {zip_filename}: {e}")
        raise
    finally:
        db.disconnect()


def parquet_worker(
    zip_filename: str, directory: str, downloader: Downloader, parquet: "ParquetWriter", cfg: Config
) -> None:
    """Worker: download, process, and write one file to Parquet."""
    for csv_path in downloader.download_file(directory, zip_filename):
        try:
            write_parquet_csv(csv_path, parquet, cfg)
        except Exception as e:
            logger.error(f"Error: {csv_path.name}: {e}")
            raise


def load_postgres_csv(
    csv_path: Path,
    zip_filename: str,
    directory: str,
    db: "Database",
    cfg: Config,
    progress: Callable[[int], None] | None = None,
) -> None:
    rows = 0
    load = db.bulk_insert if cfg.loading_strategy == "replace" else db.bulk_upsert
    for batch, table_name, columns in process_file(csv_path, cfg.batch_size):
        load(batch, table_name, columns)
        rows += len(batch)
        if progress is not None:
            progress(rows)

    if rows == 0:
        raise ValueError(f"Empty source: {csv_path.name}")

    db.mark_processed(directory, zip_filename)
    logger.info(f"  {csv_path.name}: {rows:,} rows")

    if csv_path.exists() and not cfg.keep_files:
        csv_path.unlink()


def write_parquet_csv(csv_path: Path, parquet: "ParquetWriter", cfg: Config) -> None:
    rows = 0
    for batch, table_name, _columns in process_file(csv_path, cfg.batch_size, typed=cfg.parquet_typed_output):
        parquet.write_batch(batch, table_name)
        rows += len(batch)
        if rows % 1_000_000 == 0:
            logger.info(f"  {csv_path.name}: {rows:,} rows")

    if rows == 0:
        raise ValueError(f"Empty source: {csv_path.name}")

    logger.info(f"  {csv_path.name}: {rows:,} rows")

    if csv_path.exists() and not cfg.keep_files:
        csv_path.unlink()


def wait_for_workers(futures: dict[Future[None], str], failure_message: str) -> None:
    failed = False
    with tqdm(total=len(futures), desc="Processing", unit="file") as pbar:
        for future in as_completed(futures):
            pbar.set_postfix_str(futures[future][:30])
            try:
                future.result()
            except Exception:
                failed = True
            pbar.update(1)
    if failed:
        raise RuntimeError(failure_message)


def run_parquet(
    pending_files: list[str], directory: str, downloader: Downloader, parquet: "ParquetWriter", config: Config
) -> None:
    # Validate all existing tables before downloading or publishing any new ones.
    for filename in pending_files:
        file_type = get_zip_file_type(filename)
        if file_type and file_type in FILE_MAPPINGS:
            table = FILE_MAPPINGS[file_type]
            if table not in parquet.stats and (Path(config.parquet_output_dir) / f"{table}.parquet").exists():
                parquet.include_existing_table(table)

    file_groups = group_files_by_dependency(pending_files)
    workers = config.process_workers

    for group_files in file_groups:
        if not group_files:
            continue

        # Existing tables were validated before processing the first group.
        files_to_process: list[str] = []
        tables_in_group: set[str] = set()
        skipped_tables: set[str] = set()
        for f in group_files:
            ft = get_zip_file_type(f)
            if not ft or ft not in FILE_MAPPINGS:
                continue
            table_name = FILE_MAPPINGS[ft]
            parquet_path = Path(config.parquet_output_dir) / f"{table_name}.parquet"
            if parquet_path.exists():
                if table_name not in skipped_tables:
                    logger.info(f"Skipping {table_name} (already exported)")
                    skipped_tables.add(table_name)
                continue
            files_to_process.append(f)
            tables_in_group.add(table_name)

        if not files_to_process:
            continue

        logger.info(f"Processing {len(files_to_process)} files ({', '.join(sorted(tables_in_group))})...")

        if workers > 1:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(parquet_worker, f, directory, downloader, parquet, config): f
                    for f in files_to_process
                }
                wait_for_workers(futures, "One or more workers failed, aborting to prevent incomplete export")
        else:
            for zip_filename in files_to_process:
                for csv_path, _ in downloader.download_files(directory, [zip_filename]):
                    try:
                        write_parquet_csv(csv_path, parquet, config)

                    except Exception as e:
                        logger.error(f"Error: {csv_path.name}: {e}")
                        raise

        # Flush tables in this group and run post-file commands
        for table_name in tables_in_group:
            parquet_path = parquet.flush_table(table_name)
            if parquet_path:
                logger.info(f"  {table_name}: flushed → {parquet_path.name}")
                if config.post_file_command:
                    logger.info(f"  Running post-file command for {parquet_path.name}")
                    subprocess.run(
                        [*config.post_file_command.split(), str(parquet_path)],
                        check=True,
                    )


def run_postgres(
    pending_files: list[str], directory: str, downloader: Downloader, db: "Database", config: Config
) -> None:
    resumed_tables: set[str] = set()
    if config.loading_strategy == "replace":
        resumed_tables = {
            FILE_MAPPINGS[ft]
            for filename in db.get_processed_files(directory)
            if (ft := get_zip_file_type(filename)) and ft in FILE_MAPPINGS
        }
        db.preserve_tables(resumed_tables)

    # Database mode: process files by dependency group
    file_groups = group_files_by_dependency(pending_files)
    workers = config.process_workers

    for group_files in file_groups:
        if not group_files:
            continue

        if workers > 1:
            # Pre-truncate for replace strategy before spawning workers
            pre_truncated: set[str] = set()
            if config.loading_strategy == "replace":
                pre_truncated = {
                    FILE_MAPPINGS[ft] for f in group_files if (ft := get_zip_file_type(f)) and ft in FILE_MAPPINGS
                }
                for table in pre_truncated - resumed_tables:
                    db.truncate_table(table)

            logger.info(f"Processing {len(group_files)} files with {workers} workers...")
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(pg_worker, f, directory, downloader, config, pre_truncated): f for f in group_files
                }
                wait_for_workers(futures, "One or more workers failed, aborting to prevent data corruption")
        else:
            # Sequential: download in parallel, process one at a time
            file_iterator = downloader.download_files(directory, group_files)
            with tqdm(file_iterator, total=len(group_files), desc="Processing", unit="file") as pbar:
                for csv_path, zip_filename in pbar:
                    pbar.set_postfix_str(csv_path.name[:30])
                    try:
                        load_postgres_csv(
                            csv_path,
                            zip_filename,
                            directory,
                            db,
                            config,
                            lambda rows: pbar.set_postfix_str(f"{csv_path.name[:20]} {rows:,} rows"),
                        )

                    except Exception as e:
                        logger.error(f"Error: {csv_path.name}: {e}")
                        raise


def main(cfg: Config | None = None) -> None:
    """Main pipeline entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    args = parse_args()
    config = cfg if cfg is not None else Config.from_env()

    downloader = Downloader(config)

    # Handle --list mode
    if args.list:
        available = downloader.get_available_directories()
        print("Available months:")
        for month in available:
            print(f"  {month}")
        return

    is_parquet = config.output_format == "parquet"

    if not is_parquet and not config.database_url:
        logger.error("DATABASE_URL not set (required for postgres output)")
        sys.exit(1)

    db = None
    parquet = None

    if not is_parquet:
        from database import Database

        db = Database(config.database_url, retry_attempts=config.retry_attempts)
        db.ensure_schema()

    try:
        if args.month:
            available = downloader.get_available_directories()
            if args.month not in available:
                logger.error(f"Month {args.month} not available. Use --list to see options.")
                sys.exit(1)
            directory = args.month
        else:
            directory = downloader.get_latest_directory()

        if is_parquet:
            from parquet_writer import ParquetWriter

            parquet = ParquetWriter(
                config.parquet_output_dir, source_month=directory, typed=config.parquet_typed_output
            )
            logger.info(f"Parquet mode: output to {config.parquet_output_dir}")

        # Handle --force mode (database only)
        if args.force and db:
            logger.info(f"Force mode: clearing processed files for {directory}")
            db.clear_processed_files(directory)

        all_files = downloader.get_directory_files(directory)

        if db:
            processed = db.get_processed_files(directory)
            pending_files = [f for f in all_files if f not in processed]
        else:
            pending_files = list(all_files)

        if not pending_files:
            logger.info("All files already processed!")
            return

        logger.info(f"Processing {len(pending_files)} files from {directory}")

        pending_files.sort(key=get_file_priority)

        if parquet is not None:
            run_parquet(pending_files, directory, downloader, parquet, config)
        else:
            assert db is not None
            run_postgres(pending_files, directory, downloader, db, config)

        if is_parquet:
            assert parquet is not None
            parquet.close()
            manifest = parquet.write_manifest(source_month=directory)
            total_rows = manifest["totals"]["rows"]
            total_size = manifest["totals"]["sizeBytes"] / 1024 / 1024 / 1024
            logger.info(f"Parquet export complete: {total_rows:,} rows, {total_size:.2f} GB")

        logger.info("Done!")

    except Exception as e:
        logger.error(f"Failed: {e}")
        sys.exit(1)

    finally:
        if parquet is not None:
            parquet.abort()
        if db:
            db.disconnect()
        downloader.cleanup()


if __name__ == "__main__":
    main()
