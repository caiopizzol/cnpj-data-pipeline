#!/usr/bin/env python3
"""
CNPJ Data Pipeline - Download and process Brazilian company data from Receita Federal.

Usage:
    python main.py                    # Process latest month
    python main.py --list             # List available months
    python main.py --month 2024-11    # Process specific month
    python main.py --month 2024-11 --force   # Force re-process
    docker compose up                 # Run with Docker
"""

import argparse
import logging
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from tqdm import tqdm

from config import config
from downloader import Downloader
from processor import FILE_MAPPINGS, get_file_type, process_file

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Dependency groups — files within the same group have no inter-dependencies
# and can be processed in parallel. Groups must be processed in order.
DEPENDENCY_GROUPS = [
    ["CNAECSV", "MOTICSV", "MUNICCSV", "NATJUCSV", "PAISCSV", "QUALSCSV"],  # references
    ["EMPRECSV"],  # empresas
    ["ESTABELE", "SOCIOCSV", "SIMPLESCSV"],  # depends on empresas
]

# Flat processing order derived from dependency groups (for sorting)
PROCESSING_ORDER = [ft for group in DEPENDENCY_GROUPS for ft in group]

# ZIP filename prefix → file type (zip names differ from CSV names inside)
ZIP_PREFIX_MAP = [
    ("SIMPLES", "SIMPLESCSV"),
    ("CNAE", "CNAECSV"),
    ("MOTI", "MOTICSV"),
    ("MUNIC", "MUNICCSV"),
    ("NATUR", "NATJUCSV"),
    ("PAIS", "PAISCSV"),
    ("QUALIFICAC", "QUALSCSV"),
    ("EMPRES", "EMPRECSV"),
    ("ESTABELE", "ESTABELE"),
    ("SOCIO", "SOCIOCSV"),
]


def get_zip_file_type(zip_filename: str) -> str | None:
    """Determine file type from ZIP filename."""
    name = zip_filename.upper()
    for prefix, file_type in ZIP_PREFIX_MAP:
        if name.startswith(prefix):
            return file_type
    return None


def get_file_priority(filename: str) -> int:
    """Get processing priority for a file (lower = first)."""
    file_type = get_zip_file_type(filename) or get_file_type(filename)
    if file_type in PROCESSING_ORDER:
        return PROCESSING_ORDER.index(file_type)
    return 999


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="CNPJ Data Pipeline - Download and process Brazilian company data")
    parser.add_argument("--list", "-l", action="store_true", help="List available months without processing")
    parser.add_argument("--month", "-m", type=str, help="Specific month to process (format: YYYY-MM, e.g., 2024-11)")
    parser.add_argument("--force", "-f", action="store_true", help="Force re-processing even if already processed")
    return parser.parse_args()


def group_files_by_dependency(files: list[str]) -> list[list[str]]:
    """Group pending files by dependency level. Returns ordered list of groups."""
    groups: list[list[str]] = [[] for _ in DEPENDENCY_GROUPS]
    for f in files:
        file_type = get_zip_file_type(f)
        if not file_type:
            continue
        for i, dep_types in enumerate(DEPENDENCY_GROUPS):
            if file_type in dep_types:
                groups[i].append(f)
                break
    return groups


def _pg_worker(zip_filename, directory, downloader, cfg, pre_truncated=None):
    """Worker: download, process, and load one file to PostgreSQL."""
    from database import Database

    db = Database(
        cfg.database_url, pre_truncated=pre_truncated, retry_attempts=cfg.retry_attempts, retry_delay=cfg.retry_delay
    )
    try:
        for csv_path in downloader.download_file(directory, zip_filename):
            rows = 0
            load = db.bulk_insert if cfg.loading_strategy == "replace" else db.bulk_upsert
            for batch, table_name, columns in process_file(csv_path, cfg.batch_size):
                load(batch, table_name, columns)
                rows += len(batch)

            db.mark_processed(directory, zip_filename)
            logger.info(f"  {csv_path.name}: {rows:,} rows")

            if csv_path.exists() and not cfg.keep_files:
                csv_path.unlink()
    except Exception as e:
        logger.error(f"Error processing {zip_filename}: {e}")
        raise
    finally:
        db.disconnect()


def _parquet_worker(zip_filename, directory, downloader, parquet, cfg):
    """Worker: download, process, and write one file to Parquet."""
    for csv_path in downloader.download_file(directory, zip_filename):
        try:
            rows = 0
            for batch, table_name, columns in process_file(csv_path, cfg.batch_size):
                parquet.write_batch(batch, table_name, columns)
                rows += len(batch)

            logger.info(f"  {csv_path.name}: {rows:,} rows")

            if csv_path.exists() and not cfg.keep_files:
                csv_path.unlink()
        except Exception as e:
            logger.error(f"Error: {csv_path.name}: {e}")
            raise


def main():
    """Main pipeline entry point."""
    args = parse_args()

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

    if is_parquet:
        from parquet_writer import ParquetWriter

        parquet = ParquetWriter(config.parquet_output_dir)
        logger.info(f"Parquet mode: output to {config.parquet_output_dir}")
    else:
        from database import Database

        db = Database(config.database_url, retry_attempts=config.retry_attempts, retry_delay=config.retry_delay)

    try:
        # Select directory
        if args.month:
            available = downloader.get_available_directories()
            if args.month not in available:
                logger.error(f"Month {args.month} not available. Use --list to see options.")
                sys.exit(1)
            directory = args.month
        else:
            directory = downloader.get_latest_directory()

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

        # Sort files by processing order
        pending_files.sort(key=get_file_priority)

        if is_parquet:
            file_groups = group_files_by_dependency(pending_files)
            workers = config.process_workers

            for group_files in file_groups:
                if not group_files:
                    continue

                # Filter out files whose tables are already exported (resume)
                files_to_process = []
                tables_in_group = set()
                skipped_tables = set()
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
                    failed = False
                    with ThreadPoolExecutor(max_workers=workers) as executor:
                        futures = {
                            executor.submit(_parquet_worker, f, directory, downloader, parquet, config): f
                            for f in files_to_process
                        }
                        with tqdm(total=len(futures), desc="Processing", unit="file") as pbar:
                            for future in as_completed(futures):
                                filename = futures[future]
                                pbar.set_postfix_str(filename[:30])
                                try:
                                    future.result()
                                except Exception:
                                    failed = True
                                pbar.update(1)
                    if failed:
                        raise RuntimeError("One or more workers failed, aborting to prevent incomplete export")
                else:
                    for zip_filename in files_to_process:
                        for csv_path, _ in downloader.download_files(directory, [zip_filename]):
                            try:
                                rows = 0
                                for batch, tname, columns in process_file(csv_path, config.batch_size):
                                    parquet.write_batch(batch, tname, columns)
                                    rows += len(batch)
                                    if rows % 1_000_000 == 0:
                                        logger.info(f"  {csv_path.name}: {rows:,} rows")

                                logger.info(f"  {csv_path.name}: {rows:,} rows total")

                                if csv_path.exists() and not config.keep_files:
                                    csv_path.unlink()

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

        else:
            # Database mode: process files by dependency group
            file_groups = group_files_by_dependency(pending_files)
            workers = config.process_workers

            for group_files in file_groups:
                if not group_files:
                    continue

                if workers > 1:
                    # Pre-truncate for replace strategy before spawning workers
                    pre_truncated = set()
                    if config.loading_strategy == "replace":
                        pre_truncated = {
                            FILE_MAPPINGS[ft]
                            for f in group_files
                            if (ft := get_zip_file_type(f)) and ft in FILE_MAPPINGS
                        }
                        for table in pre_truncated:
                            db.truncate_table(table)

                    logger.info(f"Processing {len(group_files)} files with {workers} workers...")
                    failed = False
                    with ThreadPoolExecutor(max_workers=workers) as executor:
                        futures = {
                            executor.submit(_pg_worker, f, directory, downloader, config, pre_truncated): f
                            for f in group_files
                        }
                        with tqdm(total=len(futures), desc="Processing", unit="file") as pbar:
                            for future in as_completed(futures):
                                filename = futures[future]
                                pbar.set_postfix_str(filename[:30])
                                try:
                                    future.result()
                                except Exception:
                                    failed = True
                                pbar.update(1)
                    if failed:
                        raise RuntimeError("One or more workers failed, aborting to prevent data corruption")
                else:
                    # Sequential: download in parallel, process one at a time
                    file_iterator = downloader.download_files(directory, group_files)
                    with tqdm(file_iterator, total=len(group_files), desc="Processing", unit="file") as pbar:
                        for csv_path, zip_filename in pbar:
                            pbar.set_postfix_str(csv_path.name[:30])
                            try:
                                rows = 0
                                load = db.bulk_insert if config.loading_strategy == "replace" else db.bulk_upsert
                                for batch, table_name, columns in process_file(csv_path, config.batch_size):
                                    load(batch, table_name, columns)
                                    rows += len(batch)
                                    pbar.set_postfix_str(f"{csv_path.name[:20]} {rows:,} rows")

                                db.mark_processed(directory, zip_filename)

                                if csv_path.exists() and not config.keep_files:
                                    csv_path.unlink()

                            except Exception as e:
                                logger.error(f"Error: {csv_path.name}: {e}")
                                raise

        if is_parquet:
            parquet.close()
            manifest = parquet.write_manifest()
            total_rows = manifest["totals"]["rows"]
            total_size = manifest["totals"]["sizeBytes"] / 1024 / 1024 / 1024
            logger.info(f"Parquet export complete: {total_rows:,} rows, {total_size:.2f} GB")

        logger.info("Done!")

    except Exception as e:
        logger.error(f"Failed: {e}")
        sys.exit(1)

    finally:
        if db:
            db.disconnect()
        downloader.cleanup()


if __name__ == "__main__":
    main()
