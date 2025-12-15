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
import sys

from tqdm import tqdm

from config import config
from database import Database
from downloader import Downloader
from processor import process_file, get_file_type

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Processing order (respects foreign key dependencies)
PROCESSING_ORDER = [
    "CNAECSV",      # cnaes
    "MOTICSV",      # motivos
    "MUNICCSV",     # municipios
    "NATJUCSV",     # naturezas_juridicas
    "PAISCSV",      # paises
    "QUALSCSV",     # qualificacoes_socios
    "EMPRECSV",     # empresas
    "ESTABELE",     # estabelecimentos
    "SOCIOCSV",     # socios
    "SIMPLESCSV",   # dados_simples
]


def get_file_priority(filename: str) -> int:
    """Get processing priority for a file (lower = first)."""
    file_type = get_file_type(filename)
    if file_type in PROCESSING_ORDER:
        return PROCESSING_ORDER.index(file_type)
    return 999


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="CNPJ Data Pipeline - Download and process Brazilian company data"
    )
    parser.add_argument(
        "--list", "-l",
        action="store_true",
        help="List available months without processing"
    )
    parser.add_argument(
        "--month", "-m",
        type=str,
        help="Specific month to process (format: YYYY-MM, e.g., 2024-11)"
    )
    parser.add_argument(
        "--force", "-f",
        action="store_true",
        help="Force re-processing even if already processed"
    )
    return parser.parse_args()


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

    if not config.database_url:
        logger.error("DATABASE_URL not set")
        sys.exit(1)

    db = Database(config.database_url)

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

        # Handle --force mode
        if args.force:
            print(f"Force mode: clearing processed files for {directory}")
            db.clear_processed_files(directory)

        all_files = downloader.get_directory_files(directory)
        processed = db.get_processed_files(directory)
        pending_files = [f for f in all_files if f not in processed]

        if not pending_files:
            print("All files already processed!")
            return

        print(f"Processing {len(pending_files)} files from {directory}")

        # Sort files by processing order
        pending_files.sort(key=get_file_priority)

        # Download and process files
        file_iterator = downloader.download_files(directory, pending_files)
        with tqdm(file_iterator, total=len(pending_files), desc="Processing", unit="file") as pbar:
            for csv_path, zip_filename in pbar:
                pbar.set_postfix_str(csv_path.name[:30])
                try:
                    rows = 0
                    for batch, table_name, columns in process_file(csv_path, config.batch_size):
                        db.bulk_upsert(batch, table_name, columns)
                        rows += len(batch)
                        pbar.set_postfix_str(f"{csv_path.name[:20]} {rows:,} rows")

                    db.mark_processed(directory, zip_filename)

                except Exception as e:
                    logger.error(f"Error: {csv_path.name}: {e}")

                finally:
                    if csv_path.exists() and not config.keep_files:
                        csv_path.unlink()

        print("Done!")

    except Exception as e:
        logger.error(f"Failed: {e}")
        sys.exit(1)

    finally:
        db.disconnect()
        downloader.cleanup()


if __name__ == "__main__":
    main()
