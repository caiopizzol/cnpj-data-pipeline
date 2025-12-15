#!/usr/bin/env python3
"""
CNPJ Data Pipeline - Download and process Brazilian company data from Receita Federal.

Usage:
    python main.py           # Run full pipeline
    docker compose up        # Run with Docker
"""

import logging
import sys

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


def main():
    """Main pipeline entry point."""
    if not config.database_url:
        logger.error("DATABASE_URL not set")
        sys.exit(1)

    db = Database(config.database_url)
    downloader = Downloader(config)

    try:
        directory = downloader.get_latest_directory()
        all_files = downloader.get_directory_files(directory)
        processed = db.get_processed_files(directory)
        pending_files = [f for f in all_files if f not in processed]

        if not pending_files:
            print("All files already processed!")
            return

        print(f"Processing {len(pending_files)} files from {directory}")

        # 4. Sort files by processing order
        pending_files.sort(key=get_file_priority)

        # 5. Download and process files
        for csv_path, zip_filename in downloader.download_files(directory, pending_files):
            try:
                # Process CSV in batches
                for batch, table_name, columns in process_file(csv_path, config.batch_size):
                    db.bulk_upsert(batch, table_name, columns)

                # Mark as processed
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
