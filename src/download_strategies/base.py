"""
Base download strategy for CNPJ data pipeline.

Defines the interface that all download strategies must implement.
"""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Iterator, List

from tqdm import tqdm

logger = logging.getLogger(__name__)


class DownloadStrategy(ABC):
    """
    Abstract base class for download strategies.

    Implements the Strategy pattern to allow different download approaches
    (sequential, parallel, etc.) while maintaining the same interface.
    """

    def __init__(self, config):
        """
        Initialize the download strategy.

        Args:
            config: Configuration object with download settings
        """
        self.config = config
        self.temp_path = Path(config.temp_dir)
        self.temp_path.mkdir(exist_ok=True)
        self.stats = {
            "files_downloaded": 0,
            "total_bytes": 0,
            "start_time": None,
            "end_time": None,
            "errors": [],
        }

    @abstractmethod
    def download_files(self, directory: str, files: List[str]) -> Iterator[Path]:
        """
        Download and extract multiple files.

        Args:
            directory: Directory path containing the files
            files: List of filenames to download

        Yields:
            Path: Extracted CSV file paths as they become available
        """
        pass

    def download_single_file(self, directory: str, filename: str) -> List[Path]:
        """
        Download and extract a single file.

        This is the core download logic that will be shared between strategies.

        Args:
            directory: Directory path containing the file
            filename: Filename to download

        Returns:
            List of extracted CSV file paths
        """
        import time
        import zipfile

        import requests

        # Check for existing extracted files if keeping downloads
        if self.config.keep_downloaded_files:
            existing_files = self._check_existing_csv_files(filename)
            if existing_files:
                logger.info(
                    f"Found existing CSV files for {filename}, skipping download"
                )
                return existing_files

        url = f"{self.config.base_url}/{directory}/{filename}"
        zip_path = self.temp_path / filename

        # Download with retries
        for attempt in range(self.config.retry_attempts):
            try:
                logger.debug(f"Downloading {filename} (attempt {attempt + 1})")

                response = requests.get(
                    url,
                    stream=True,
                    timeout=(self.config.connect_timeout, self.config.read_timeout),
                )
                response.raise_for_status()

                # Get content length for progress bar
                total_size = int(response.headers.get("content-length", 0))

                # Create progress bar for this file download
                with tqdm(
                    total=total_size,
                    unit="B",
                    unit_scale=True,
                    desc=f"Downloading {filename}",
                    leave=False,  # Don't leave progress bar after completion
                ) as pbar:
                    bytes_downloaded = 0
                    with open(zip_path, "wb") as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                            bytes_downloaded += len(chunk)
                            pbar.update(len(chunk))

                logger.debug(f"Downloaded {filename} ({bytes_downloaded} bytes)")
                self.stats["files_downloaded"] += 1
                self.stats["total_bytes"] += bytes_downloaded
                break

            except Exception as e:
                error_msg = f"Download attempt {attempt + 1} failed for {filename}: {e}"
                logger.warning(error_msg)
                self.stats["errors"].append(error_msg)

                if attempt < self.config.retry_attempts - 1:
                    time.sleep(self.config.retry_delay)
                else:
                    raise

        # Extract files
        extracted_files = []
        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                # Known CNPJ file patterns
                known_patterns = [
                    "CNAECSV",
                    "MOTICSV",
                    "MUNICCSV",
                    "NATJUCSV",
                    "PAISCSV",
                    "QUALSCSV",
                    "EMPRECSV",
                    "ESTABELE",
                    "SOCIOCSV",
                    "SIMPLESCSV",
                ]

            for member in zip_ref.namelist():
                # Remove pontos do nome do arquivo para facilitar a busca
                member_upper = member.upper().replace(".", "")
                is_cnpj_file = any(
                    pattern in member_upper for pattern in known_patterns
                )

                if is_cnpj_file:
                    extract_path = self.temp_path / member
                    zip_ref.extract(member, self.temp_path)
                    extracted_files.append(extract_path)
                    logger.debug(f"Extracted CNPJ file: {member}")

            logger.debug(f"Extracted {len(extracted_files)} CSV files from {filename}")

        except Exception as e:
            logger.error(f"Error extracting {filename}: {e}")
            raise
        finally:
            # Only cleanup zip file if not keeping downloaded files
            if zip_path.exists() and not self.config.keep_downloaded_files:
                zip_path.unlink()

        return extracted_files

    def _check_existing_csv_files(self, zip_filename: str) -> List[Path]:
        """
        Check if CSV files from this ZIP already exist in temp directory.

        Args:
            zip_filename: Name of the ZIP file (e.g., "Empresas0.zip")

        Returns:
            List of existing CSV file paths, empty if none found
        """
        import re

        # Map ZIP filenames to their expected CSV patterns
        zip_to_csv_patterns = {
            # Reference tables (single files)
            "CNAES.ZIP": ["CNAECSV"],
            "MOTIVOS.ZIP": ["MOTICSV"],
            "MUNICIPIOS.ZIP": ["MUNICCSV"],
            "NATUREZAS.ZIP": ["NATJUCSV"],
            "PAISES.ZIP": ["PAISCSV"],
            "QUALIFICACOES.ZIP": ["QUALSCSV"],
            "SIMPLES.ZIP": ["SIMPLESCSV"],
            # Data files (numbered parts)
            "EMPRESAS": ["EMPRECSV"],
            "ESTABELECIMENTOS": ["ESTABELE"],
            "SOCIOS": ["SOCIOCSV"],
        }

        existing_files = []
        zip_upper = zip_filename.upper()

        # Check if this is a numbered ZIP file (e.g., Empresas0.zip, Socios5.zip)
        numbered_match = re.match(r"^([A-Z]+)(\d+)\.ZIP$", zip_upper)

        if numbered_match:
            # Handle numbered data files
            base_name = numbered_match.group(1)  # "EMPRESAS"
            part_number = numbered_match.group(2)  # "0", "1", "2", etc.

            if base_name in zip_to_csv_patterns:
                expected_patterns = zip_to_csv_patterns[base_name]
                logger.debug(
                    f"ZIP {zip_filename} matches numbered pattern {base_name}{part_number}, expecting CSV patterns: {expected_patterns} with Y{part_number}"
                )

                # Look for files that match the pattern AND have the correct part number
                for csv_file in self.temp_path.glob("*"):
                    if not csv_file.is_file() or csv_file.name.startswith("."):
                        continue

                    csv_name_upper = csv_file.name.upper()

                    for pattern in expected_patterns:
                        if csv_name_upper.endswith(pattern):
                            # Check if this file has the matching Y number
                            if f"Y{part_number}" in csv_name_upper:
                                existing_files.append(csv_file)
                                logger.debug(
                                    f"Found existing CSV file: {csv_file.name} (matches {pattern} with Y{part_number})"
                                )
                                break
            else:
                logger.debug(f"No pattern found for numbered ZIP base: {base_name}")
        else:
            # Handle single reference files
            for zip_pattern, csv_patterns in zip_to_csv_patterns.items():
                if zip_upper == zip_pattern:
                    logger.debug(
                        f"ZIP {zip_filename} matches pattern {zip_pattern}, expecting CSV patterns: {csv_patterns}"
                    )

                    for csv_file in self.temp_path.glob("*"):
                        if not csv_file.is_file() or csv_file.name.startswith("."):
                            continue

                        csv_name_upper = csv_file.name.upper()

                        for pattern in csv_patterns:
                            if csv_name_upper.endswith(pattern):
                                existing_files.append(csv_file)
                                logger.debug(
                                    f"Found existing CSV file: {csv_file.name} (matches {pattern})"
                                )
                                break
                    break

        if not existing_files:
            logger.debug(f"No existing files found for {zip_filename}")

        return existing_files

    def get_stats(self) -> Dict[str, Any]:
        """Get download statistics."""
        return self.stats.copy()

    def reset_stats(self):
        """Reset download statistics."""
        self.stats = {
            "files_downloaded": 0,
            "total_bytes": 0,
            "start_time": None,
            "end_time": None,
            "errors": [],
        }

    def cleanup(self):
        """Clean up temporary files."""
        if self.config.keep_downloaded_files:
            logger.info(
                "Keeping downloaded files as requested (KEEP_DOWNLOADED_FILES=true)"
            )
            return

        try:
            for file in self.temp_path.glob("*"):
                if file.is_file():
                    file.unlink()
            logger.debug("Temporary files cleaned up")
        except Exception as e:
            logger.warning(f"Error during cleanup: {e}")


class DownloadError(Exception):
    """Custom exception for download-related errors."""

    pass
