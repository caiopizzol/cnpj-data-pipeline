"""Download and extract CNPJ data files from Receita Federal."""

import logging
import re
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterator, List, Tuple

import requests
from tqdm import tqdm

from config import Config

logger = logging.getLogger(__name__)

# Known CNPJ file patterns for extraction
CNPJ_FILE_PATTERNS = [
    "CNAECSV",
    "MOTICSV",
    "MUNICCSV",
    "NATJUCSV",
    "PAISCSV",
    "QUALSCSV",
    "EMPRECSV",
    "ESTABELE",
    "SOCIOCSV",
    "SIMPLES",
]

# Reference tables (must be processed first)
REFERENCE_FILES = {
    "Cnaes.zip",
    "Motivos.zip",
    "Municipios.zip",
    "Naturezas.zip",
    "Paises.zip",
    "Qualificacoes.zip",
}


class Downloader:
    """Download and extract CNPJ data files with parallel support."""

    def __init__(self, config: Config):
        self.config = config
        self.temp_path = Path(config.temp_dir)
        self.temp_path.mkdir(exist_ok=True)

    def get_available_directories(self) -> List[str]:
        """Get all available data directories from Receita Federal."""
        response = requests.get(
            self.config.base_url,
            timeout=(self.config.connect_timeout, self.config.read_timeout),
        )
        response.raise_for_status()

        # Parse HTML to find directories (format: YYYY-MM)
        pattern = r'href="(\d{4}-\d{2})/"'
        matches = re.findall(pattern, response.text)

        if not matches:
            raise ValueError("No data directories found")

        return sorted(matches)

    def get_latest_directory(self) -> str:
        """Get the latest data directory from Receita Federal."""
        return self.get_available_directories()[-1]

    def get_directory_files(self, directory: str) -> List[str]:
        """Get list of ZIP files in a directory."""
        url = f"{self.config.base_url}/{directory}"
        response = requests.get(
            url,
            timeout=(self.config.connect_timeout, self.config.read_timeout),
        )
        response.raise_for_status()

        # Parse HTML to find ZIP files
        pattern = r'href="([^"]+\.zip)"'
        return re.findall(pattern, response.text, re.IGNORECASE)

    def download_files(self, directory: str, files: List[str]) -> Iterator[Tuple[Path, str]]:
        """
        Download files with parallel support.

        Reference tables are downloaded first (sequentially),
        then data files in parallel.

        Yields:
            Tuple of (extracted_csv_path, original_zip_filename)
        """
        if not files:
            return

        # Split into reference and data files
        reference_files = [f for f in files if f in REFERENCE_FILES]
        data_files = [f for f in files if f not in REFERENCE_FILES]

        # Process reference files first (sequentially)
        for filename in reference_files:
            try:
                for csv_path in self._download_and_extract(directory, filename):
                    yield csv_path, filename
            except Exception as e:
                logger.error(f"Failed: {filename}: {e}")

        # Process data files in parallel
        if data_files:
            yield from self._download_parallel(directory, data_files)

    def _download_parallel(self, directory: str, files: List[str]) -> Iterator[Tuple[Path, str]]:
        """Download data files in parallel using ThreadPoolExecutor."""
        with ThreadPoolExecutor(max_workers=self.config.download_workers) as executor:
            future_to_filename = {
                executor.submit(self._download_and_extract, directory, filename): filename for filename in files
            }

            for future in as_completed(future_to_filename):
                filename = future_to_filename[future]
                try:
                    extracted_files = future.result()
                    for csv_path in extracted_files:
                        yield csv_path, filename
                except Exception as e:
                    logger.error(f"Failed: {filename}: {e}")

    def _download_and_extract(self, directory: str, filename: str) -> List[Path]:
        """Download a single ZIP file and extract CSV files."""
        url = f"{self.config.base_url}/{directory}/{filename}"
        zip_path = self.temp_path / filename

        # Skip download if keeping files and valid ZIP already exists
        if self.config.keep_files and zip_path.exists() and zipfile.is_zipfile(zip_path):
            logger.debug(f"Using cached: {filename}")
        else:
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

                    total_size = int(response.headers.get("content-length", 0))

                    with tqdm(
                        total=total_size,
                        unit="B",
                        unit_scale=True,
                        desc=f"Downloading {filename}",
                        leave=False,
                    ) as pbar:
                        with open(zip_path, "wb") as f:
                            for chunk in response.iter_content(chunk_size=8192):
                                f.write(chunk)
                                pbar.update(len(chunk))

                    break

                except Exception as e:
                    logger.warning(f"Download attempt {attempt + 1} failed: {e}")
                    if attempt < self.config.retry_attempts - 1:
                        time.sleep(self.config.retry_delay)
                    else:
                        raise

        # Extract CSV files
        extracted_files = []
        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                for member in zip_ref.namelist():
                    member_upper = member.upper()
                    is_cnpj_file = any(pattern in member_upper for pattern in CNPJ_FILE_PATTERNS)

                    if is_cnpj_file:
                        extract_path = self.temp_path / member
                        zip_ref.extract(member, self.temp_path)
                        extracted_files.append(extract_path)
                        logger.debug(f"Extracted: {member}")

        finally:
            # Cleanup ZIP file unless keeping files
            if zip_path.exists() and not self.config.keep_files:
                zip_path.unlink()

        return extracted_files

    def cleanup(self):
        """Clean up temporary files."""
        if self.config.keep_files:
            return

        for file in self.temp_path.glob("*"):
            if file.is_file():
                file.unlink()
