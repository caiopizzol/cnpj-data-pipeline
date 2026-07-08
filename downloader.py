"""Download and extract CNPJ data files from Receita Federal."""

import logging
import os
import re
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterator, List, Mapping, Tuple
from xml.etree import ElementTree

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

# WebDAV XML namespace
DAV_NS = {"d": "DAV:"}
CONTENT_RANGE_RE = re.compile(r"bytes (\d+)-(\d+)/(\d+)")
UNSATISFIED_CONTENT_RANGE_RE = re.compile(r"bytes \*/(\d+)")


class DownloadIncompleteError(RuntimeError):
    """Raised when a response ends before the advertised ZIP size is reached."""


class Downloader:
    """Download and extract CNPJ data files with parallel support."""

    def __init__(self, config: Config):
        self.config = config
        self.temp_path = Path(config.temp_dir)
        self.temp_path.mkdir(exist_ok=True)
        self.auth = (config.share_token, "")

    def _propfind(self, path: str = "") -> ElementTree.Element:
        """Execute a WebDAV PROPFIND request and return parsed XML."""
        url = f"{self.config.base_url}/{path}".rstrip("/") + "/"
        response = requests.request(
            "PROPFIND",
            url,
            auth=self.auth,
            headers={"Depth": "1"},
            timeout=(self.config.connect_timeout, self.config.read_timeout),
        )
        response.raise_for_status()
        return ElementTree.fromstring(response.content)

    def get_available_directories(self) -> List[str]:
        """Get all available data directories from Receita Federal."""
        root = self._propfind()

        directories = []
        for response in root.findall("d:response", DAV_NS):
            href = response.find("d:href", DAV_NS).text
            # Match YYYY-MM directory pattern from href path
            match = re.search(r"(\d{4}-\d{2})/?$", href)
            if match:
                directories.append(match.group(1))

        if not directories:
            raise ValueError("No data directories found")

        return sorted(directories)

    def get_latest_directory(self) -> str:
        """Get the latest data directory from Receita Federal."""
        return self.get_available_directories()[-1]

    def get_directory_files(self, directory: str) -> List[str]:
        """Get list of ZIP files in a directory."""
        root = self._propfind(directory)

        files = []
        for response in root.findall("d:response", DAV_NS):
            href = response.find("d:href", DAV_NS).text
            # Extract .zip filenames from href
            match = re.search(r"/([^/]+\.zip)$", href, re.IGNORECASE)
            if match:
                files.append(match.group(1))

        return files

    def download_file(self, directory: str, filename: str) -> List[Path]:
        """Download and extract a single ZIP file. Returns list of extracted CSV paths."""
        return self._download_and_extract(directory, filename)

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
            for csv_path in self._download_and_extract(directory, filename):
                yield csv_path, filename

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
                extracted_files = future.result()
                for csv_path in extracted_files:
                    yield csv_path, filename

    def _download_and_extract(self, directory: str, filename: str) -> List[Path]:
        """Download a single ZIP file and extract CSV files."""
        url = f"{self.config.base_url}/{directory}/{filename}"
        zip_path = self.temp_path / filename

        # Skip download if keeping files and valid ZIP already exists
        # Use info logging when tqdm is disabled (e.g., Docker, CI)
        log = logger.info if os.environ.get("TQDM_DISABLE") else logger.debug

        if self.config.keep_files and zip_path.exists() and zipfile.is_zipfile(zip_path):
            log(f"Using cached: {filename}")
        else:
            self._download_zip(url, filename, zip_path, log)

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

    def _download_zip(self, url: str, filename: str, zip_path: Path, log) -> None:
        """Download a ZIP through a resumable .part file and validate it."""
        part_path = zip_path.with_name(f"{zip_path.name}.part")

        if zip_path.exists():
            zip_path.unlink()

        for attempt in range(self.config.retry_attempts):
            try:
                log(f"Downloading {filename}...")
                self._download_zip_once(url, filename, zip_path, part_path)
                return
            except Exception as e:
                if zip_path.exists():
                    zip_path.unlink()

                logger.warning(f"Download attempt {attempt + 1} failed: {e}")
                if attempt < self.config.retry_attempts - 1:
                    time.sleep(self.config.retry_delay)
                else:
                    raise

    def _download_zip_once(self, url: str, filename: str, zip_path: Path, part_path: Path) -> None:
        offset = part_path.stat().st_size if part_path.exists() else 0
        headers = {"Range": f"bytes={offset}-"} if offset else {}

        response = requests.get(
            url,
            auth=self.auth,
            headers=headers,
            stream=True,
            timeout=(self.config.connect_timeout, self.config.read_timeout),
        )

        status_code = self._status_code(response)
        if offset and status_code == 416:
            expected_total = self._unsatisfied_range_total(response.headers)
            if expected_total == offset:
                logger.info(f"Completing previously downloaded partial ZIP: {filename}")
                part_path.replace(zip_path)
                self._validate_zip_file(zip_path)
                return

            reason = (
                "server did not report the remote size"
                if expected_total is None
                else f"local size {offset} differs from remote size {expected_total}"
            )
            logger.info(f"Discarding partial download for {filename}: {reason}")
            part_path.unlink(missing_ok=True)
            raise DownloadIncompleteError(f"Cannot resume {filename}: {reason}")

        response.raise_for_status()

        expected_total, write_offset = self._prepare_download_response(response, filename, part_path, offset)

        with tqdm(
            total=expected_total,
            initial=write_offset,
            unit="B",
            unit_scale=True,
            desc=f"Downloading {filename}",
            leave=False,
        ) as pbar:
            with part_path.open("ab") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if not chunk:
                        continue
                    f.write(chunk)
                    pbar.update(len(chunk))

        current_size = part_path.stat().st_size
        if current_size > expected_total:
            part_path.unlink(missing_ok=True)
            raise DownloadIncompleteError(f"Downloaded {current_size} bytes for {filename}, expected {expected_total}")

        if current_size != expected_total:
            raise DownloadIncompleteError(f"Incomplete download for {filename}: {current_size}/{expected_total} bytes")

        part_path.replace(zip_path)
        self._validate_zip_file(zip_path)

    def _prepare_download_response(
        self,
        response: requests.Response,
        filename: str,
        part_path: Path,
        offset: int,
    ) -> tuple[int, int]:
        status_code = self._status_code(response)

        if offset and status_code == 200:
            logger.info(f"Server ignored Range for {filename}; discarding partial download and restarting from byte 0")
            part_path.unlink(missing_ok=True)
            return self._required_content_length(response.headers, filename), 0

        if status_code == 206:
            range_start, range_end, total_size = self._required_content_range(response.headers, filename)
            if range_start != offset:
                logger.info(
                    f"Discarding partial download for {filename}: server resumed at byte {range_start}, "
                    f"expected {offset}"
                )
                part_path.unlink(missing_ok=True)
                raise DownloadIncompleteError(
                    f"Cannot resume {filename}: server resumed at byte {range_start}, expected {offset}"
                )

            if total_size < offset:
                logger.info(
                    f"Discarding partial download for {filename}: local size {offset} exceeds remote size {total_size}"
                )
                part_path.unlink(missing_ok=True)
                raise DownloadIncompleteError(
                    f"Cannot resume {filename}: local size {offset} exceeds remote size {total_size}"
                )

            content_length = self._content_length(response.headers, filename, required=False)
            range_length = range_end - range_start + 1
            if content_length is not None and content_length != range_length:
                raise DownloadIncompleteError(
                    f"Content-Length mismatch for {filename}: {content_length} bytes for range of {range_length}"
                )

            return total_size, offset

        if offset:
            raise DownloadIncompleteError(f"Cannot resume {filename}: server returned HTTP {status_code}")

        return self._required_content_length(response.headers, filename), 0

    def _validate_zip_file(self, zip_path: Path) -> None:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            bad_member = zip_ref.testzip()

        if bad_member is not None:
            raise zipfile.BadZipFile(f"Corrupt ZIP member: {bad_member}")

    def _required_content_length(self, headers: Mapping[str, str], filename: str) -> int:
        content_length = self._content_length(headers, filename, required=True)
        if content_length is None:
            raise DownloadIncompleteError(f"Missing Content-Length for {filename}")
        return content_length

    def _content_length(self, headers: Mapping[str, str], filename: str, required: bool) -> int | None:
        raw_content_length = self._header(headers, "Content-Length")
        if raw_content_length is None:
            if required:
                raise DownloadIncompleteError(f"Missing Content-Length for {filename}")
            return None

        try:
            return int(raw_content_length)
        except ValueError as exc:
            raise DownloadIncompleteError(f"Invalid Content-Length for {filename}: {raw_content_length}") from exc

    def _required_content_range(self, headers: Mapping[str, str], filename: str) -> tuple[int, int, int]:
        raw_content_range = self._header(headers, "Content-Range")
        if raw_content_range is None:
            raise DownloadIncompleteError(f"Missing Content-Range for resumed download of {filename}")

        match = CONTENT_RANGE_RE.fullmatch(raw_content_range.strip())
        if not match:
            raise DownloadIncompleteError(f"Invalid Content-Range for {filename}: {raw_content_range}")

        range_start = int(match.group(1))
        range_end = int(match.group(2))
        total_size = int(match.group(3))

        if range_end < range_start:
            raise DownloadIncompleteError(f"Invalid Content-Range for {filename}: {raw_content_range}")

        return range_start, range_end, total_size

    def _unsatisfied_range_total(self, headers: Mapping[str, str]) -> int | None:
        raw_content_range = self._header(headers, "Content-Range")
        if raw_content_range is None:
            return None

        match = UNSATISFIED_CONTENT_RANGE_RE.fullmatch(raw_content_range.strip())
        if not match:
            return None

        return int(match.group(1))

    @staticmethod
    def _header(headers: Mapping[str, str], name: str) -> str | None:
        for header_name, value in headers.items():
            if header_name.lower() == name.lower():
                return value
        return None

    @staticmethod
    def _status_code(response: requests.Response) -> int:
        status_code = getattr(response, "status_code", 200)
        return status_code if isinstance(status_code, int) else 200

    def cleanup(self):
        """Clean up temporary files."""
        if self.config.keep_files:
            return

        for file in self.temp_path.glob("*"):
            if file.is_file():
                file.unlink()
