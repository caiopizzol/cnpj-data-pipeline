"""Prepare a fixed source sample, then measure CSV processing and Parquet writes.

Run from the repository root with ``uv run python -m scripts.benchmark_pipeline``.
Use a new process and output directory for each measurement.
Retry failed preparation with the same arguments and directory; setup times
cover only the latest attempt, marked by ``resumed_preparation``.
"""

import argparse
import csv
import hashlib
import json
import os
import platform
import resource
import sys
from collections.abc import Callable
from itertools import islice
from pathlib import Path
from time import perf_counter

import polars as pl
import pyarrow as pa

from config import Config
from downloader import AdaptiveDownloadConcurrency, Downloader
from parquet_writer import ParquetWriter
from processor import process_file


def sha256(path: Path) -> str:
    with path.open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


class TimedDownloader(Downloader):
    download_seconds: float = 0.0

    def _cached_zip_is_valid(self, zip_path: Path) -> bool:
        started = perf_counter()
        valid = super()._cached_zip_is_valid(zip_path)
        self.download_seconds += perf_counter() - started
        return valid

    def _download_zip(
        self,
        url: str,
        directory: str,
        filename: str,
        zip_path: Path,
        log: Callable[[str], None],
        adaptive: AdaptiveDownloadConcurrency | None,
    ) -> None:
        started = perf_counter()
        super()._download_zip(url, directory, filename, zip_path, log, adaptive)
        self.download_seconds += perf_counter() - started


def prepare(config: Config, month: str, archive: str, rows: int) -> dict[str, object]:
    if rows < 1:
        raise ValueError("Sample rows must be positive")
    root = Path(config.temp_dir)
    request = {"month": month, "archive": archive, "base_url": config.base_url, "rows": rows}
    request_path = root / "prepare.json"
    resuming = root.exists()
    if resuming:
        if (
            (root / "sample.json").exists()
            or not request_path.exists()
            or json.loads(request_path.read_text()) != request
        ):
            raise ValueError("Use a new directory or retry the same unfinished preparation")
    else:
        root.mkdir(parents=True)
        request_path.write_text(json.dumps(request, indent=2) + "\n")
    downloader = TimedDownloader(config)
    started = perf_counter()
    sources = downloader.download_file(month, archive)
    download_extract_seconds = perf_counter() - started
    if len(sources) != 1:
        raise ValueError("Choose a ZIP containing exactly one recognized CSV")
    source = sources[0]
    sample = root / f"sample.{source.name}"
    started = perf_counter()
    with source.open(encoding="iso-8859-1", newline="") as input_file:
        with sample.open("w", encoding="iso-8859-1", newline="") as output_file:
            reader = csv.reader(input_file, delimiter=";")
            writer = csv.writer(output_file, delimiter=";", quoting=csv.QUOTE_ALL, lineterminator="\n")
            sampled = 0
            for row in islice(reader, rows):
                writer.writerow(row)
                sampled += 1
    if sampled == 0:
        raise ValueError("Source CSV is empty")
    sampling_seconds = perf_counter() - started
    [archive_path] = [path for path in root.glob("*.zip") if path.name.endswith(f".{archive}")]
    result: dict[str, object] = {
        "source_month": month,
        "source_base_url": config.base_url,
        "archive": archive,
        "archive_sha256": sha256(archive_path),
        "member": source.name,
        "source_csv_bytes": source.stat().st_size,
        "source_csv_sha256": sha256(source),
        "sample": str(sample),
        "sample_rows": sampled,
        "sample_bytes": sample.stat().st_size,
        "sample_sha256": sha256(sample),
        "download_and_crc_seconds": downloader.download_seconds,
        "extraction_seconds": download_extract_seconds - downloader.download_seconds,
        "sampling_seconds": sampling_seconds,
        "resumed_preparation": resuming,
    }
    (root / "sample.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def logical_sha256(path: Path) -> str:
    """Hash schema and ordered row values independently of Parquet row groups."""
    digest = hashlib.sha256()
    frame = pl.read_parquet(path)
    digest.update(str(frame.schema).encode())
    for row in frame.iter_rows():
        digest.update(json.dumps(row, ensure_ascii=False, separators=(",", ":"), default=str).encode())
        digest.update(b"\n")
    return digest.hexdigest()


def measure(source: Path, output: Path, batch_size: int, typed: bool) -> dict[str, object]:
    if batch_size < 1:
        raise ValueError("Batch size must be positive")
    input_sha256 = sha256(source)
    with source.open(encoding="iso-8859-1", newline="") as file:
        expected_rows = sum(1 for _ in csv.reader(file, delimiter=";"))
    output.mkdir(parents=True, exist_ok=False)
    processing_seconds = 0.0
    writing_seconds = 0.0
    rows = 0
    cpu_before = resource.getrusage(resource.RUSAGE_SELF)
    started = perf_counter()
    writer = ParquetWriter(output, typed=typed)
    batches = process_file(source, batch_size=batch_size, typed=typed)
    try:
        while True:
            stage_started = perf_counter()
            item = next(batches, None)
            processing_seconds += perf_counter() - stage_started
            if item is None:
                break
            batch, table, _columns = item
            stage_started = perf_counter()
            writer.write_batch(batch, table)
            writing_seconds += perf_counter() - stage_started
            rows += batch.height
        if rows == 0:
            raise ValueError("Source produced no rows")
        if rows != expected_rows:
            raise ValueError(f"Source has {expected_rows} rows, processed {rows}")
        stage_started = perf_counter()
        writer.close()
        writing_seconds += perf_counter() - stage_started
    finally:
        batches.close()
        writer.abort()
    total_seconds = perf_counter() - started
    usage = resource.getrusage(resource.RUSAGE_SELF)
    peak_rss = usage.ru_maxrss
    peak_rss_bytes = int(peak_rss if sys.platform == "darwin" else peak_rss * 1024)
    parquet_path = next(output.glob("*.parquet"))
    result: dict[str, object] = {
        "input": str(source),
        "input_sha256": input_sha256,
        "input_bytes": source.stat().st_size,
        "rows": rows,
        "batch_size": batch_size,
        "typed": typed,
        "processing_seconds": processing_seconds,
        "writing_seconds": writing_seconds,
        "total_seconds": total_seconds,
        "cpu_seconds": usage.ru_utime + usage.ru_stime - cpu_before.ru_utime - cpu_before.ru_stime,
        "rows_per_second": rows / total_seconds,
        "peak_process_rss_bytes_before_verification": peak_rss_bytes,
        "output_bytes": parquet_path.stat().st_size,
        "output_logical_sha256": logical_sha256(parquet_path),
        "environment": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "polars": pl.__version__,
            "pyarrow": pa.__version__,
            "cpu_count": os.cpu_count(),
            "polars_threads": pl.thread_pool_size(),
        },
    }
    (output / "measurement.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    sample_parser = commands.add_parser("prepare", help="Download one ZIP and keep its first complete CSV records")
    sample_parser.add_argument("month")
    sample_parser.add_argument("archive")
    sample_parser.add_argument("work_dir", type=Path)
    sample_parser.add_argument("--rows", type=int, default=250_000)
    sample_parser.add_argument("--base-url", default=Config(database_url="").base_url)
    run_parser = commands.add_parser("run", help="Measure one fixed CSV sample in a fresh process")
    run_parser.add_argument("source", type=Path)
    run_parser.add_argument("output", type=Path)
    run_parser.add_argument("--batch-size", type=int, default=500_000)
    run_parser.add_argument("--typed", action="store_true")
    args = parser.parse_args()
    if args.command == "prepare":
        config = Config(
            database_url="", temp_dir=str(args.work_dir), base_url=args.base_url, keep_files=True, download_workers=1
        )
        result = prepare(config, args.month, args.archive, args.rows)
    else:
        result = measure(args.source, args.output, args.batch_size, args.typed)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
