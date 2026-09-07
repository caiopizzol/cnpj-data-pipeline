"""Parquet writer for CNPJ data.

Streams Polars DataFrames to Parquet files using PyArrow.
No database required — reads transformed DataFrames from process_file()
and writes directly to Parquet with ZSTD compression.

Output structure:
    output_dir/
        empresas.parquet
        estabelecimentos.parquet
        socios.parquet
        dados_simples.parquet
        cnaes.parquet
        ...
        manifest.json

One file per table. DuckDB reads them directly:
    SELECT * FROM 'empresas.parquet' WHERE cnpj_basico = '12345678'
"""

import json
import logging
import threading
import time
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import TypedDict

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

ROW_GROUP_SIZE = 100_000
COMPRESSION = "zstd"

# Version of the exported table/column layout. Both typed and string output
# currently use this version; consumers must inspect Parquet field types
# to distinguish them. This is not a fingerprint of the physical schema.
# v2: socios gains socio_id (UUID), deterministic primary key (issue #78).
SCHEMA_VERSION = "2"


def _read_pipeline_version() -> str:
    """Read the pipeline version from pyproject.toml. Returns 'unknown' if
    the file can't be located (e.g. when running from a packaged binary
    where pyproject.toml isn't shipped)."""
    pyproject = Path(__file__).parent / "pyproject.toml"
    try:
        with open(pyproject, "rb") as f:
            data = tomllib.load(f)
        return str(data["project"]["version"])
    except (FileNotFoundError, KeyError):
        return "unknown"


class ManifestTable(TypedDict):
    rows: int
    sizeBytes: int
    file: str


class ManifestTotals(TypedDict):
    rows: int
    sizeBytes: int
    files: int


class Manifest(TypedDict):
    exportedAt: str
    pipelineVersion: str
    schemaVersion: str
    sourceMonth: str | None
    tables: dict[str, ManifestTable]
    totals: ManifestTotals


@dataclass
class TableStats:
    """Track export stats per table."""

    rows: int = 0
    size_bytes: int = 0
    file: str = ""


class ParquetWriter:
    """Streams DataFrames to single Parquet files per table."""

    def __init__(self, output_dir: str | Path, *, source_month: str | None = None, typed: bool = False):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.provenance = {
            "cnpj.sourceMonth": source_month or "",
            "cnpj.schemaVersion": SCHEMA_VERSION,
            "cnpj.typed": str(typed).lower(),
        }
        self.stats: dict[str, TableStats] = {}
        self._writers: dict[str, pq.ParquetWriter] = {}
        self._lock = threading.Lock()

    def _get_writer(self, table_name: str, schema: pa.Schema) -> pq.ParquetWriter:
        """Get or create a ParquetWriter for a table."""
        if table_name not in self._writers:
            path = self.output_dir / f"{table_name}.parquet.partial"
            self._writers[table_name] = pq.ParquetWriter(
                str(path),
                schema,
                compression=COMPRESSION,
            )
            self._writers[table_name].add_key_value_metadata(self.provenance)
        return self._writers[table_name]

    def write_batch(self, df: pl.DataFrame, table_name: str) -> int:
        """Write a batch of data to Parquet. Thread-safe. Returns the number of rows written."""
        arrow_table = df.to_arrow()
        rows = len(df)

        with self._lock:
            if table_name not in self.stats:
                self.stats[table_name] = TableStats()

            writer = self._get_writer(table_name, arrow_table.schema)
            writer.write_table(arrow_table, row_group_size=ROW_GROUP_SIZE)
            self.stats[table_name].rows += rows

        return rows

    def flush_table(self, table_name: str) -> Path | None:
        """Close the writer for a specific table. Returns the file path."""
        if table_name not in self._writers:
            return None

        self._writers[table_name].close()
        del self._writers[table_name]

        path = self.output_dir / f"{table_name}.parquet"
        (self.output_dir / f"{table_name}.parquet.partial").replace(path)
        self.stats[table_name].size_bytes = path.stat().st_size
        self.stats[table_name].file = path.name
        return path

    def include_existing_table(self, table_name: str) -> None:
        """Include a resumed table in the manifest, validating its Parquet footer."""
        path = self.output_dir / f"{table_name}.parquet"
        with pq.ParquetFile(path) as existing:
            metadata = existing.metadata.metadata or {}
            for key, expected in self.provenance.items():
                actual = metadata.get(key.encode())
                if not expected or actual != expected.encode():
                    raise ValueError(
                        f"Cannot resume {path.name}: {key} is {actual!r}, expected {expected!r}. "
                        "Use a fresh output directory or regenerate this export."
                    )
            if existing.metadata.num_rows == 0:
                raise ValueError(f"Cannot resume {path.name}: empty table. Regenerate this export.")
            self.stats[table_name] = TableStats(
                rows=existing.metadata.num_rows, size_bytes=path.stat().st_size, file=path.name
            )

    def abort(self) -> None:
        """Release writers without publishing incomplete tables."""
        for table_name, writer in self._writers.items():
            try:
                writer.close()
            except Exception:
                logger.exception("Failed to close partial output for %s", table_name)
        self._writers.clear()

    def close(self):
        """Close all open writers."""
        for table_name in list(self._writers.keys()):
            self.flush_table(table_name)

    def write_manifest(self, source_month: str | None = None) -> Manifest:
        """Write manifest.json with export metadata.

        Args:
            source_month: The Receita Federal directory the data came from
                (e.g. "2024-11"). Recorded in the manifest so downstream
                consumers can detect when a new month landed without
                cross-referencing the original ZIP listing.
        """
        manifest: Manifest = {
            "exportedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "pipelineVersion": _read_pipeline_version(),
            "schemaVersion": SCHEMA_VERSION,
            "sourceMonth": source_month,
            "tables": {},
            "totals": {
                "rows": sum(s.rows for s in self.stats.values()),
                "sizeBytes": sum(s.size_bytes for s in self.stats.values()),
                "files": len(self.stats),
            },
        }

        for table_name, stats in self.stats.items():
            manifest["tables"][table_name] = {
                "rows": stats.rows,
                "sizeBytes": stats.size_bytes,
                "file": stats.file,
            }

        manifest_path = self.output_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2))
        logger.info(f"Manifest written to {manifest_path}")

        return manifest
