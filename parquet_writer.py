"""Parquet writer for CNPJ data.

Streams Polars DataFrames to partitioned Parquet files using PyArrow.
No database required — reads transformed DataFrames from process_file()
and writes directly to Parquet with ZSTD compression.

Output structure:
    output_dir/
        empresas_000.parquet
        empresas_001.parquet
        estabelecimentos/
            uf=SP/000.parquet
            uf=SP/001.parquet
            uf=RJ/000.parquet
            ...
        socios_000.parquet
        dados_simples_000.parquet
        manifest.json

Multiple files per table/partition is standard (Hive-style).
DuckDB reads them all with glob: 'estabelecimentos/uf=SP/*.parquet'
"""

import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

ROW_GROUP_SIZE = 100_000
COMPRESSION = "zstd"


@dataclass
class TableStats:
    """Track export stats per table."""

    rows: int = 0
    size_bytes: int = 0
    files: list[str] = field(default_factory=list)
    duration_ms: int = 0


class ParquetWriter:
    """Streams DataFrames to partitioned Parquet files."""

    def __init__(self, output_dir: str | Path):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.stats: dict[str, TableStats] = {}
        self._writers: dict[str, pq.ParquetWriter] = {}
        self._file_counters: dict[str, int] = {}

    def _next_path(self, base_dir: Path, prefix: str) -> Path:
        """Get the next numbered file path for a given prefix."""
        key = f"{base_dir}/{prefix}"
        count = self._file_counters.get(key, 0)
        self._file_counters[key] = count + 1
        base_dir.mkdir(parents=True, exist_ok=True)
        return base_dir / f"{prefix}_{count:03d}.parquet"

    def _get_writer(self, path: Path, schema) -> pq.ParquetWriter:
        """Get or create a ParquetWriter for a given path."""
        key = str(path)
        if key not in self._writers:
            path.parent.mkdir(parents=True, exist_ok=True)
            self._writers[key] = pq.ParquetWriter(
                str(path),
                schema,
                compression=COMPRESSION,
            )
        return self._writers[key]

    def write_batch(self, df, table_name: str, columns: list[str]) -> int:
        """Write a batch of data to Parquet.

        For estabelecimentos, partitions by UF.
        For all other tables, writes to a single numbered file.

        Returns the number of rows written.
        """
        if table_name not in self.stats:
            self.stats[table_name] = TableStats()

        arrow_table = df.to_arrow()
        rows = len(df)

        if table_name == "estabelecimentos" and "uf" in columns:
            self._write_partitioned(arrow_table, table_name, "uf")
        else:
            path = self._current_path(table_name)
            writer = self._get_writer(path, arrow_table.schema)
            writer.write_table(arrow_table, row_group_size=ROW_GROUP_SIZE)

        self.stats[table_name].rows += rows
        return rows

    def _current_path(self, table_name: str) -> Path:
        """Get the current file path for a non-partitioned table.

        Returns the existing open file, or creates a new numbered one if none is open.
        """
        # Check if we already have an open writer for this table
        for key in self._writers:
            if table_name in key and "estabelecimentos" not in key:
                return Path(key)

        # No open writer — create a new numbered file
        return self._next_path(self.output_dir, table_name)

    def _write_partitioned(self, table, table_name: str, partition_col: str):
        """Write a table partitioned by a column value."""
        import pyarrow.compute as pc

        partition_dir = self.output_dir / table_name

        col_idx = table.schema.get_field_index(partition_col)
        values = table.column(col_idx).to_pylist()
        unique_values = set(v for v in values if v is not None)

        for value in unique_values:
            mask = pc.equal(table.column(col_idx), value)
            partition_table = table.filter(mask)

            path = self._current_partitioned_path(partition_dir, partition_col, value)
            writer = self._get_writer(path, partition_table.schema)
            writer.write_table(partition_table, row_group_size=ROW_GROUP_SIZE)

    def _current_partitioned_path(self, partition_dir: Path, partition_col: str, value: str) -> Path:
        """Get current file for a partition, or create a new numbered one."""
        part_dir = partition_dir / f"{partition_col}={value}"

        # Check for existing open writer
        for key in self._writers:
            if str(part_dir) in key:
                return Path(key)

        # New numbered file
        return self._next_path(part_dir, "part")

    def flush(self) -> list[Path]:
        """Close all open writers and return absolute paths of flushed files.

        After flush, subsequent writes create new numbered files.
        This prevents overwriting when multiple source files write to the same table.
        """
        flushed: list[Path] = []

        for key, writer in self._writers.items():
            writer.close()
            path = Path(key)
            size = path.stat().st_size
            flushed.append(path)

            for table_name, stats in self.stats.items():
                if table_name in key:
                    stats.size_bytes += size
                    stats.files.append(str(path.relative_to(self.output_dir)))
                    break

        self._writers.clear()
        return flushed

    def close(self):
        """Close all writers and compute file sizes."""
        self.flush()

    def write_manifest(self) -> dict:
        """Write manifest.json with export metadata."""
        manifest = {
            "exportedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "tables": {},
            "totals": {
                "rows": sum(s.rows for s in self.stats.values()),
                "sizeBytes": sum(s.size_bytes for s in self.stats.values()),
                "files": sum(len(s.files) for s in self.stats.values()),
            },
        }

        for table_name, stats in self.stats.items():
            manifest["tables"][table_name] = {
                "rows": stats.rows,
                "sizeBytes": stats.size_bytes,
                "files": stats.files,
            }

        manifest_path = self.output_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2))
        logger.info(f"Manifest written to {manifest_path}")

        return manifest
