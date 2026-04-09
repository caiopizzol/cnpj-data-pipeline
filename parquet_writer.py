"""Parquet writer for CNPJ data.

Streams Polars DataFrames to Parquet files using PyArrow.
No database required — reads transformed DataFrames from process_file()
and writes directly to Parquet with ZSTD compression.

Output structure:
    output_dir/
        empresas.parquet
        estabelecimentos.parquet
        socios.parquet
        simples.parquet
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
from dataclasses import dataclass
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
    file: str = ""


class ParquetWriter:
    """Streams DataFrames to single Parquet files per table."""

    def __init__(self, output_dir: str | Path):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.stats: dict[str, TableStats] = {}
        self._writers: dict[str, pq.ParquetWriter] = {}
        self._lock = threading.Lock()

    def _get_writer(self, table_name: str, schema) -> pq.ParquetWriter:
        """Get or create a ParquetWriter for a table."""
        if table_name not in self._writers:
            path = self.output_dir / f"{table_name}.parquet"
            self._writers[table_name] = pq.ParquetWriter(
                str(path),
                schema,
                compression=COMPRESSION,
            )
        return self._writers[table_name]

    def write_batch(self, df, table_name: str, columns: list[str]) -> int:
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
        if path.exists():
            size = path.stat().st_size
            self.stats[table_name].size_bytes = size
            self.stats[table_name].file = str(path.relative_to(self.output_dir))
            return path
        return None

    def close(self):
        """Close all open writers."""
        for table_name in list(self._writers.keys()):
            self.flush_table(table_name)

    def write_manifest(self) -> dict:
        """Write manifest.json with export metadata."""
        manifest = {
            "exportedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
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
