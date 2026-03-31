"""Parquet writer for CNPJ data.

Streams Polars DataFrames to partitioned Parquet files using PyArrow.
No database required — reads transformed DataFrames from process_file()
and writes directly to Parquet with ZSTD compression.

Output structure:
    output_dir/
        empresas.parquet
        estabelecimentos/
            uf=SP.parquet
            uf=RJ.parquet
            ...
        socios.parquet
        dados_simples.parquet
        cnaes.parquet
        municipios.parquet
        ...
        manifest.json
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
        For all other tables, writes to a single file.

        Returns the number of rows written.
        """
        if table_name not in self.stats:
            self.stats[table_name] = TableStats()

        arrow_table = df.to_arrow()
        rows = len(df)

        if table_name == "estabelecimentos" and "uf" in columns:
            self._write_partitioned(arrow_table, table_name, "uf")
        else:
            path = self.output_dir / f"{table_name}.parquet"
            writer = self._get_writer(path, arrow_table.schema)
            writer.write_table(arrow_table, row_group_size=ROW_GROUP_SIZE)

        self.stats[table_name].rows += rows
        return rows

    def _write_partitioned(self, table, table_name: str, partition_col: str):
        """Write a table partitioned by a column value."""
        partition_dir = self.output_dir / table_name

        # Get unique partition values
        col_idx = table.schema.get_field_index(partition_col)
        values = table.column(col_idx).to_pylist()
        unique_values = set(v for v in values if v is not None)

        for value in unique_values:
            # Filter rows for this partition
            import pyarrow.compute as pc

            mask = pc.equal(table.column(col_idx), value)
            partition_table = table.filter(mask)

            path = partition_dir / f"{partition_col}={value}.parquet"
            writer = self._get_writer(path, partition_table.schema)
            writer.write_table(partition_table, row_group_size=ROW_GROUP_SIZE)

    def close(self):
        """Close all writers and compute file sizes."""
        for key, writer in self._writers.items():
            writer.close()
            path = Path(key)
            size = path.stat().st_size
            # Find which table this file belongs to
            for table_name, stats in self.stats.items():
                if table_name in key:
                    stats.size_bytes += size
                    stats.files.append(str(path.relative_to(self.output_dir)))
                    break

        self._writers.clear()

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
