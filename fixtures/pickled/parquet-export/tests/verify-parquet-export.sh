#!/usr/bin/env bash
set -euo pipefail

test -f README.md
test -f scripts/export-parquet.sh
test -x scripts/export-parquet.sh

grep -Fq "set -euo pipefail" scripts/export-parquet.sh
grep -Fq "OUTPUT_FORMAT=parquet" scripts/export-parquet.sh
grep -Fq "PARQUET_OUTPUT_DIR" scripts/export-parquet.sh
grep -Fq "./parquet" scripts/export-parquet.sh
grep -Fq "PARQUET_TYPED_OUTPUT=true" scripts/export-parquet.sh
grep -Fq "uv run python main.py" scripts/export-parquet.sh
grep -Fq -- "--month" scripts/export-parquet.sh
grep -Fq '"$1"' scripts/export-parquet.sh

if grep -R "DATABASE_URL\\|psql\\|docker compose\\|just up" scripts/export-parquet.sh >/dev/null; then
  echo "Parquet export should not require PostgreSQL or Docker" >&2
  exit 1
fi
