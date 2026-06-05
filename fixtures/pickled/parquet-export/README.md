Create `scripts/export-parquet.sh`.

The script should:

- be executable
- use `set -euo pipefail`
- set `OUTPUT_FORMAT=parquet`
- default `PARQUET_OUTPUT_DIR` to `./parquet`
- set `PARQUET_TYPED_OUTPUT=true`
- run `uv run python main.py`
- when the first argument is present, pass it as `--month <value>`

Do not set `DATABASE_URL`, call `psql`, or start Docker.
