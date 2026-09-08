FROM python:3.12-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.12.10 /uv /bin/uv

COPY pyproject.toml uv.lock ./
RUN uv export --locked --no-dev --no-emit-project -o /tmp/requirements.txt \
    && uv pip install --system --require-hashes --no-deps -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

COPY file_types.py config.py database.py downloader.py processor.py parquet_writer.py main.py ./
COPY initial.sql ./
RUN uv pip install --system --no-deps -e .

RUN mkdir -p /app/temp /app/parquet

ENTRYPOINT ["python", "main.py"]
