FROM python:3.12-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

COPY pyproject.toml ./
COPY config.py database.py downloader.py processor.py parquet_writer.py main.py ./
COPY initial.sql ./

RUN uv pip install --system -e .

RUN mkdir -p /app/temp /app/parquet

ENTRYPOINT ["python", "main.py"]
