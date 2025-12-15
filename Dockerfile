FROM python:3.12-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Copy project files
COPY pyproject.toml .
COPY config.py database.py downloader.py processor.py main.py ./

# Install dependencies
RUN uv pip install --system -e .

# Create temp directory
RUN mkdir -p /app/temp

CMD ["python", "main.py"]
