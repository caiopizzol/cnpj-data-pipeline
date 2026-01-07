# CNPJ Data Pipeline

# Install dependencies
install:
    uv sync

# Start PostgreSQL
up:
    docker compose up -d postgres

# Stop PostgreSQL
down:
    docker compose down

# Enter database shell
db:
    docker exec -it cnpj-pipeline-postgres psql -U postgres -d cnpj

# Run pipeline
run *ARGS:
    uv run python main.py {{ARGS}}

# Reset database (delete all data)
reset:
    docker compose down -v && docker compose up -d postgres

# Lint code
lint:
    uv run ruff check .

# Format code
format:
    uv run ruff format .

# Run tests
test:
    uv run pytest

# Run all checks (lint, format, test)
check:
    uv run ruff check . && uv run ruff format --check . && uv run pytest
