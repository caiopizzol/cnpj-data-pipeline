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

# Run pipeline once
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

# Run tests with coverage (requires PostgreSQL)
test:
    uv run --frozen pytest --cov

# Check Python types strictly
typecheck:
    uv run --frozen pyright

# Run all checks (lint, format, typecheck, test)
check:
    uv run --frozen ruff check . && uv run --frozen ruff format --check . && uv run --frozen pyright && uv run --frozen pytest --cov

# Data quality report. Only check digits are sampled; --full scans those too.
# Requires DATABASE_URL pointing at a populated CNPJ database.
data-quality-report *ARGS:
    uv run python scripts/data_quality_report.py {{ARGS}}
