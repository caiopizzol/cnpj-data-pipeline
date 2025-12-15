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
run:
    uv run python main.py

# Reset database (delete all data)
reset:
    docker compose down -v && docker compose up -d postgres
