# CNPJ Data Pipeline Makefile
# Simple, focused commands for ETL pipeline operations

# Default shell
SHELL := /bin/bash

# Python interpreter
PYTHON := python3

# Default target
.DEFAULT_GOAL := help

# Variable to hold the correct Docker Compose command
# It uses docker compose (V2) if available; otherwise, it falls back to docker-compose (V1).
DOCKER_COMPOSE := $(shell command -v docker compose >/dev/null 2>&1 && echo 'docker compose' || echo 'docker-compose')

# Help command - shows available targets
help:
	@echo "Available commands:"
	@echo "  make setup          🚀 Run interactive setup wizard"
	@echo "  make install        📦 Install Python dependencies"
	@echo "  make env            🔧 Create .env from example"
	@echo "  make run            🏃 Run the full pipeline"
	@echo "  make docker-build   🐳 Build Docker image"
	@echo "  make docker-run     🐳 Run pipeline in Docker (interactive)"
	@echo "  make docker-rund    🐳 Run pipeline in Docker (detached)"
	@echo "  make docker-db      🐳 Start PostgreSQL container"
	@echo "  make docker-stop    🛑 Stop all containers"
	@echo "  make docker-clean   🗑️  Remove containers and volumes"
	@echo "  make clean          🧹 Remove temporary files and logs"
	@echo "  make clean-data     🗑️  Remove downloaded data files"
	@echo "  make logs           📋 Show recent log entries"

# Setup & Installation targets
setup:
	@echo "🚀 Running interactive setup..."
	@$(PYTHON) setup.py

install:
	@echo "📦 Installing dependencies..."
	@pip install -r requirements.txt

env:
	@echo "🔧 Creating .env file..."
	@cp -n env.example .env || true
	@echo "✅ .env created. Please edit it with your settings."

# Pipeline operation targets
run:
	@echo "🏃 Running CNPJ pipeline..."
	@$(PYTHON) main.py

# Docker targets
docker-build:
	@echo "🐳 Building Docker image..."
	@$(DOCKER_COMPOSE) build

docker-run:
	@echo "🐳 Running pipeline in Docker (interactive mode)..."
	@$(DOCKER_COMPOSE) --profile postgres up

docker-rund:
	@echo "🐳 Running pipeline in Docker (detached mode)..."
	@$(DOCKER_COMPOSE) --profile postgres up -d

docker-db:
	@echo "🐳 Starting PostgreSQL database..."
	@$(DOCKER_COMPOSE) --profile postgres up -d postgres

docker-stop:
	@echo "🛑 Stopping all containers..."
	@$(DOCKER_COMPOSE) --profile postgres down

docker-clean:
	@echo "🗑️ Removing containers and volumes..."
	@$(DOCKER_COMPOSE) --profile postgres down -v
	@echo "✅ Docker cleanup complete!"

# Maintenance targets
clean:
	@echo "🧹 Cleaning temporary files..."
	@rm -rf __pycache__ */__pycache__ */*/__pycache__
	@rm -rf .pytest_cache
	@rm -f .coverage
	@find . -name "*.pyc" -delete
	@find . -name ".DS_Store" -delete
	@echo "✅ Clean complete!"

clean-data:
	@echo "⚠️  WARNING: This will delete downloaded CNPJ data files!"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		echo "🗑️ Removing data files..."; \
		rm -rf temp/; \
		echo "✅ Downloaded files removed!"; \
	else \
		echo "Cancelled."; \
	fi

logs:
	@echo "📋 Recent log entries:"
	@tail -n 50 logs/cnpj_loader.log 2>/dev/null || echo "No logs found. Run 'make run' first."

# Export sample for testing/demo
export-sample:
	@echo "📦 Exporting São Paulo sample (10k companies)..."
	@python export.py sample
	@echo "✅ Done! Check exports/ directory"

# Export full São Paulo dataset
export-sp-full:
	@echo "🚀 Exporting ALL São Paulo companies (this will take time)..."
	@python export.py sp_full
	@echo "✅ Done! Check exports/ directory"

.PHONY: help setup install env run \
	docker-build docker-run docker-rund docker-db docker-stop docker-clean \
	clean clean-data logs \
	export-sample export-sp-full
