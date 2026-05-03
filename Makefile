.PHONY: help up down build test lint dashboard sim parser

PYTHON ?= python3

help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Docker-Compose targets
# ---------------------------------------------------------------------------
up:  ## Start the full stack (Kafka + simulator + parser + dashboard)
	docker compose up --build -d
	@echo ""
	@echo "Dashboard → http://localhost:8501"

down:  ## Stop and remove all containers
	docker compose down

build:  ## (Re)build images without starting
	docker compose build

logs:  ## Tail logs from all services
	docker compose logs -f

# ---------------------------------------------------------------------------
# Local development (requires Kafka running at localhost:9092)
# ---------------------------------------------------------------------------
install:  ## Install Python dependencies locally
	pip install -r requirements.txt

sim:  ## Run the swarm simulator locally
	$(PYTHON) -m sim.swarm_simulator

parser:  ## Run the telemetry parser locally
	$(PYTHON) -m parser.telemetry_parser

dashboard:  ## Run the Streamlit dashboard locally
	streamlit run dashboard/app.py

# ---------------------------------------------------------------------------
# Testing
# ---------------------------------------------------------------------------
test:  ## Run unit tests
	pytest tests/ -v

test-cov:  ## Run tests with coverage report
	pytest tests/ -v --tb=short
