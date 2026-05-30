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
# ──────────────────────────────────────────────────────────────────────────────
# Drone Telemetry Engine – runtime commands
# ──────────────────────────────────────────────────────────────────────────────

COMPOSE        := docker compose
GRAFANA_URL    := http://localhost:3000
KAFKA_UI_URL   := http://localhost:8080

# Cross-platform browser opener
ifeq ($(OS),Windows_NT)
  OPEN := cmd /c start
else
  UNAME := $(shell uname -s)
  ifeq ($(UNAME),Darwin)
    OPEN := open
  else
    OPEN := xdg-open
  endif
endif

.DEFAULT_GOAL := help

.PHONY: help env up dev down restart build logs ps dashboard kafka-ui clean

## help       : Show this help message
help:
	@grep -E '^## ' Makefile | sed 's/^## /  /'

## env        : Create .env from .env.example (skips if .env already exists)
env:
ifeq ($(OS),Windows_NT)
	@if not exist .env (copy .env.example .env & echo .env created from .env.example) else (echo .env already exists - skipping)
else
	@[ -f .env ] || (cp .env.example .env && echo ".env created from .env.example")
endif

## up         : Start all services in scale mode (background)
up: env
	$(COMPOSE) up -d
	@echo ""
	@echo "  Grafana dashboard → $(GRAFANA_URL)  (admin / admin)"
	@echo "  Kafka UI          → $(KAFKA_UI_URL)"
	@echo ""
	@echo "Run 'make dashboard' to open Grafana in your browser."

## dev        : Start all services including PX4 SITL (dev profile)
dev: env
	$(COMPOSE) --profile dev up -d
	@echo ""
	@echo "  Grafana dashboard → $(GRAFANA_URL)  (admin / admin)"
	@echo ""

## down       : Stop and remove containers (keeps volumes)
down:
	$(COMPOSE) down

## restart    : Restart all running containers
restart:
	$(COMPOSE) restart

## build      : Build / rebuild simulator and consumer images
build:
	$(COMPOSE) build

## logs       : Follow logs for all services  (SERVICE=<name> to filter)
logs:
	$(COMPOSE) logs -f $(SERVICE)

## ps         : Show container status
ps:
	$(COMPOSE) ps

## dashboard  : Open the Grafana dashboard in your browser
dashboard:
	$(OPEN) $(GRAFANA_URL)

## kafka-ui   : Open the Kafka UI in your browser
kafka-ui:
	$(OPEN) $(KAFKA_UI_URL)

## clean      : Stop containers and delete all volumes (⚠ destroys data)
clean:
	$(COMPOSE) down -v
	@echo "All containers and volumes removed."
