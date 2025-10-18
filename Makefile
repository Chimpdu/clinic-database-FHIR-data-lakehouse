compose_file ?= local.compose.yaml

DC_ARGS ?=

.PHONY: help
help: ## Show this help
	@echo "Usage: make <target> [DC_ARGS=\"<docker-compose args>\"]"
	@echo ""
	@echo "Hint:"
	@echo "  To get started run: make clean up logs"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z0-9_/-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| sort -f \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help

.PHONY: logs
logs: ## Check logs from the datalake
	@docker compose -f $(compose_file) logs -f $(DC_ARGS)

.PHONY: up
up: ## Spin up the docker compose for the datalake
	@docker compose -f $(compose_file) up -d $(DC_ARGS)
	@echo "[TIP] You can run make logs to see logs"

.PHONY: down
down: ## Stop running instances of the compose for the datalake
	@docker compose -f $(compose_file) down $(DC_ARGS)

.PHONY: clean
clean: ## Wipe all persisted data
	@docker compose -f $(compose_file) down -v --remove-orphans $(DC_ARGS)

.PHONY: python
python/%:
	@./python-lakehouse/$*

.PHONY: python/create python/read

python/create: python/create_json.py ## Run the python example to upload the patients in the ./data dir to the datalake
	@true

python/read: python/read.py ## Run the python example to read the uploaded patients from the data lake
	@true

trino: ## Use trino to query for patients
	@cd trino-client && \
		go run main.go
