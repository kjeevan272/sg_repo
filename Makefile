.PHONY: fmt lint test local-up local-down seed
fmt:        ; ruff check --fix . && black .
lint:       ; ruff check . && black --check . && mypy src
test:       ; pytest -q
local-up:   ; docker compose -f docker-compose.dev.yml up -d
local-down: ; docker compose -f docker-compose.dev.yml down -v
seed:       ; python -m scripts.seed_localstack
