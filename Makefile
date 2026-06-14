COMPOSE := docker compose

# WSL: redirect build artefacts to native Linux fs (sccache / chmod won't work on NTFS)
WSL_TARGET_DIR := /home/$(USER)/folio-target
CARGO_ENV      := $(if $(filter /mnt/%,$(CURDIR)),CARGO_TARGET_DIR=$(WSL_TARGET_DIR))

.DEFAULT_GOAL := help

.PHONY: help
help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "  up             start local stack (etcd + minio + folio-node)"
	@echo "  down           stop and remove containers"
	@echo "  logs           tail all container logs"
	@echo "  ps             show container status"
	@echo "  build          build Docker image (folio/node:latest)"
	@echo ""
	@echo "  unit           fast tests — no io_uring required (macOS / WSL safe)"
	@echo "  test           all cargo tests — requires Linux kernel ≥ 5.1 (io_uring)"
	@echo "  docker-test    full test suite inside Docker (seccomp:unconfined)"
	@echo "  check          cargo check + clippy + fmt"

# ── Docker compose ─────────────────────────────────────────────────────────────
.PHONY: up
up:
	$(COMPOSE) up -d

.PHONY: down
down:
	$(COMPOSE) down

.PHONY: logs
logs:
	$(COMPOSE) logs -f

.PHONY: ps
ps:
	$(COMPOSE) ps

.PHONY: build
build:
	docker build --target folio-node -t folio/node:latest .

# ── Tests ──────────────────────────────────────────────────────────────────────
# unit: subset of tests that do NOT require io_uring.
#       Safe on macOS, Windows/WSL, and CI runners without seccomp bypass.
.PHONY: unit
unit:
	$(CARGO_ENV) cargo test \
		--test crash_recovery_integration \
		--test cache_tier_integration \
		--test ledger_hmac_integration \
		-p folio-node \
		-- --nocapture

# test: all cargo tests for folio-node. Requires io_uring (Linux ≥ 5.1).
.PHONY: test
test:
	$(CARGO_ENV) cargo test -p folio-node -- --nocapture

# docker-test: builds a Docker image with pre-compiled tests and runs them.
#              Uses seccomp:unconfined so io_uring syscalls are permitted.
.PHONY: docker-test
docker-test:
	docker build --target test -t folio-test .
	docker run --rm --security-opt seccomp=unconfined folio-test

# ── Code quality ───────────────────────────────────────────────────────────────
.PHONY: check
check:
	$(CARGO_ENV) cargo check --workspace
	$(CARGO_ENV) cargo clippy --workspace -- -D warnings
	$(CARGO_ENV) cargo fmt --all -- --check
