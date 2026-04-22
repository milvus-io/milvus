# tests/Makefile — lint helpers for Python files under tests/.
#
# All targets operate on the *PR-changed* set: files under tests/**/*.py that
# differ between $(BASE_REF) and HEAD (three-dot diff = since merge-base),
# matching the GitHub Actions "Python Lint (tests/)" workflow.
#
# Usage (run from this tests/ directory):
#   make lint          # ruff check          (CI equivalent)
#   make format-check  # ruff format --check (CI equivalent)
#   make ci            # lint + format-check
#   make lint-fix      # ruff check --fix
#   make format        # ruff format
#
# Variables:
#   BASE_REF      diff base ref (default: upstream/master if present, else origin/master)
#   RUFF_VERSION  ruff version (default: 0.15.11, pinned to .github/workflows/python-lint.yaml)
#
# Requires `uvx` (ships with `uv`). Ruff is fetched & cached on first run.

RUFF_VERSION ?= 0.15.11
RUFF         := uvx ruff@$(RUFF_VERSION)
REPO_ROOT    := $(shell git rev-parse --show-toplevel)
BASE_REF     ?= $(shell git -C $(REPO_ROOT) rev-parse --verify upstream/master >/dev/null 2>&1 && echo upstream/master || echo origin/master)

CHANGED_FILES_CMD = git -C $(REPO_ROOT) diff --name-only --diff-filter=ACMR $(BASE_REF)...HEAD -- 'tests/**/*.py'

.PHONY: help lint format-check ci lint-fix format

help:
	@echo "tests/ Makefile — lint PR-changed *.py vs $(BASE_REF) using ruff $(RUFF_VERSION)"
	@echo
	@echo "Targets:"
	@echo "  make lint          ruff check          (CI equivalent)"
	@echo "  make format-check  ruff format --check (CI equivalent)"
	@echo "  make ci            lint + format-check"
	@echo "  make lint-fix      ruff check --fix"
	@echo "  make format        ruff format"
	@echo
	@echo "Override: BASE_REF=origin/master  RUFF_VERSION=0.15.11"

lint:
	@files=$$($(CHANGED_FILES_CMD)); \
	if [ -z "$$files" ]; then echo "No changed Python files under tests/ (base: $(BASE_REF))."; \
	else cd $(REPO_ROOT) && $(RUFF) check $$files; fi

format-check:
	@files=$$($(CHANGED_FILES_CMD)); \
	if [ -z "$$files" ]; then echo "No changed Python files under tests/ (base: $(BASE_REF))."; \
	else cd $(REPO_ROOT) && $(RUFF) format --check --diff $$files; fi

ci: lint format-check

lint-fix:
	@files=$$($(CHANGED_FILES_CMD)); \
	if [ -z "$$files" ]; then echo "No changed Python files under tests/ (base: $(BASE_REF))."; \
	else cd $(REPO_ROOT) && $(RUFF) check --fix $$files; fi

format:
	@files=$$($(CHANGED_FILES_CMD)); \
	if [ -z "$$files" ]; then echo "No changed Python files under tests/ (base: $(BASE_REF))."; \
	else cd $(REPO_ROOT) && $(RUFF) format $$files; fi
