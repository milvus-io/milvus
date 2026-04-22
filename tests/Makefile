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
#   BASE_REF      diff base ref. Auto-detected from the current branch's open PR
#                 via `gh pr view`. If no PR exists, you MUST set it explicitly,
#                 e.g.: make ci BASE_REF=origin/master
#   RUFF_VERSION  ruff version (default: 0.15.11, pinned to .github/workflows/python-lint.yaml)
#
# Requires `uvx` (ships with `uv`). Ruff is fetched & cached on first run.
# Auto-detection requires `gh` authenticated (`gh auth status`).

RUFF_VERSION ?= 0.15.11
RUFF         := uvx ruff@$(RUFF_VERSION)
REPO_ROOT    := $(shell git rev-parse --show-toplevel)

# Auto-detect the base ref from the current branch's open PR.
# Steps:
#   1. Read the PR URL (always lives under the base repo) and base branch via gh.
#      Extract base repo as "<owner>/<name>" from the URL path.
#   2. Find which local remote points to that base repo (must NOT assume "origin"
#      — for forks, "upstream" is typically the base; for direct clones, "origin").
#   3. Emit "<remote>/<baseRefName>".
# Empty when no PR exists, no remote matches, or gh is unauthenticated —
# user must then set BASE_REF explicitly.
BASE_REF ?= $(shell \
  pr_url=$$(gh pr view --json url -q .url 2>/dev/null); \
  base_branch=$$(gh pr view --json baseRefName -q .baseRefName 2>/dev/null); \
  if [ -z "$$pr_url" ] || [ -z "$$base_branch" ]; then exit 0; fi; \
  base_repo=$$(echo "$$pr_url" | sed -E 's|^https?://[^/]+/([^/]+/[^/]+)/.*|\1|'); \
  if [ -z "$$base_repo" ]; then exit 0; fi; \
  remote=$$(git -C $(REPO_ROOT) remote -v | awk -v p="$$base_repo" 'index($$2, p) > 0 && $$3=="(fetch)" {print $$1; exit}'); \
  if [ -z "$$remote" ]; then exit 0; fi; \
  echo "$$remote/$$base_branch")

CHANGED_FILES_CMD = git -C $(REPO_ROOT) diff --name-only --diff-filter=ACMR $(BASE_REF)...HEAD -- 'tests/**/*.py'

.PHONY: help lint format-check ci lint-fix format _require-base-ref

help:
	@echo "tests/ Makefile — lint PR-changed *.py using ruff $(RUFF_VERSION)"
	@echo
	@echo "Targets:"
	@echo "  make lint          ruff check          (CI equivalent)"
	@echo "  make format-check  ruff format --check (CI equivalent)"
	@echo "  make ci            lint + format-check"
	@echo "  make lint-fix      ruff check --fix"
	@echo "  make format        ruff format"
	@echo
	@echo "BASE_REF: auto-detected from the current branch's open PR via 'gh pr view'."
	@echo "          When no PR exists, you must set it explicitly:"
	@echo "            make ci BASE_REF=origin/master"
	@if [ -n "$(BASE_REF)" ]; then echo; echo "Currently detected: BASE_REF=$(BASE_REF)"; \
	else echo; echo "Currently: BASE_REF is unset (no open PR detected)."; fi

_require-base-ref:
	@if [ -z "$(BASE_REF)" ]; then \
	  echo "Error: BASE_REF is not set."; \
	  echo "       No open PR detected for the current branch via 'gh pr view'."; \
	  echo "       Set it explicitly, e.g.:"; \
	  echo "         make $(or $(MAKECMDGOALS),ci) BASE_REF=origin/master"; \
	  exit 1; \
	fi

lint: _require-base-ref
	@files=$$($(CHANGED_FILES_CMD)); \
	if [ -z "$$files" ]; then echo "No changed Python files under tests/ (base: $(BASE_REF))."; \
	else cd $(REPO_ROOT) && $(RUFF) check $$files; fi

format-check: _require-base-ref
	@files=$$($(CHANGED_FILES_CMD)); \
	if [ -z "$$files" ]; then echo "No changed Python files under tests/ (base: $(BASE_REF))."; \
	else cd $(REPO_ROOT) && $(RUFF) format --check --diff $$files; fi

ci: lint format-check

lint-fix: _require-base-ref
	@files=$$($(CHANGED_FILES_CMD)); \
	if [ -z "$$files" ]; then echo "No changed Python files under tests/ (base: $(BASE_REF))."; \
	else cd $(REPO_ROOT) && $(RUFF) check --fix $$files; fi

format: _require-base-ref
	@files=$$($(CHANGED_FILES_CMD)); \
	if [ -z "$$files" ]; then echo "No changed Python files under tests/ (base: $(BASE_REF))."; \
	else cd $(REPO_ROOT) && $(RUFF) format $$files; fi
