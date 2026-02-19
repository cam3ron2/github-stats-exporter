#!/usr/bin/make -f
# Set default shell to bash
SHELL := /bin/bash
CWD := $(shell pwd)
GH_APP_KEY := $(abspath $(CWD)/config/keys/gh-app-key.pem)
export GH_APP_KEY

# parse and load .env files
ifneq (,$(wildcard ./local.env))
	include ./local.env
  export
endif

BINARY_NAME := github-stats-exporter
CMD_SOURCE  := ./cmd/github-stats-exporter
DOCKER_TAG  := github-stats-exporter
OCI_IMAGE_AUTHORS ?= cam3ron2
OCI_IMAGE_URL ?= https://github.com/cam3ron2/github-stats-exporter
OCI_IMAGE_DOCUMENTATION ?= https://github.com/cam3ron2/github-stats-exporter/blob/main/README.md
OCI_IMAGE_SOURCE ?= https://github.com/cam3ron2/github-stats-exporter
OCI_IMAGE_REVISION ?= $(shell git rev-parse --short=12 HEAD 2>/dev/null || echo unknown)
OCI_IMAGE_VERSION ?= dev
OCI_IMAGE_CREATED ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
OCI_IMAGE_TITLE ?= github-stats-exporter
OCI_IMAGE_DESCRIPTION ?= GitHub activity metrics exporter in OpenMetrics format.

ifndef NOCOLOR
  GREEN  := $(shell tput -Txterm setaf 2)
  YELLOW := $(shell tput -Txterm setaf 3)
  WHITE  := $(shell tput -Txterm setaf 7)
  RESET  := $(shell tput -Txterm sgr0)
endif

GIT_TOPLEVEL = $(shell git rev-parse --show-toplevel)

GOCACHE ?= $(CURDIR)/.gocache
GOMODCACHE ?= $(CURDIR)/.gomodcache
GO := env -u GOROOT GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) go

.PHONY: default
default: help

.PHONY: test test-e2e test-e2e-live build run fmt compose issues-backlog issues-backlog-apply

## Run all unit tests.
.PHONY: test
test:
	@echo "${YELLOW}[TEST] ${GREEN}Start Running unit tests.${RESET}"
	$(call quiet-command,$(GO) test -race -v -count=1 -timeout 45s ./...)

## Run deterministic end-to-end tests.
.PHONY: test-e2e
test-e2e:
	@echo "${YELLOW}[TEST] ${GREEN}Start running deterministic e2e tests.${RESET}"
	$(call quiet-command,$(GO) test -tags=e2e -race -v -count=1 -timeout 240s ./test/e2e/...)

## Run live end-to-end scrape tests using config/local.yaml and local app keys.
.PHONY: test-e2e-live
test-e2e-live:
	@echo "${YELLOW}[TEST] ${GREEN}Start running live e2e scrape tests.${RESET}"
	$(call quiet-command,$(GO) test -tags='e2e live' -race -v -count=1 -timeout 600s -run '^TestLive' ./test/e2e/...)

## Run the application
.PHONY: run
run:
	@echo "${YELLOW}[RUN] ${GREEN}Starting application.${RESET}"
	$(GO) run $(CMD_SOURCE) --config=config/local.yaml

## Update formatting with go fmt
.PHONY: fmt
fmt:
	@echo "${YELLOW}[LINT] ${GREEN}Formatting go files.${RESET}"
	$(call quiet-command,$(GO) fmt ./...)

.PHONY: coverage/generate
coverage/generate:
	@echo "${YELLOW}[TEST] ${GREEN}Start Running unit tests and generating coverage report.${RESET}"
	$(call quiet-command,$(GO) test -race -v -cover -coverpkg=./... -coverprofile=cover.out -count=1 -timeout 45s ./...)

.PHONY: coverage/load
coverage/load:
	@echo "${YELLOW}[TEST] ${GREEN}Start Loading coverage report.${RESET}"
	$(call quiet-command,$(GO) tool cover -html=cover.out)

## Generate and load coverage report
.PHONY: coverage
coverage: coverage/generate coverage/load

## Build the application
.PHONY: build
build :
	@echo "${YELLOW}[RUN] ${GREEN}Building binary with go build.${RESET}"
	@mkdir -p ./dist
	$(GO) build -ldflags "$(LD_FLAGS)" $(GO_FLAGS) -o ./dist/$(BINARY_NAME) $(CMD_SOURCE)

## Build the container
.PHONY: build-docker
build-docker :
	@echo "${YELLOW}[RUN] ${GREEN}Building docker image.${RESET}"
	docker build . -t $(DOCKER_TAG) \
		--build-arg OCI_IMAGE_AUTHORS="$(OCI_IMAGE_AUTHORS)" \
		--build-arg OCI_IMAGE_URL="$(OCI_IMAGE_URL)" \
		--build-arg OCI_IMAGE_DOCUMENTATION="$(OCI_IMAGE_DOCUMENTATION)" \
		--build-arg OCI_IMAGE_SOURCE="$(OCI_IMAGE_SOURCE)" \
		--build-arg OCI_IMAGE_REVISION="$(OCI_IMAGE_REVISION)" \
		--build-arg OCI_IMAGE_VERSION="$(OCI_IMAGE_VERSION)" \
		--build-arg OCI_IMAGE_CREATED="$(OCI_IMAGE_CREATED)" \
		--build-arg OCI_IMAGE_TITLE="$(OCI_IMAGE_TITLE)" \
		--build-arg OCI_IMAGE_DESCRIPTION="$(OCI_IMAGE_DESCRIPTION)"

## Run the container locally via Docker Compose
.PHONY: compose
compose:
	@echo "${YELLOW}[RUN] ${GREEN}Start running the container locally.${RESET}"
	$(call quiet-command,docker-compose rm -f)
	$(call quiet-command,docker-compose up --build)

## Dry-run backlog issue creation from docs/ISSUE_BACKLOG.md.
.PHONY: issues-backlog
issues-backlog:
	@echo "${YELLOW}[OPS] ${GREEN}Preview backlog issue creation.${RESET}"
	$(call quiet-command,bash scripts/create-backlog-issues.sh --file docs/ISSUE_BACKLOG.md)

## Create backlog issues from docs/ISSUE_BACKLOG.md.
.PHONY: issues-backlog-apply
issues-backlog-apply:
	@echo "${YELLOW}[OPS] ${GREEN}Create backlog issues.${RESET}"
	$(call quiet-command,bash scripts/create-backlog-issues.sh --file docs/ISSUE_BACKLOG.md --apply)

.PHONY: lint-go
lint-go:
	@echo "${YELLOW}[LINT] ${GREEN}Start golangci-lint.${RESET}"
	$(call quiet-command,env -u GOROOT GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) GOLANGCI_LINT_CACHE=$(CURDIR)/.golangci-cache golangci-lint run -c .github/linters/.golangci.yml ./...)

.PHONY: gitleaks
gitleaks:
	@echo "${YELLOW}[LINT] ${GREEN}Start gitleaks.${RESET}"
	$(call quiet-command,gitleaks --log-level info -v -c .github/linters/.gitleaks.toml dir .)

.PHONY: lint-yaml
lint-yaml:
	@echo "${YELLOW}[LINT] ${GREEN}Start yamllint.${RESET}"
	$(call quiet-command,yamllint -c .github/linters/.yaml-lint.yml .)

.PHONY: lint-checkov
lint-checkov:
	@echo "${YELLOW}[LINT] ${GREEN}Start checkov.${RESET}"
	$(call quiet-command,checkov --summary-position bottom --config-file .github/linters/.checkov.yaml -d .)

.PHONY: fix-go
fix-go:
	@echo "${YELLOW}[LINT] ${GREEN}Start golangci-lint with --fix.${RESET}"
	$(call quiet-command,env -u GOROOT GOCACHE=$(GOCACHE) GOMODCACHE=$(GOMODCACHE) GOLANGCI_LINT_CACHE=$(CURDIR)/.golangci-cache golangci-lint run --fix -c .github/linters/.golangci.yml ./...)

.PHONY: fix-yaml
fix-yaml:
	@echo "${YELLOW}[LINT] ${GREEN}Start yamlfmt.${RESET}"
	$(call quiet-command, yamlfmt -conf .github/linters/.yamlfmt.yml .)

## Run all linters
.PHONY: lint
lint: lint-go lint-yaml lint-checkov gitleaks

## Run all formatters
.PHONY: fix
fix: fmt fix-go fix-yaml

## Combine dependabot branches into a single PR
.PHONY: dependabot/bulk

# Default base branch for combined updates (override with: make dependabot/bulk BASE=my-branch)
BASE ?= updates

dependabot/bulk:
	@@BASE='$(BASE)' bash -c 'set -Eeuo pipefail; \
	  echo "==> Bulk merging Dependabot branches into '\''$${BASE}'\''"; \
	  git fetch --all --prune; \
	  git fetch -p; \
	  for branch in $(git branch -vv | grep ': gone]' | awk '{print $$1}'); do \
	  	git branch -D "$${branch}"; \
	  done; \
	  echo "Pruning stale remote branches from local (dry-run)..."; \
	  p="$$(git remote prune origin --dry-run || true)"; \
	  if [[ -n "$$p" ]]; then \
	    echo "$$p"; \
	    read -rp "Do you want to proceed? (y/n) " choice; \
	    case "$$choice" in \
	      [yY]) echo "Proceeding..."; git remote prune origin ;; \
	      [nN]) echo "Aborting."; exit 0 ;; \
	      *) echo "Invalid input. Please enter '\''y'\'' or '\''n'\''."; exit 1 ;; \
	    esac; \
	  fi; \
	  d="$$(git branch -r | grep dependabot || true)"; \
	  if [[ -z "$$d" ]]; then \
	    echo "No dependabot branches found"; \
	    exit 0; \
	  fi; \
	  git checkout -B "$${BASE}"; \
	  for branch in $$d; do \
	    echo "Merging $$branch ..."; \
	    if ! git merge --no-edit "$$branch"; then :; fi; \
	    c="$$(git diff --name-only --diff-filter=U || true)"; \
	    if [[ -n "$$c" ]]; then \
	      echo "Conflicts found while merging $$branch:"; \
	      echo "$$c"; \
	      read -rp "Do you want to resolve conflicts now? (y/n) " choice; \
	      case "$$choice" in \
	        [yY]) $${EDITOR:-vim} -p $$c; git add $$c; git commit -m "chore(deps): resolve conflicts merging $$branch" ;; \
	        [nN]) echo "Aborting."; exit 0 ;; \
	        *) echo "Invalid input. Please enter '\''y'\'' or '\''n'\''."; exit 1 ;; \
	      esac; \
	    fi; \
	  done; \
	  git push -u origin "$${BASE}"; \
	  gh pr create --title "chore(deps): bulk dependency updates" -a "@me" || true; \
	  git switch main; \
	'

## Remove generated files
.PHONY: clean
clean :
	$(GO) clean
	$(call rm-command,$(BINARY_NAME))
	$(call rm-command,dist/*)
	$(call rm-command,coverage.out)
	$(call rm-command,cover.out)
	$(call rm-command,.gocache)
	$(call rm-command,.gomodcache)
	$(call rm-command,.golangci-cache)

## Display help for all targets
.PHONY: help
help:
	@awk '/^.PHONY: / { \
		msg = match(lastLine, /^## /); \
			if (msg) { \
				cmd = substr($$0, 9, 100); \
				msg = substr(lastLine, 4, 1000); \
				printf "  ${GREEN}%-30s${RESET} %s\n", cmd, msg; \
			} \
	} \
	{ lastLine = $$0 }' $(MAKEFILE_LIST)

quiet-command = $(if ${V},${1},$(if ${2},@echo ${2} && ${1}, @${1}))
rm-command    = $(call quiet-command,shopt -s nullglob; rm -rf ${1},"${YELLOW}[CLEAN] ${GREEN}${1}${RESET}")
