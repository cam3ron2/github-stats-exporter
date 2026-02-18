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

BINARY_NAME := github-stats
CMD_SOURCE  := ./cmd/github-stats
DOCKER_TAG  := github-stats

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

.PHONY: test build run fmt compose

test:
	$(GO) test ./...

build:
	$(GO) build ./cmd/github-stats

run:
	$(GO) run ./cmd/github-stats --config=config/local.yaml

fmt:
	env -u GOROOT gofmt -w $$(find . -name '*.go' -type f)

## Run the container locally via Docker Compose
.PHONY: compose
compose:
	@echo "${YELLOW}[RUN] ${GREEN}Start running the container locally.${RESET}"
	$(call quiet-command,docker-compose rm -f)
	$(call quiet-command,docker-compose up --build)

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