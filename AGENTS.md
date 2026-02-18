---
post_title: "AGENTS.md"
author1: "cam3ron2"
post_slug: "agents-md"
microsoft_alias: "cam3ron2"
featured_image: "https://example.com/agents-md.png"
categories: ["documentation", "devops"]
tags: ["agents", "automation", "go", "docker", "kubernetes"]
ai_note: "AI-assisted"
summary: "Agent-focused guidance for working on github-stats-exporter, including setup, workflows, testing, and deployment."
post_date: "2026-02-17"
---

# AGENTS

## Project overview

`github-stats-exporter` is a Go service that scrapes GitHub org metrics, stores them, and exposes an HTTP endpoint with metrics and health. It includes leader/follower roles, backfill processing, and Redis/RabbitMQ integrations for queueing and storage.

Key technologies:

- Go 1.25 (module: `github.com/cam3ron2/github-stats-exporter`)
- Docker/Docker Compose for local runtime
- Redis and RabbitMQ for storage/queueing
- Prometheus/OpenMetrics exporter and HTTP health endpoints

## Setup commands

- Install Go toolchain: `go version` should report `1.25.x`
- Fetch dependencies: `go mod download`
- Local configuration: `config/local.yaml`
- GitHub App key: place PEM at `config/keys/gh-app-key.pem` (see `config/keys/example-org.pem`)
- Optional env overrides: create `local.env` (auto-loaded by `Makefile`)

## Development workflow

- Run locally (Go): `make run` (uses `config/local.yaml`)
- Build binary: `make build`
- Start via Docker Compose: `make compose`
- Format Go code: `make fmt`

Notes:

- `make` uses isolated Go caches in `.gocache/` and `.gomodcache/`.
- Docker Compose expects Redis and RabbitMQ to be healthy before the app starts.

## Testing instructions

- Run all tests: `make test`
- Run a package: `go test ./internal/app`
- Run a single test: `go test ./internal/app -run TestRoleManager_Run`

Test conventions:

- Unit tests live alongside code in `*_test.go` files under `internal/`.
- Prefer table-driven tests and deterministic time via injected clocks (see `runtime.go`).

## Code style

- Go formatting: `gofmt -w` (or `make fmt`)
- Keep packages small and focused under `internal/`.
- Error handling: return early, wrap with context (`fmt.Errorf("...: %w", err)`)
- Avoid new dependencies unless necessary; prefer standard library.

## Build and deployment

- Local binary: `make build` produces `github-stats-exporter`
- Docker build: `docker build -t github-stats-exporter:dev .`
- Compose runtime: `make compose` (builds and runs `github-stats-exporter:dev`)

Deployment inputs:

- Configuration is file-based (`config/local.yaml`) and mounted into the container at `/app/config/local.yaml`.
- Secrets are file-based (GitHub App PEM mounted at `/app/keys/gh-app-key.pem`).

## Security considerations

- Never commit real GitHub App keys. Use `config/keys/example-org.pem` only as a template.
- Prefer env files (`local.env`) for local secrets and keep them out of git.
- Validate configuration changes against `config/local.yaml` schema expectations in `internal/config`.

## Pull request guidelines

- Run `make fmt` and `make test` before opening a PR.
- Keep changes focused; update tests for behavioral changes.
- If you touch deployment manifests under `deploy/`, validate YAML formatting and consistency.

## Debugging and troubleshooting

- If leader scrape cycles show no metrics, confirm GitHub App credentials and org allowlist in `config/local.yaml`.
- For queue issues, check RabbitMQ health on port `15672` when running via Compose.
- For Redis issues, ensure `redis:6379` is reachable from the app container.

## CI/CD notes

- Linting runs via Super Linter in `.github/workflows/lint.yml` on PRs.
- Markdown files are excluded from linting in CI (`FILTER_REGEX_EXCLUDE`).
