# internal/app

## Logic overview

The `app` package wires runtime role behavior (leader vs follower), scrape/backfill loops, shared health state, and HTTP endpoint composition.

- Runtime backend wiring selects shared dependencies from config:
  - Redis store when `store.backend=redis` and connection succeeds.
  - In-memory store fallback if Redis init fails.
  - RabbitMQ management API queue when AMQP config is present.
  - In-memory queue fallback if RabbitMQ init fails.
- Leader mode runs scheduled scrape cycles and writes metrics to the shared store.
- In multi-org mode, leader scheduling runs one ticker loop per org using each org's configured `scrape_interval`.
- Each leader cycle also writes `gh_exporter_leader_cycle_last_run_unixtime` so `/metrics` is non-empty even when no activity metrics are produced.
- Leader cycles enqueue targeted backfill for both whole-org failures and per-repo missed windows returned by scraper partial results.
- GitHub unhealthy cooldown gates scheduled scraping after repeated failures and enqueues `github_unhealthy` backfill jobs while cooling down.
- Leader cycles run store GC and emit operational metrics (`scrape_runs_total`, `store_write_total`, backfill enqueue counters, rate-limit metrics, dependency health).
- Follower mode consumes backfill jobs with configurable worker fan-out, re-scrapes missed windows, applies idempotency locks, and writes backfill processing metrics.
- Health status is evaluated from role-aware dependency flags and continuously exported as `gh_exporter_dependency_health`.
- HTTP routing exposes `/metrics`, `/livez`, `/readyz`, and `/healthz`, with `/metrics` backed by configurable snapshot caching (`full` or `incremental` refresh).
- Runtime logs include role startup context plus per-cycle scrape summaries for easier operational debugging.

## API reference

### Types

- `Runtime`: orchestrates store, queue, scraper manager, health status, and role loop lifecycle.
- `RoleHooks`: lifecycle contract used by role management (`StartLeader`, `StopLeader`, `StartFollower`, `StopFollower`).
- `RoleManager`: consumes leadership events and transitions between leader/follower hooks.

### Functions

- `NewRuntime(cfg *config.Config, orgScraper scrape.OrgScraper, logger ...*zap.Logger) *Runtime`: constructs runtime, initializes configured store/queue backends with safe fallbacks, creates dispatcher, and wires scrape manager.
- `NewHTTPHandler(metricsHandler http.Handler, healthHandler http.Handler) http.Handler`: returns a chi router for metrics and health endpoints.
- `NewRoleManager(hooks RoleHooks) *RoleManager`: constructs a role manager.

### Methods

- `(*Runtime) Store() runtimeStore`: returns the runtime metric store abstraction used by exporter/tests.
- `(*Runtime) QueueDepth() int`: returns queued backfill message depth.
- `(*Runtime) Handler() http.Handler`: returns combined metrics+health handler.
- `(*Runtime) StartLeader(ctx context.Context)`: transitions runtime into leader loop.
- `(*Runtime) StopLeader()`: stops leader responsibilities.
- `(*Runtime) StartFollower(ctx context.Context)`: transitions runtime into follower loop.
- `(*Runtime) StopFollower()`: stops follower responsibilities.
- `(*Runtime) CurrentStatus(ctx context.Context) health.Status`: returns evaluated health status.
- `(*Runtime) RunLeaderCycle(ctx context.Context) error`: performs one scrape pass, queues backfill for failures, and records internal cycle heartbeat metrics.
- `(*RoleManager) Run(ctx context.Context, roleEvents <-chan bool, electorErrs <-chan error) error`: processes role changes until context cancellation, stream close, or elector error.
