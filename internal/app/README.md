# internal/app

## Logic overview

The `app` package wires runtime role behavior (leader vs follower), scrape/backfill loops, shared health state, and HTTP endpoint composition.

- Leader mode runs scheduled scrape cycles and writes metrics to the store.
- Each leader cycle also writes `gh_exporter_leader_cycle_last_run_unixtime` so `/metrics` is non-empty even when no activity metrics are produced.
- Follower mode consumes backfill jobs from the queue and writes backfill result metrics.
- Health status is evaluated from role-aware dependency flags.
- HTTP routing exposes `/metrics`, `/livez`, `/readyz`, and `/healthz`.
- Runtime logs include role startup context plus per-cycle scrape summaries for easier operational debugging.

## API reference

### Types

- `Runtime`: orchestrates store, queue, scraper manager, health status, and role loop lifecycle.
- `RoleHooks`: lifecycle contract used by role management (`StartLeader`, `StopLeader`, `StartFollower`, `StopFollower`).
- `RoleManager`: consumes leadership events and transitions between leader/follower hooks.

### Functions

- `NewRuntime(cfg *config.Config, orgScraper scrape.OrgScraper, logger ...*zap.Logger) *Runtime`: constructs runtime defaults, in-memory store/queue, dispatcher, and scrape manager.
- `NewHTTPHandler(metricsHandler http.Handler, healthHandler http.Handler) http.Handler`: returns a chi router for metrics and health endpoints.
- `NewRoleManager(hooks RoleHooks) *RoleManager`: constructs a role manager.

### Methods

- `(*Runtime) Store() *store.MemoryStore`: returns the runtime metric store.
- `(*Runtime) QueueDepth() int`: returns queued backfill message depth.
- `(*Runtime) Handler() http.Handler`: returns combined metrics+health handler.
- `(*Runtime) StartLeader(ctx context.Context)`: transitions runtime into leader loop.
- `(*Runtime) StopLeader()`: stops leader responsibilities.
- `(*Runtime) StartFollower(ctx context.Context)`: transitions runtime into follower loop.
- `(*Runtime) StopFollower()`: stops follower responsibilities.
- `(*Runtime) CurrentStatus(ctx context.Context) health.Status`: returns evaluated health status.
- `(*Runtime) RunLeaderCycle(ctx context.Context) error`: performs one scrape pass, queues backfill for failures, and records internal cycle heartbeat metrics.
- `(*RoleManager) Run(ctx context.Context, roleEvents <-chan bool, electorErrs <-chan error) error`: processes role changes until context cancellation, stream close, or elector error.
