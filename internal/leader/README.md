# internal/leader

## Logic overview

The `leader` package provides leadership-event orchestration primitives used to drive leader/follower runtime behavior.

- `Elector` is the abstraction for any leader-election backend (Kubernetes Lease, static mode, test feed).
- `Runner` converts election callbacks into role and error channels for consumers like runtime role managers.
- Role transitions are deduplicated to avoid repeated `leader -> leader` or `follower -> follower` churn.
- Context cancellation is treated as normal shutdown and not surfaced as an error.

## API reference

### Types

- `Elector`: backend contract that emits role transitions.
- `Runner`: wraps an `Elector` and exposes role/error channels.
- `StaticElector`: emits a fixed role and waits for context cancellation.
- `ChannelElector`: emits role transitions from a channel.

### Functions

- `NewRunner(elector Elector, logger ...*zap.Logger) *Runner`: creates a runner with optional zap logger.

### Methods

- `(*Runner) Start(ctx context.Context) (<-chan bool, <-chan error)`: starts election observation and returns deduplicated role transitions plus fatal election errors.
- `(StaticElector) Run(ctx context.Context, emit func(isLeader bool)) error`: emits the configured role once.
- `(ChannelElector) Run(ctx context.Context, emit func(isLeader bool)) error`: relays events from `Events` until close or cancellation.
