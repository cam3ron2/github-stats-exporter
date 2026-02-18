# internal/health

## Logic overview

The `health` package evaluates runtime dependency health and serves health endpoints.

- `StatusEvaluator` calculates readiness and mode (`healthy`, `degraded`, `unhealthy`) from role-aware inputs.
- `NewHandler` exposes `/livez`, `/readyz`, and `/healthz`.
- `/healthz` returns full JSON status including component-level booleans.

## API reference

### Types

- `Role`: runtime role enum (`leader`, `follower`).
- `Mode`: health mode enum (`healthy`, `degraded`, `unhealthy`).
- `Input`: dependency state used by evaluator.
- `Status`: evaluated health result (`Role`, `Mode`, `Ready`, `Components`).
- `Provider`: interface for runtime status providers used by HTTP handler.
- `StatusEvaluator`: role-aware health evaluation engine.

### Functions

- `NewStatusEvaluator() *StatusEvaluator`: constructs evaluator.
- `NewHandler(provider Provider) http.Handler`: constructs health endpoint handler.

### Methods

- `(*StatusEvaluator) Evaluate(input Input) Status`: computes readiness/mode and component map.
