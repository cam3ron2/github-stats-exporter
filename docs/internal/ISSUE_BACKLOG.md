# Issue Backlog Status

This file tracks status for the original pre-init backlog.

Status legend:

- `done`: implemented
- `partial`: partially implemented; follow-up work remains
- `open`: not started

| # | Item | Status | Notes |
| --- | --- | --- | --- |
| 1 | Implement single-target Prometheus scrape topology with failover | `done` | Single logical scrape target defined via service DNS and scrape config. |
| 2 | Add backfill coalescing, dedup keys, and enqueue safety caps | `done` | Dispatcher coalescing, dedup lock behavior, and org enqueue caps are implemented. |
| 3 | Implement Redis sharded index model and incremental metrics export cache | `done` | Redis sharded index usage and incremental export cache flow are implemented. |
| 4 | Implement LOC contributor-stats state machine (`warming`/`ready`/`stale`) | `done` | LOC state machine exists and is wired into scrape behavior. |
| 5 | Add LOC fallback global API budgets and incomplete-quality signaling | `done` | Per-org fallback budget and incomplete signaling metric are implemented. |
| 6 | Implement role-aware readiness and dependency health contracts | `done` | Role-aware readiness and dependency health endpoints/contracts are implemented. |
| 7 | Add kustomize base/nonprod/prod deployment with in-repo Redis and RabbitMQ | `done` | Base + nonprod + prod overlays with Redis/RabbitMQ are present. |
| 8 | Harden production Kubernetes posture (anti-affinity, network policy, secret handling) | `partial` | Anti-affinity and network policy are implemented; secret handling still uses in-repo/static templates. |
| 9 | Build resilience test suite for failover, duplicate delivery, and outage recovery | `partial` | Compose functional checks exist; comprehensive automated failover/duplicate/outage suite is still incomplete. |
