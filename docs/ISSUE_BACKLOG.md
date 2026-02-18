# Pre-init Issue Backlog

Create these issues once the repository is initialized.

1. Implement single-target Prometheus scrape topology with failover
2. Add backfill coalescing, dedup keys, and enqueue safety caps
3. Implement Redis sharded index model and incremental metrics export cache
4. Implement LOC contributor-stats state machine (`warming`/`ready`/`stale`)
5. Add LOC fallback global API budgets and incomplete-quality signaling
6. Implement role-aware readiness and dependency health contracts
7. Add kustomize base/nonprod/prod deployment with in-repo Redis and RabbitMQ
8. Harden production Kubernetes posture (anti-affinity, network policy, secret handling)
9. Build resilience test suite for failover, duplicate delivery, and outage recovery
