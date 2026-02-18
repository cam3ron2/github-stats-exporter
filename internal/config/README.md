# internal/config

## Logic overview

The `config` package loads YAML configuration, applies defaults, parses flexible durations, and validates runtime constraints.

- YAML decoding uses `yaml.v3` with known-field enforcement.
- Duration fields support Go duration syntax plus `d` (days) and `w` (weeks).
- Defaults are applied for log level, metrics topology, store backend/mode, and export cache mode.
- Validation enforces role-critical constraints (org uniqueness, fallback budgets, store mode requirements).

## API reference

### Types

- `Config`: root application configuration.
- `ServerConfig`: HTTP bind address and log level.
- `MetricsConfig`: metrics scrape topology settings.
- `LeaderElectionConfig`: lease/election timing and names.
- `GitHubConfig`: API settings and organization blocks.
- `GitHubOrgConfig`: per-org GitHub App installation and scrape settings.
- `RateLimitConfig`: rate-limit policy controls.
- `RetryConfig`: retry attempt/backoff controls.
- `LOCConfig`: LOC source and fallback policy settings.
- `BackfillConfig`: queue processing and dedup settings.
- `AMQPConfig`: RabbitMQ endpoint/exchange/queue names.
- `StoreConfig`: Redis backend and retention/cache settings.
- `HealthConfig`: dependency probe settings.
- `TelemetryConfig`: OpenTelemetry mode and sampling settings.

### Functions

- `Load(reader io.Reader) (*Config, error)`: decodes YAML, applies defaults, validates, and returns config.

### Methods

- `(*Config) Validate() error`: validates semantic constraints and aggregates validation failures.
