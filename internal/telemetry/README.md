# internal/telemetry

## Logic overview

The `telemetry` package configures OpenTelemetry tracing behavior.

- Supports operational trace modes: `off`, `sampled`, `errors`, `detailed`.
- Applies sampler strategy based on configured mode and sample ratio.
- Stores active mode globally so dependency clients can omit detailed spans unless `detailed` is enabled.
- Registers a global tracer provider.
- Exposes shutdown hook for graceful provider flush/cleanup.

## API reference

### Types

- `Config`: telemetry config input (`Enabled`, `ServiceName`, `TraceMode`, `TraceSampleRatio`).
- `Runtime`: initialized telemetry runtime (`TracerProvider`, `Shutdown`).

### Functions

- `Setup(cfg Config) (Runtime, error)`: initializes global tracing provider and returns runtime handles.
- `TraceMode() string`: returns normalized active trace mode.
- `ShouldTraceDependencies() bool`: returns true only when detailed dependency spans should be emitted.
