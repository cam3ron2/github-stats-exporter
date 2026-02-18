package telemetry

import (
	"context"
	"strings"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

var globalTraceMode atomic.Value

const (
	traceModeOff      = "off"
	traceModeErrors   = "errors"
	traceModeSampled  = "sampled"
	traceModeDetailed = "detailed"
)

// Config configures OpenTelemetry tracing setup.
type Config struct {
	Enabled          bool
	ServiceName      string
	TraceMode        string
	TraceSampleRatio float64
}

// Runtime contains initialized telemetry providers and lifecycle hooks.
type Runtime struct {
	TracerProvider *sdktrace.TracerProvider
	Shutdown       func(ctx context.Context) error
}

// Setup initializes global OpenTelemetry tracing according to the provided configuration.
func Setup(cfg Config) (Runtime, error) {
	serviceName := strings.TrimSpace(cfg.ServiceName)
	if serviceName == "" {
		serviceName = "github-stats"
	}

	effectiveTraceMode := cfg.TraceMode
	if !cfg.Enabled {
		effectiveTraceMode = traceModeOff
	}
	setTraceMode(effectiveTraceMode)

	sampler := samplerForMode(effectiveTraceMode, cfg.TraceSampleRatio)
	if !cfg.Enabled {
		sampler = sdktrace.NeverSample()
	}

	resourceConfig, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(semconv.ServiceNameKey.String(serviceName)),
	)
	if err != nil {
		return Runtime{}, err
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sampler),
		sdktrace.WithResource(resourceConfig),
	)
	otel.SetTracerProvider(provider)

	return Runtime{
		TracerProvider: provider,
		Shutdown:       provider.Shutdown,
	}, nil
}

func samplerForMode(mode string, ratio float64) sdktrace.Sampler {
	clampedRatio := clampRatio(ratio)

	switch normalizeTraceMode(mode) {
	case traceModeOff:
		return sdktrace.NeverSample()
	case traceModeDetailed:
		return sdktrace.AlwaysSample()
	case traceModeErrors:
		if clampedRatio <= 0 {
			clampedRatio = 0.01
		}
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(clampedRatio))
	case traceModeSampled, "":
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(clampedRatio))
	default:
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(clampedRatio))
	}
}

// TraceMode reports the configured global trace mode.
func TraceMode() string {
	value := globalTraceMode.Load()
	if value == nil {
		return traceModeOff
	}
	mode, _ := value.(string)
	if mode == "" {
		return traceModeOff
	}
	return mode
}

// ShouldTraceDependencies reports if detailed dependency spans should be emitted.
func ShouldTraceDependencies() bool {
	return TraceMode() == traceModeDetailed
}

func setTraceMode(mode string) {
	globalTraceMode.Store(normalizeTraceMode(mode))
}

func normalizeTraceMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case traceModeOff:
		return traceModeOff
	case traceModeErrors:
		return traceModeErrors
	case traceModeSampled, "":
		return traceModeSampled
	case traceModeDetailed:
		return traceModeDetailed
	default:
		return traceModeSampled
	}
}

func clampRatio(ratio float64) float64 {
	if ratio < 0 {
		return 0
	}
	if ratio > 1 {
		return 1
	}
	return ratio
}
