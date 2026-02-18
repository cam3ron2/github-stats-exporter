package telemetry

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
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

	sampler := samplerForMode(cfg.TraceMode, cfg.TraceSampleRatio)
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

	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "off":
		return sdktrace.NeverSample()
	case "detailed":
		return sdktrace.AlwaysSample()
	case "errors":
		if clampedRatio <= 0 {
			clampedRatio = 0.01
		}
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(clampedRatio))
	case "sampled", "":
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(clampedRatio))
	default:
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(clampedRatio))
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
