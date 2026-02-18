package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

var globalTraceMode atomic.Value
var newOTLPExporter = func(endpoint string) (sdktrace.SpanExporter, error) {
	return newOTLPHTTPExporter(endpoint), nil
}

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
	OTLPEndpoint     string
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
	if cfg.Enabled && strings.TrimSpace(cfg.OTLPEndpoint) != "" {
		exporter, err := buildOTLPExporter(cfg.OTLPEndpoint)
		if err != nil {
			return Runtime{}, err
		}
		provider = sdktrace.NewTracerProvider(
			sdktrace.WithSampler(sampler),
			sdktrace.WithResource(resourceConfig),
			sdktrace.WithBatcher(exporter),
		)
	}
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

func buildOTLPExporter(endpoint string) (sdktrace.SpanExporter, error) {
	normalizedEndpoint, err := otlpTraceEndpoint(endpoint)
	if err != nil {
		return nil, err
	}

	exporter, err := newOTLPExporter(normalizedEndpoint)
	if err != nil {
		return nil, fmt.Errorf("create otlp exporter: %w", err)
	}
	return exporter, nil
}

func otlpTraceEndpoint(rawEndpoint string) (string, error) {
	trimmed := strings.TrimSpace(rawEndpoint)
	if trimmed == "" {
		return "", fmt.Errorf("otlp endpoint is required")
	}

	if strings.Contains(trimmed, "://") {
		parsed, err := url.Parse(trimmed)
		if err != nil {
			return "", fmt.Errorf("parse otlp endpoint: %w", err)
		}
		if parsed.Host == "" {
			return "", fmt.Errorf("otlp endpoint host is required")
		}
		if parsed.Path == "" || parsed.Path == "/" {
			parsed.Path = "/v1/traces"
		}
		return parsed.String(), nil
	}

	parsed, err := url.Parse("http://" + trimmed)
	if err != nil {
		return "", fmt.Errorf("parse otlp endpoint: %w", err)
	}
	parsed.Path = "/v1/traces"
	return parsed.String(), nil
}

func newOTLPHTTPExporter(endpoint string) *otlpHTTPExporter {
	return &otlpHTTPExporter{
		endpoint: endpoint,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

type otlpHTTPExporter struct {
	endpoint string
	client   *http.Client
}

func (e *otlpHTTPExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if len(spans) == 0 {
		return nil
	}
	payload, err := json.Marshal(buildOTLPTraceRequest(spans))
	if err != nil {
		return fmt.Errorf("marshal otlp traces request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpoint, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build otlp request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	//nolint:gosec // Endpoint is validated during exporter construction and sourced from trusted config.
	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("send otlp request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if readErr != nil {
			return fmt.Errorf("otlp export failed status=%d body-read-error=%v", resp.StatusCode, readErr)
		}
		return fmt.Errorf(
			"otlp export failed status=%d body=%s",
			resp.StatusCode,
			strings.TrimSpace(string(body)),
		)
	}
	return nil
}

func (e *otlpHTTPExporter) Shutdown(context.Context) error {
	return nil
}

type otlpTraceRequest struct {
	ResourceSpans []otlpResourceSpans `json:"resourceSpans"`
}

type otlpResourceSpans struct {
	Resource   otlpResource    `json:"resource"`
	ScopeSpans []otlpScopeSpan `json:"scopeSpans"`
}

type otlpResource struct {
	Attributes []otlpKeyValue `json:"attributes,omitempty"`
}

type otlpScopeSpan struct {
	Scope otlpScope  `json:"scope"`
	Spans []otlpSpan `json:"spans"`
}

type otlpScope struct {
	Name    string `json:"name,omitempty"`
	Version string `json:"version,omitempty"`
}

type otlpSpan struct {
	TraceID           string         `json:"traceId"`
	SpanID            string         `json:"spanId"`
	ParentSpanID      string         `json:"parentSpanId,omitempty"`
	Name              string         `json:"name"`
	Kind              int            `json:"kind,omitempty"`
	StartTimeUnixNano string         `json:"startTimeUnixNano"`
	EndTimeUnixNano   string         `json:"endTimeUnixNano"`
	Attributes        []otlpKeyValue `json:"attributes,omitempty"`
	Status            otlpStatus     `json:"status"`
}

type otlpStatus struct {
	Code    int    `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

type otlpKeyValue struct {
	Key   string       `json:"key"`
	Value otlpAnyValue `json:"value"`
}

type otlpAnyValue struct {
	StringValue string  `json:"stringValue,omitempty"`
	IntValue    string  `json:"intValue,omitempty"`
	DoubleValue float64 `json:"doubleValue,omitempty"`
	BoolValue   bool    `json:"boolValue,omitempty"`
}

func buildOTLPTraceRequest(spans []sdktrace.ReadOnlySpan) otlpTraceRequest {
	resourceSpans := make([]otlpResourceSpans, 0, len(spans))
	for _, span := range spans {
		instrumentation := span.InstrumentationScope()
		traceSpan := otlpSpan{
			TraceID:           span.SpanContext().TraceID().String(),
			SpanID:            span.SpanContext().SpanID().String(),
			Name:              span.Name(),
			Kind:              int(span.SpanKind()),
			StartTimeUnixNano: strconv.FormatInt(span.StartTime().UnixNano(), 10),
			EndTimeUnixNano:   strconv.FormatInt(span.EndTime().UnixNano(), 10),
			Attributes:        toOTLPAttributes(span.Attributes()),
			Status: otlpStatus{
				Code:    int(span.Status().Code),
				Message: span.Status().Description,
			},
		}
		parentID := span.Parent().SpanID().String()
		if parentID != "0000000000000000" {
			traceSpan.ParentSpanID = parentID
		}

		resourceSpans = append(resourceSpans, otlpResourceSpans{
			Resource: otlpResource{
				Attributes: toOTLPAttributes(span.Resource().Attributes()),
			},
			ScopeSpans: []otlpScopeSpan{
				{
					Scope: otlpScope{
						Name:    instrumentation.Name,
						Version: instrumentation.Version,
					},
					Spans: []otlpSpan{traceSpan},
				},
			},
		})
	}
	return otlpTraceRequest{ResourceSpans: resourceSpans}
}

func toOTLPAttributes(attributes []attribute.KeyValue) []otlpKeyValue {
	if len(attributes) == 0 {
		return nil
	}
	result := make([]otlpKeyValue, 0, len(attributes))
	for _, item := range attributes {
		converted := otlpKeyValue{
			Key: string(item.Key),
			Value: otlpAnyValue{
				StringValue: item.Value.Emit(),
			},
		}
		switch item.Value.Type() {
		case attribute.BOOL:
			converted.Value = otlpAnyValue{BoolValue: item.Value.AsBool()}
		case attribute.INT64:
			converted.Value = otlpAnyValue{IntValue: strconv.FormatInt(item.Value.AsInt64(), 10)}
		case attribute.FLOAT64:
			converted.Value = otlpAnyValue{DoubleValue: item.Value.AsFloat64()}
		case attribute.STRING:
			converted.Value = otlpAnyValue{StringValue: item.Value.AsString()}
		}
		result = append(result, converted)
	}
	return result
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
