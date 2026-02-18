package app

import (
	"net/http"
	"strings"

	"github.com/cam3ron2/github-stats/internal/telemetry"
	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// NewHTTPHandler wires metrics and health endpoints on a single mux.
func NewHTTPHandler(metricsHandler http.Handler, healthHandler http.Handler) http.Handler {
	router := chi.NewRouter()
	traceMode := telemetry.TraceMode()
	router.Handle("/metrics", wrapHTTPHandler(traceMode, "metrics", metricsHandler))
	router.Handle("/livez", wrapHTTPHandler(traceMode, "livez", healthHandler))
	router.Handle("/readyz", wrapHTTPHandler(traceMode, "readyz", healthHandler))
	router.Handle("/healthz", wrapHTTPHandler(traceMode, "healthz", healthHandler))
	return router
}

func wrapHTTPHandler(traceMode, route string, handler http.Handler) http.Handler {
	if handler == nil {
		handler = http.NotFoundHandler()
	}
	if strings.EqualFold(strings.TrimSpace(traceMode), "off") {
		return handler
	}

	operation := strings.TrimSpace(route)
	if operation == "" {
		operation = "handler"
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := otel.Tracer("github-stats/internal/app").Start(
			r.Context(),
			"http.server."+operation,
			trace.WithAttributes(
				attribute.String("http.method", r.Method),
				attribute.String("http.target", r.URL.Path),
			),
		)
		defer span.End()

		recorder := &statusCapturingResponseWriter{
			ResponseWriter: w,
			status:         http.StatusOK,
		}
		handler.ServeHTTP(recorder, r.WithContext(ctx))
		span.SetAttributes(attribute.Int("http.status_code", recorder.status))
		if recorder.status >= http.StatusInternalServerError {
			span.SetStatus(codes.Error, http.StatusText(recorder.status))
			return
		}
		span.SetStatus(codes.Ok, "request completed")
	})
}

type statusCapturingResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusCapturingResponseWriter) WriteHeader(statusCode int) {
	w.status = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}
