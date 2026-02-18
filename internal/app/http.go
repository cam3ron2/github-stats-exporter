package app

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// NewHTTPHandler wires metrics and health endpoints on a single mux.
func NewHTTPHandler(metricsHandler http.Handler, healthHandler http.Handler) http.Handler {
	router := chi.NewRouter()
	router.Handle("/metrics", metricsHandler)
	router.Handle("/livez", healthHandler)
	router.Handle("/readyz", healthHandler)
	router.Handle("/healthz", healthHandler)
	return router
}
