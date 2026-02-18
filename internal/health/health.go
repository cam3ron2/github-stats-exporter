package health

import (
	"context"
	"encoding/json"
	"net/http"
)

// Role identifies the runtime role for readiness evaluation.
type Role string

const (
	// RoleLeader is the leader role.
	RoleLeader Role = "leader"
	// RoleFollower is the follower role.
	RoleFollower Role = "follower"
)

// Mode indicates high-level health mode.
type Mode string

const (
	// ModeHealthy indicates all required dependencies are healthy.
	ModeHealthy Mode = "healthy"
	// ModeDegraded indicates the app is running but a non-readiness dependency is degraded.
	ModeDegraded Mode = "degraded"
	// ModeUnhealthy indicates a required dependency is unhealthy.
	ModeUnhealthy Mode = "unhealthy"
)

// Input represents dependency states used for health evaluation.
type Input struct {
	Role               Role
	RedisHealthy       bool
	AMQPHealthy        bool
	SchedulerHealthy   bool
	GitHubClientUsable bool
	ConsumerHealthy    bool
	ExporterHealthy    bool
	GitHubHealthy      bool
}

// Status represents evaluated application health.
type Status struct {
	Role       Role            `json:"role"`
	Mode       Mode            `json:"mode"`
	Ready      bool            `json:"ready"`
	Components map[string]bool `json:"components"`
}

// Provider supplies current health status.
type Provider interface {
	CurrentStatus(ctx context.Context) Status
}

// StatusEvaluator evaluates role-aware health and readiness.
type StatusEvaluator struct{}

// NewStatusEvaluator creates a health evaluator.
func NewStatusEvaluator() *StatusEvaluator {
	return &StatusEvaluator{}
}

// Evaluate evaluates readiness and mode from dependency state.
func (e *StatusEvaluator) Evaluate(input Input) Status {
	components := map[string]bool{
		"redis":          input.RedisHealthy,
		"amqp":           input.AMQPHealthy,
		"scheduler":      input.SchedulerHealthy,
		"github_client":  input.GitHubClientUsable,
		"consumer":       input.ConsumerHealthy,
		"exporter_cache": input.ExporterHealthy,
		"github_healthy": input.GitHubHealthy,
	}

	ready := input.RedisHealthy && input.AMQPHealthy && input.ExporterHealthy
	if input.Role == RoleLeader {
		ready = ready && input.SchedulerHealthy && input.GitHubClientUsable
	} else {
		ready = ready && input.ConsumerHealthy
	}

	mode := ModeHealthy
	if !ready {
		mode = ModeUnhealthy
	} else if !input.GitHubHealthy {
		mode = ModeDegraded
	}

	return Status{
		Role:       input.Role,
		Mode:       mode,
		Ready:      ready,
		Components: components,
	}
}

// NewHandler returns the health HTTP handler with /livez, /readyz, and /healthz endpoints.
func NewHandler(provider Provider) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("ok")); err != nil {
			return
		}
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		status := provider.CurrentStatus(r.Context())
		if status.Ready {
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte("ready")); err != nil {
				return
			}
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		if _, err := w.Write([]byte("not ready")); err != nil {
			return
		}
	})

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		status := provider.CurrentStatus(r.Context())
		payload, err := json.Marshal(status)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			if _, writeErr := w.Write([]byte(`{"mode":"unhealthy","error":"marshal health status"}`)); writeErr != nil {
				return
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		//nolint:gosec // Health payload is server-generated JSON status.
		if _, err := w.Write(payload); err != nil {
			return
		}
	})

	return mux
}
