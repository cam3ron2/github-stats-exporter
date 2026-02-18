package health

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStatusEvaluatorEvaluate(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		input     Input
		wantReady bool
		wantMode  Mode
	}{
		{
			name: "leader_healthy",
			input: Input{
				Role:               RoleLeader,
				RedisHealthy:       true,
				AMQPHealthy:        true,
				SchedulerHealthy:   true,
				GitHubClientUsable: true,
				ConsumerHealthy:    false,
				ExporterHealthy:    true,
				GitHubHealthy:      true,
			},
			wantReady: true,
			wantMode:  ModeHealthy,
		},
		{
			name: "leader_github_degraded_but_usable",
			input: Input{
				Role:               RoleLeader,
				RedisHealthy:       true,
				AMQPHealthy:        true,
				SchedulerHealthy:   true,
				GitHubClientUsable: true,
				ConsumerHealthy:    false,
				ExporterHealthy:    true,
				GitHubHealthy:      false,
			},
			wantReady: true,
			wantMode:  ModeDegraded,
		},
		{
			name: "leader_not_ready_when_scheduler_unhealthy",
			input: Input{
				Role:               RoleLeader,
				RedisHealthy:       true,
				AMQPHealthy:        true,
				SchedulerHealthy:   false,
				GitHubClientUsable: true,
				ConsumerHealthy:    true,
				ExporterHealthy:    true,
				GitHubHealthy:      true,
			},
			wantReady: false,
			wantMode:  ModeUnhealthy,
		},
		{
			name: "leader_not_ready_when_github_client_not_usable",
			input: Input{
				Role:               RoleLeader,
				RedisHealthy:       true,
				AMQPHealthy:        true,
				SchedulerHealthy:   true,
				GitHubClientUsable: false,
				ConsumerHealthy:    true,
				ExporterHealthy:    true,
				GitHubHealthy:      false,
			},
			wantReady: false,
			wantMode:  ModeUnhealthy,
		},
		{
			name: "follower_healthy",
			input: Input{
				Role:               RoleFollower,
				RedisHealthy:       true,
				AMQPHealthy:        true,
				SchedulerHealthy:   false,
				GitHubClientUsable: false,
				ConsumerHealthy:    true,
				ExporterHealthy:    true,
				GitHubHealthy:      false,
			},
			wantReady: true,
			wantMode:  ModeDegraded,
		},
		{
			name: "follower_not_ready_when_consumer_unhealthy",
			input: Input{
				Role:               RoleFollower,
				RedisHealthy:       true,
				AMQPHealthy:        true,
				SchedulerHealthy:   true,
				GitHubClientUsable: true,
				ConsumerHealthy:    false,
				ExporterHealthy:    true,
				GitHubHealthy:      true,
			},
			wantReady: false,
			wantMode:  ModeUnhealthy,
		},
		{
			name: "follower_not_ready_when_exporter_cache_unhealthy",
			input: Input{
				Role:               RoleFollower,
				RedisHealthy:       true,
				AMQPHealthy:        true,
				SchedulerHealthy:   true,
				GitHubClientUsable: true,
				ConsumerHealthy:    true,
				ExporterHealthy:    false,
				GitHubHealthy:      true,
			},
			wantReady: false,
			wantMode:  ModeUnhealthy,
		},
	}

	evaluator := NewStatusEvaluator()
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := evaluator.Evaluate(tc.input)
			if got.Ready != tc.wantReady {
				t.Fatalf("Evaluate().Ready = %t, want %t", got.Ready, tc.wantReady)
			}
			if got.Mode != tc.wantMode {
				t.Fatalf("Evaluate().Mode = %q, want %q", got.Mode, tc.wantMode)
			}
		})
	}
}

type staticProvider struct {
	status Status
}

func (s *staticProvider) CurrentStatus(_ context.Context) Status {
	return s.status
}

func TestHandler(t *testing.T) {
	t.Parallel()

	evaluator := NewStatusEvaluator()
	healthyStatus := evaluator.Evaluate(Input{
		Role:               RoleLeader,
		RedisHealthy:       true,
		AMQPHealthy:        true,
		SchedulerHealthy:   true,
		GitHubClientUsable: true,
		ConsumerHealthy:    false,
		ExporterHealthy:    true,
		GitHubHealthy:      true,
	})
	unhealthyStatus := evaluator.Evaluate(Input{
		Role:               RoleFollower,
		RedisHealthy:       true,
		AMQPHealthy:        false,
		SchedulerHealthy:   true,
		GitHubClientUsable: true,
		ConsumerHealthy:    true,
		ExporterHealthy:    true,
		GitHubHealthy:      true,
	})

	testCases := []struct {
		name       string
		status     Status
		path       string
		wantCode   int
		wantSubstr []string
	}{
		{
			name:       "livez_always_ok",
			status:     unhealthyStatus,
			path:       "/livez",
			wantCode:   http.StatusOK,
			wantSubstr: []string{"ok"},
		},
		{
			name:       "readyz_healthy",
			status:     healthyStatus,
			path:       "/readyz",
			wantCode:   http.StatusOK,
			wantSubstr: []string{"ready"},
		},
		{
			name:       "readyz_unhealthy",
			status:     unhealthyStatus,
			path:       "/readyz",
			wantCode:   http.StatusServiceUnavailable,
			wantSubstr: []string{"not ready"},
		},
		{
			name:       "healthz_json_contains_mode_and_role",
			status:     healthyStatus,
			path:       "/healthz",
			wantCode:   http.StatusOK,
			wantSubstr: []string{"mode", "role", "components"},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			handler := NewHandler(&staticProvider{status: tc.status})
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != tc.wantCode {
				t.Fatalf("status code = %d, want %d", rec.Code, tc.wantCode)
			}
			body := rec.Body.String()
			for _, substr := range tc.wantSubstr {
				if !strings.Contains(body, substr) {
					t.Fatalf("body %q missing %q", body, substr)
				}
			}

			if tc.path == "/healthz" {
				var parsed map[string]any
				if err := json.Unmarshal(rec.Body.Bytes(), &parsed); err != nil {
					t.Fatalf("healthz body is not valid json: %v", err)
				}
			}
		})
	}
}
