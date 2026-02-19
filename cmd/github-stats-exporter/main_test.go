package main

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"testing"

	"github.com/cam3ron2/github-stats-exporter/internal/config"
	"github.com/cam3ron2/github-stats-exporter/internal/leader"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestLogLevel(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		input string
		want  zapcore.Level
	}{
		{name: "debug", input: "debug", want: zapcore.DebugLevel},
		{name: "warn", input: "warn", want: zapcore.WarnLevel},
		{name: "error", input: "error", want: zapcore.ErrorLevel},
		{name: "default_info", input: "other", want: zapcore.InfoLevel},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := logLevel(tc.input)
			if got != tc.want {
				t.Fatalf("logLevel(%q) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}

func TestShouldIgnoreLoggerSyncError(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil_error",
			err:  nil,
			want: false,
		},
		{
			name: "einval_direct",
			err:  syscall.EINVAL,
			want: true,
		},
		{
			name: "enotty_direct",
			err:  syscall.ENOTTY,
			want: true,
		},
		{
			name: "wrapped_einval",
			err:  fmt.Errorf("wrapped: %w", syscall.EINVAL),
			want: true,
		},
		{
			name: "other_error",
			err:  errors.New("boom"),
			want: false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := shouldIgnoreLoggerSyncError(tc.err)
			if got != tc.want {
				t.Fatalf("shouldIgnoreLoggerSyncError(%v) = %t, want %t", tc.err, got, tc.want)
			}
		})
	}
}

func TestBuildElector(t *testing.T) {
	testCases := []struct {
		name        string
		cfg         *config.Config
		initialRole string
		wantLeader  bool
	}{
		{
			name:       "nil_config_defaults_to_leader",
			cfg:        nil,
			wantLeader: true,
		},
		{
			name: "leader_election_disabled_follower",
			cfg: &config.Config{
				LeaderElection: config.LeaderElectionConfig{Enabled: false},
			},
			initialRole: "follower",
			wantLeader:  false,
		},
		{
			name: "leader_election_enabled_without_cluster_falls_back_to_leader",
			cfg: &config.Config{
				LeaderElection: config.LeaderElectionConfig{
					Enabled:   true,
					Namespace: "github-stats-exporter",
					LeaseName: "leader",
				},
			},
			wantLeader: true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			previousRole := os.Getenv("GITHUB_STATS_INITIAL_ROLE")
			if tc.initialRole == "" {
				_ = os.Unsetenv("GITHUB_STATS_INITIAL_ROLE")
			} else {
				_ = os.Setenv("GITHUB_STATS_INITIAL_ROLE", tc.initialRole)
			}
			t.Cleanup(func() {
				if previousRole == "" {
					_ = os.Unsetenv("GITHUB_STATS_INITIAL_ROLE")
				} else {
					_ = os.Setenv("GITHUB_STATS_INITIAL_ROLE", previousRole)
				}
			})

			elector, err := buildElector(tc.cfg, zap.NewNop())
			if err != nil {
				t.Fatalf("buildElector() unexpected error: %v", err)
			}
			staticElector, ok := elector.(leader.StaticElector)
			if !ok {
				t.Fatalf("elector type = %T, want leader.StaticElector in this test", elector)
			}
			if staticElector.IsLeader != tc.wantLeader {
				t.Fatalf("StaticElector.IsLeader = %t, want %t", staticElector.IsLeader, tc.wantLeader)
			}
		})
	}
}

func TestInClusterKubernetesClientMissingEnvironment(t *testing.T) {
	originalHost := os.Getenv("KUBERNETES_SERVICE_HOST")
	originalPort := os.Getenv("KUBERNETES_SERVICE_PORT")
	_ = os.Unsetenv("KUBERNETES_SERVICE_HOST")
	_ = os.Unsetenv("KUBERNETES_SERVICE_PORT")
	t.Cleanup(func() {
		if originalHost == "" {
			_ = os.Unsetenv("KUBERNETES_SERVICE_HOST")
		} else {
			_ = os.Setenv("KUBERNETES_SERVICE_HOST", originalHost)
		}
		if originalPort == "" {
			_ = os.Unsetenv("KUBERNETES_SERVICE_PORT")
		} else {
			_ = os.Setenv("KUBERNETES_SERVICE_PORT", originalPort)
		}
	})

	_, _, _, err := inClusterKubernetesClient()
	if err == nil {
		t.Fatalf("inClusterKubernetesClient() expected error, got nil")
	}
}
