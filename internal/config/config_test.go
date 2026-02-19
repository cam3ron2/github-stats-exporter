package config

import (
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

func TestLoadAndValidate(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		yaml       string
		wantErr    bool
		errSubstrs []string
	}{
		{
			name: "valid_minimal_configuration",
			yaml: `
server:
  listen_addr: ":8080"
  log_level: "info"
metrics:
  topology: "single_service_target"
  scrape_service_dns: "github-stats-exporter-metrics.github-stats-exporter.svc.cluster.local:8080"
leader_election:
  enabled: true
  namespace: "github-stats-exporter"
  lease_name: "github-stats-exporter-leader"
  lease_duration: "30s"
  renew_deadline: "20s"
  retry_period: "5s"
github:
  api_base_url: "https://api.github.com"
  request_timeout: "20s"
  unhealthy_failure_threshold: 6
  unhealthy_cooldown: "2m"
  orgs:
    - org: "org-a"
      app_id: 111111
      installation_id: 222222
      private_key_path: "/etc/github-stats-exporter/keys/org-a.pem"
      scrape_interval: "5m"
      repo_allowlist: ["*"]
      per_org_concurrency: 8
rate_limit:
  min_remaining_threshold: 200
  min_reset_buffer: "10s"
  secondary_limit_backoff: "60s"
retry:
  max_attempts: 7
  initial_backoff: "2s"
  max_backoff: "2m"
  jitter: true
loc:
  source: "stats_contributors"
  refresh_interval: "24h"
  fallback_enabled: true
  fallback_max_commits_per_repo_per_week: 500
  fallback_max_commit_detail_calls_per_org_per_hour: 3000
  large_repo_zero_detection_windows: 2
  large_repo_cooldown: "7d"
backfill:
  enabled: true
  max_message_age: "24h"
  consumer_count: 6
  requeue_delays: ["1m", "5m", "30m", "2h"]
  coalesce_window: "15m"
  dedup_ttl: "12h"
  max_enqueues_per_org_per_minute: 200
amqp:
  url: "amqp://githubstats:githubstats@rabbitmq:5672/"
  exchange: "gh.backfill"
  queue: "gh.backfill.jobs"
  dlq: "gh.backfill.dlq"
store:
  backend: "redis"
  redis_mode: "standalone"
  redis_addr: "redis:6379"
  redis_master_set: "mymaster"
  redis_sentinel_addrs: []
  redis_password: ""
  redis_db: 0
  retention: "30d"
  metric_refresh_interval: "30s"
  index_shards: 128
  export_cache_mode: "incremental"
  max_series_budget: 10000000
health:
  github_probe_interval: "30s"
  github_recover_success_threshold: 3
telemetry:
  otel_enabled: false
  otel_exporter_otlp_endpoint: ""
  otel_trace_mode: "off"
  otel_trace_sample_ratio: 0.05
`,
		},
		{
			name: "invalid_log_level",
			yaml: `
server:
  listen_addr: ":8080"
  log_level: "verbose"
github:
  api_base_url: "https://api.github.com"
  request_timeout: "20s"
  unhealthy_failure_threshold: 6
  unhealthy_cooldown: "2m"
  orgs:
    - org: "org-a"
      app_id: 1
      installation_id: 2
      private_key_path: "/tmp/org-a.pem"
      scrape_interval: "5m"
rate_limit:
  min_remaining_threshold: 200
  min_reset_buffer: "10s"
  secondary_limit_backoff: "60s"
retry:
  max_attempts: 7
  initial_backoff: "2s"
  max_backoff: "2m"
  jitter: true
loc:
  source: "stats_contributors"
  refresh_interval: "24h"
  fallback_enabled: true
  fallback_max_commits_per_repo_per_week: 100
  fallback_max_commit_detail_calls_per_org_per_hour: 1000
  large_repo_zero_detection_windows: 2
  large_repo_cooldown: "7d"
backfill:
  enabled: true
  max_message_age: "24h"
  consumer_count: 1
  requeue_delays: ["1m"]
  coalesce_window: "15m"
  dedup_ttl: "12h"
  max_enqueues_per_org_per_minute: 10
amqp:
  url: "amqp://guest:guest@localhost:5672/"
  exchange: "gh.backfill"
  queue: "gh.backfill.jobs"
  dlq: "gh.backfill.dlq"
store:
  backend: "redis"
  redis_mode: "standalone"
  redis_addr: "redis:6379"
  redis_master_set: "mymaster"
  redis_sentinel_addrs: []
  retention: "1d"
  metric_refresh_interval: "30s"
  index_shards: 16
  export_cache_mode: "incremental"
  max_series_budget: 1000
`,
			wantErr:    true,
			errSubstrs: []string{"server.log_level", "debug|info|warn|error"},
		},
		{
			name: "missing_github_orgs",
			yaml: `
server:
  listen_addr: ":8080"
  log_level: "info"
github:
  api_base_url: "https://api.github.com"
  request_timeout: "20s"
  unhealthy_failure_threshold: 6
  unhealthy_cooldown: "2m"
  orgs: []
rate_limit:
  min_remaining_threshold: 200
  min_reset_buffer: "10s"
  secondary_limit_backoff: "60s"
retry:
  max_attempts: 7
  initial_backoff: "2s"
  max_backoff: "2m"
  jitter: true
loc:
  source: "stats_contributors"
  refresh_interval: "24h"
  fallback_enabled: false
  fallback_max_commits_per_repo_per_week: 100
  fallback_max_commit_detail_calls_per_org_per_hour: 1000
  large_repo_zero_detection_windows: 2
  large_repo_cooldown: "7d"
backfill:
  enabled: true
  max_message_age: "24h"
  consumer_count: 1
  requeue_delays: ["1m"]
  coalesce_window: "15m"
  dedup_ttl: "12h"
  max_enqueues_per_org_per_minute: 10
amqp:
  url: "amqp://guest:guest@localhost:5672/"
  exchange: "gh.backfill"
  queue: "gh.backfill.jobs"
  dlq: "gh.backfill.dlq"
store:
  backend: "redis"
  redis_mode: "standalone"
  redis_addr: "redis:6379"
  redis_master_set: "mymaster"
  redis_sentinel_addrs: []
  retention: "1d"
  metric_refresh_interval: "30s"
  index_shards: 16
  export_cache_mode: "incremental"
  max_series_budget: 1000
`,
			wantErr:    true,
			errSubstrs: []string{"github.orgs", "at least one"},
		},
		{
			name: "duplicate_org_names",
			yaml: `
server:
  listen_addr: ":8080"
  log_level: "info"
github:
  api_base_url: "https://api.github.com"
  request_timeout: "20s"
  unhealthy_failure_threshold: 6
  unhealthy_cooldown: "2m"
  orgs:
    - org: "org-a"
      app_id: 1
      installation_id: 2
      private_key_path: "/tmp/a.pem"
      scrape_interval: "5m"
    - org: "org-a"
      app_id: 3
      installation_id: 4
      private_key_path: "/tmp/b.pem"
      scrape_interval: "5m"
rate_limit:
  min_remaining_threshold: 200
  min_reset_buffer: "10s"
  secondary_limit_backoff: "60s"
retry:
  max_attempts: 7
  initial_backoff: "2s"
  max_backoff: "2m"
  jitter: true
loc:
  source: "stats_contributors"
  refresh_interval: "24h"
  fallback_enabled: false
  fallback_max_commits_per_repo_per_week: 100
  fallback_max_commit_detail_calls_per_org_per_hour: 1000
  large_repo_zero_detection_windows: 2
  large_repo_cooldown: "7d"
backfill:
  enabled: true
  max_message_age: "24h"
  consumer_count: 1
  requeue_delays: ["1m"]
  coalesce_window: "15m"
  dedup_ttl: "12h"
  max_enqueues_per_org_per_minute: 10
amqp:
  url: "amqp://guest:guest@localhost:5672/"
  exchange: "gh.backfill"
  queue: "gh.backfill.jobs"
  dlq: "gh.backfill.dlq"
store:
  backend: "redis"
  redis_mode: "standalone"
  redis_addr: "redis:6379"
  redis_master_set: "mymaster"
  redis_sentinel_addrs: []
  retention: "1d"
  metric_refresh_interval: "30s"
  index_shards: 16
  export_cache_mode: "incremental"
  max_series_budget: 1000
`,
			wantErr:    true,
			errSubstrs: []string{"github.orgs", "duplicate"},
		},
		{
			name: "sentinel_mode_requires_sentinel_addrs",
			yaml: `
server:
  listen_addr: ":8080"
  log_level: "info"
github:
  api_base_url: "https://api.github.com"
  request_timeout: "20s"
  unhealthy_failure_threshold: 6
  unhealthy_cooldown: "2m"
  orgs:
    - org: "org-a"
      app_id: 1
      installation_id: 2
      private_key_path: "/tmp/a.pem"
      scrape_interval: "5m"
rate_limit:
  min_remaining_threshold: 200
  min_reset_buffer: "10s"
  secondary_limit_backoff: "60s"
retry:
  max_attempts: 7
  initial_backoff: "2s"
  max_backoff: "2m"
  jitter: true
loc:
  source: "stats_contributors"
  refresh_interval: "24h"
  fallback_enabled: false
  fallback_max_commits_per_repo_per_week: 100
  fallback_max_commit_detail_calls_per_org_per_hour: 1000
  large_repo_zero_detection_windows: 2
  large_repo_cooldown: "7d"
backfill:
  enabled: true
  max_message_age: "24h"
  consumer_count: 1
  requeue_delays: ["1m"]
  coalesce_window: "15m"
  dedup_ttl: "12h"
  max_enqueues_per_org_per_minute: 10
amqp:
  url: "amqp://guest:guest@localhost:5672/"
  exchange: "gh.backfill"
  queue: "gh.backfill.jobs"
  dlq: "gh.backfill.dlq"
store:
  backend: "redis"
  redis_mode: "sentinel"
  redis_addr: ""
  redis_master_set: "mymaster"
  redis_sentinel_addrs: []
  retention: "1d"
  metric_refresh_interval: "30s"
  index_shards: 16
  export_cache_mode: "incremental"
  max_series_budget: 1000
`,
			wantErr:    true,
			errSubstrs: []string{"store.redis_sentinel_addrs", "required"},
		},
		{
			name: "loc_fallback_enabled_requires_budget",
			yaml: `
server:
  listen_addr: ":8080"
  log_level: "info"
github:
  api_base_url: "https://api.github.com"
  request_timeout: "20s"
  unhealthy_failure_threshold: 6
  unhealthy_cooldown: "2m"
  orgs:
    - org: "org-a"
      app_id: 1
      installation_id: 2
      private_key_path: "/tmp/a.pem"
      scrape_interval: "5m"
rate_limit:
  min_remaining_threshold: 200
  min_reset_buffer: "10s"
  secondary_limit_backoff: "60s"
retry:
  max_attempts: 7
  initial_backoff: "2s"
  max_backoff: "2m"
  jitter: true
loc:
  source: "stats_contributors"
  refresh_interval: "24h"
  fallback_enabled: true
  fallback_max_commits_per_repo_per_week: 0
  fallback_max_commit_detail_calls_per_org_per_hour: 0
  large_repo_zero_detection_windows: 2
  large_repo_cooldown: "7d"
backfill:
  enabled: true
  max_message_age: "24h"
  consumer_count: 1
  requeue_delays: ["1m"]
  coalesce_window: "15m"
  dedup_ttl: "12h"
  max_enqueues_per_org_per_minute: 10
amqp:
  url: "amqp://guest:guest@localhost:5672/"
  exchange: "gh.backfill"
  queue: "gh.backfill.jobs"
  dlq: "gh.backfill.dlq"
store:
  backend: "redis"
  redis_mode: "standalone"
  redis_addr: "redis:6379"
  redis_master_set: "mymaster"
  redis_sentinel_addrs: []
  retention: "1d"
  metric_refresh_interval: "30s"
  index_shards: 16
  export_cache_mode: "incremental"
  max_series_budget: 1000
`,
			wantErr: true,
			errSubstrs: []string{
				"loc.fallback_max_commits_per_repo_per_week",
				"loc.fallback_max_commit_detail_calls_per_org_per_hour",
			},
		},
		{
			name: "invalid_metrics_topology",
			yaml: `
server:
  listen_addr: ":8080"
  log_level: "info"
metrics:
  topology: "pod_discovery"
  scrape_service_dns: ""
github:
  api_base_url: "https://api.github.com"
  request_timeout: "20s"
  unhealthy_failure_threshold: 6
  unhealthy_cooldown: "2m"
  orgs:
    - org: "org-a"
      app_id: 1
      installation_id: 2
      private_key_path: "/tmp/a.pem"
      scrape_interval: "5m"
rate_limit:
  min_remaining_threshold: 200
  min_reset_buffer: "10s"
  secondary_limit_backoff: "60s"
retry:
  max_attempts: 7
  initial_backoff: "2s"
  max_backoff: "2m"
  jitter: true
loc:
  source: "stats_contributors"
  refresh_interval: "24h"
  fallback_enabled: false
  fallback_max_commits_per_repo_per_week: 100
  fallback_max_commit_detail_calls_per_org_per_hour: 1000
  large_repo_zero_detection_windows: 2
  large_repo_cooldown: "7d"
backfill:
  enabled: true
  max_message_age: "24h"
  consumer_count: 1
  requeue_delays: ["1m"]
  coalesce_window: "15m"
  dedup_ttl: "12h"
  max_enqueues_per_org_per_minute: 10
amqp:
  url: "amqp://guest:guest@localhost:5672/"
  exchange: "gh.backfill"
  queue: "gh.backfill.jobs"
  dlq: "gh.backfill.dlq"
store:
  backend: "redis"
  redis_mode: "standalone"
  redis_addr: "redis:6379"
  redis_master_set: "mymaster"
  redis_sentinel_addrs: []
  retention: "1d"
  metric_refresh_interval: "30s"
  index_shards: 16
  export_cache_mode: "incremental"
  max_series_budget: 1000
`,
			wantErr:    true,
			errSubstrs: []string{"metrics.topology", "single_service_target"},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := Load(strings.NewReader(tc.yaml))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Load() expected error, got nil")
				}
				for _, substr := range tc.errSubstrs {
					if !strings.Contains(err.Error(), substr) {
						t.Fatalf("Load() error = %q, missing substring %q", err.Error(), substr)
					}
				}
				return
			}

			if err != nil {
				t.Fatalf("Load() unexpected error: %v", err)
			}
			if cfg == nil {
				t.Fatalf("Load() returned nil config")
			}
		})
	}
}

func TestLoadAdditionalBehaviors(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		reader      io.Reader
		wantErr     bool
		errContains string
		assert      func(t *testing.T, cfg *Config)
	}{
		{
			name:        "nil_reader_returns_error",
			reader:      nil,
			wantErr:     true,
			errContains: "config reader is nil",
		},
		{
			name:        "invalid_yaml_returns_parse_error",
			reader:      strings.NewReader("server: [oops"),
			wantErr:     true,
			errContains: "unmarshal yaml",
		},
		{
			name: "applies_defaults_and_parses_day_duration",
			reader: strings.NewReader(`
server:
  listen_addr: ":8080"
github:
  api_base_url: "https://api.github.com"
  request_timeout: "20s"
  unhealthy_failure_threshold: 1
  unhealthy_cooldown: "2m"
  orgs:
    - org: "org-a"
      app_id: 1
      installation_id: 2
      private_key_path: "/tmp/key.pem"
      scrape_interval: "5m"
rate_limit:
  min_remaining_threshold: 1
  min_reset_buffer: "1s"
  secondary_limit_backoff: "1s"
retry:
  max_attempts: 1
  initial_backoff: "1s"
  max_backoff: "1s"
loc:
  source: "stats_contributors"
  refresh_interval: "24h"
  fallback_enabled: false
  fallback_max_commits_per_repo_per_week: 1
  fallback_max_commit_detail_calls_per_org_per_hour: 1
  large_repo_zero_detection_windows: 1
  large_repo_cooldown: "7d"
backfill:
  enabled: true
  max_message_age: "24h"
  consumer_count: 1
  requeue_delays: ["1m"]
  coalesce_window: "15m"
  dedup_ttl: "1h"
  max_enqueues_per_org_per_minute: 1
amqp:
  url: "amqp://guest:guest@localhost:5672/"
  exchange: "gh.backfill"
  queue: "gh.backfill.jobs"
  dlq: "gh.backfill.dlq"
store:
  redis_addr: "redis:6379"
  retention: "30d"
  metric_refresh_interval: "30s"
  index_shards: 1
  max_series_budget: 100
`),
			assert: func(t *testing.T, cfg *Config) {
				t.Helper()
				if cfg.Server.LogLevel != "info" {
					t.Fatalf("Server.LogLevel = %q, want info", cfg.Server.LogLevel)
				}
				if cfg.Metrics.Topology != "single_service_target" {
					t.Fatalf("Metrics.Topology = %q, want single_service_target", cfg.Metrics.Topology)
				}
				if cfg.Store.Backend != "redis" {
					t.Fatalf("Store.Backend = %q, want redis", cfg.Store.Backend)
				}
				if cfg.Store.RedisMode != "standalone" {
					t.Fatalf("Store.RedisMode = %q, want standalone", cfg.Store.RedisMode)
				}
				if cfg.Store.ExportCacheMode != "incremental" {
					t.Fatalf("Store.ExportCacheMode = %q, want incremental", cfg.Store.ExportCacheMode)
				}
				if cfg.LOC.LargeRepoCooldown != 7*24*time.Hour {
					t.Fatalf("LOC.LargeRepoCooldown = %s, want %s", cfg.LOC.LargeRepoCooldown, 7*24*time.Hour)
				}
				if cfg.Store.Retention != 30*24*time.Hour {
					t.Fatalf("Store.Retention = %s, want %s", cfg.Store.Retention, 30*24*time.Hour)
				}
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := Load(tc.reader)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Load() expected error, got nil")
				}
				if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
					t.Fatalf("Load() error = %q, missing %q", err.Error(), tc.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() unexpected error: %v", err)
			}
			if tc.assert != nil {
				tc.assert(t, cfg)
			}
		})
	}
}

func TestLoadCopilotConfig(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		copilotYAML   string
		wantErr       bool
		errSubstrings []string
		assert        func(t *testing.T, cfg *Config)
	}{
		{
			name:        "missing_copilot_uses_defaults",
			copilotYAML: "",
			assert: func(t *testing.T, cfg *Config) {
				t.Helper()
				if cfg.Copilot.Enabled {
					t.Fatalf("Copilot.Enabled = true, want false")
				}
				if !cfg.Copilot.IncludeOrg28d {
					t.Fatalf("Copilot.IncludeOrg28d = false, want true")
				}
				if cfg.Copilot.UserLabelMode != "login" {
					t.Fatalf(
						"Copilot.UserLabelMode = %q, want login",
						cfg.Copilot.UserLabelMode,
					)
				}
			},
		},
		{
			name: "enabled_org_only",
			copilotYAML: `
copilot:
  enabled: true
  scrape_interval: "6h"
  request_timeout: "30s"
  download_timeout: "90s"
  include_org_28d: true
  include_org_users_28d: true
  include_enterprise_28d: false
  include_enterprise_users_28d: false
  include_breakdown_ide: false
  include_breakdown_feature: false
  include_breakdown_language: false
  include_breakdown_model: false
  include_pull_request_activity: true
  user_label_mode: "hashed"
  emit_day_label: true
  max_records_per_report: 20000
  max_users_per_report: 5000
  refresh_if_report_unchanged: true
  enterprise:
    enabled: false
    slug: ""
    app_id: 0
    installation_id: 0
    private_key_path: ""
`,
			assert: func(t *testing.T, cfg *Config) {
				t.Helper()
				if !cfg.Copilot.Enabled {
					t.Fatalf("Copilot.Enabled = false, want true")
				}
				if cfg.Copilot.ScrapeInterval != 6*time.Hour {
					t.Fatalf(
						"Copilot.ScrapeInterval = %s, want %s",
						cfg.Copilot.ScrapeInterval,
						6*time.Hour,
					)
				}
				if cfg.Copilot.UserLabelMode != "hashed" {
					t.Fatalf(
						"Copilot.UserLabelMode = %q, want hashed",
						cfg.Copilot.UserLabelMode,
					)
				}
				if !cfg.Copilot.EmitDayLabel {
					t.Fatalf("Copilot.EmitDayLabel = false, want true")
				}
				if !cfg.Copilot.RefreshIfReportUnchanged {
					t.Fatalf(
						"Copilot.RefreshIfReportUnchanged = false, want true",
					)
				}
			},
		},
		{
			name: "enterprise_disabled_coerces_enterprise_flags",
			copilotYAML: `
copilot:
  enabled: true
  include_enterprise_28d: true
  include_enterprise_users_28d: true
  enterprise:
    enabled: false
`,
			assert: func(t *testing.T, cfg *Config) {
				t.Helper()
				if cfg.Copilot.IncludeEnterprise28d {
					t.Fatalf(
						"Copilot.IncludeEnterprise28d = true, want false",
					)
				}
				if cfg.Copilot.IncludeEnterpriseUsers28d {
					t.Fatalf(
						"Copilot.IncludeEnterpriseUsers28d = true, want false",
					)
				}
			},
		},
		{
			name: "enterprise_enabled_requires_credentials",
			copilotYAML: `
copilot:
  enabled: true
  include_enterprise_28d: true
  enterprise:
    enabled: true
`,
			wantErr: true,
			errSubstrings: []string{
				"copilot.enterprise.slug",
				"copilot.enterprise.app_id",
				"copilot.enterprise.installation_id",
				"copilot.enterprise.private_key_path",
			},
		},
		{
			name: "invalid_user_label_mode",
			copilotYAML: `
copilot:
  enabled: true
  user_label_mode: "email"
`,
			wantErr:       true,
			errSubstrings: []string{"copilot.user_label_mode"},
		},
		{
			name: "negative_limits_rejected",
			copilotYAML: `
copilot:
  enabled: true
  max_records_per_report: -1
  max_users_per_report: -1
`,
			wantErr: true,
			errSubstrings: []string{
				"copilot.max_records_per_report",
				"copilot.max_users_per_report",
			},
		},
		{
			name: "disabled_copilot_does_not_require_enterprise_credentials",
			copilotYAML: `
copilot:
  enabled: false
  enterprise:
    enabled: true
`,
			assert: func(t *testing.T, cfg *Config) {
				t.Helper()
				if cfg.Copilot.Enabled {
					t.Fatalf("Copilot.Enabled = true, want false")
				}
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := Load(strings.NewReader(baseConfigYAML(tc.copilotYAML)))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Load() expected error, got nil")
				}
				for _, expected := range tc.errSubstrings {
					if !strings.Contains(err.Error(), expected) {
						t.Fatalf(
							"Load() error = %q, missing %q",
							err.Error(),
							expected,
						)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() unexpected error: %v", err)
			}
			if tc.assert != nil {
				tc.assert(t, cfg)
			}
		})
	}
}

func baseConfigYAML(extraSections ...string) string {
	sections := []string{
		`server:
  listen_addr: ":8080"
  log_level: "info"
metrics:
  topology: "single_service_target"
  scrape_service_dns: "github-stats-exporter-metrics.github-stats-exporter.svc.cluster.local:8080"
leader_election:
  enabled: false
  namespace: "github-stats-exporter"
  lease_name: "github-stats-exporter-leader"
  lease_duration: "30s"
  renew_deadline: "20s"
  retry_period: "5s"
github:
  api_base_url: "https://api.github.com"
  request_timeout: "20s"
  unhealthy_failure_threshold: 6
  unhealthy_cooldown: "2m"
  orgs:
    - org: "org-a"
      app_id: 111111
      installation_id: 222222
      private_key_path: "/etc/github-stats-exporter/keys/org-a.pem"
      scrape_interval: "5m"
      repo_allowlist: ["*"]
      per_org_concurrency: 8
rate_limit:
  min_remaining_threshold: 200
  min_reset_buffer: "10s"
  secondary_limit_backoff: "60s"
retry:
  max_attempts: 5
  initial_backoff: "2s"
  max_backoff: "30s"
  jitter: true
loc:
  source: "stats_contributors"
  refresh_interval: "24h"
  fallback_enabled: true
  fallback_max_commits_per_repo_per_week: 250
  fallback_max_commit_detail_calls_per_org_per_hour: 1000
  large_repo_zero_detection_windows: 2
  large_repo_cooldown: "7d"
backfill:
  enabled: true
  max_message_age: "24h"
  consumer_count: 2
  requeue_delays: ["1m", "5m", "30m", "2h"]
  coalesce_window: "15m"
  dedup_ttl: "12h"
  max_enqueues_per_org_per_minute: 100
amqp:
  url: "amqp://githubstats:githubstats@rabbitmq:5672/"
  exchange: "gh.backfill"
  queue: "gh.backfill.jobs"
  dlq: "gh.backfill.dlq"
store:
  backend: "redis"
  redis_mode: "standalone"
  redis_addr: "redis:6379"
  redis_master_set: "mymaster"
  redis_sentinel_addrs: []
  redis_password: ""
  redis_db: 0
  retention: "30d"
  metric_refresh_interval: "30s"
  index_shards: 64
  export_cache_mode: "incremental"
  max_series_budget: 500000
health:
  github_probe_interval: "30s"
  github_recover_success_threshold: 3
telemetry:
  otel_enabled: false
  otel_exporter_otlp_endpoint: ""
  otel_trace_mode: "off"
  otel_trace_sample_ratio: 0.05`,
	}
	for _, extra := range extraSections {
		trimmed := strings.TrimSpace(extra)
		if trimmed == "" {
			continue
		}
		sections = append(sections, trimmed)
	}
	return fmt.Sprintf("%s\n", strings.Join(sections, "\n"))
}
