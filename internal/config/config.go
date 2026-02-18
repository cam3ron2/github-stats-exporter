package config

import (
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

var validLogLevels = []string{"debug", "info", "warn", "error"}

// Config is the root application configuration.
type Config struct {
	Server         ServerConfig
	Metrics        MetricsConfig
	LeaderElection LeaderElectionConfig
	GitHub         GitHubConfig
	RateLimit      RateLimitConfig
	Retry          RetryConfig
	LOC            LOCConfig
	Backfill       BackfillConfig
	AMQP           AMQPConfig
	Store          StoreConfig
	Health         HealthConfig
	Telemetry      TelemetryConfig
}

// ServerConfig contains HTTP server settings.
type ServerConfig struct {
	ListenAddr string `yaml:"listen_addr"`
	LogLevel   string `yaml:"log_level"`
}

// MetricsConfig contains scrape topology settings.
type MetricsConfig struct {
	Topology         string `yaml:"topology"`
	ScrapeServiceDNS string `yaml:"scrape_service_dns"`
}

// LeaderElectionConfig contains leader election settings.
type LeaderElectionConfig struct {
	Enabled       bool
	Namespace     string
	LeaseName     string
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
}

// GitHubConfig configures GitHub API interactions.
type GitHubConfig struct {
	APIBaseURL                string
	RequestTimeout            time.Duration
	UnhealthyFailureThreshold int
	UnhealthyCooldown         time.Duration
	Orgs                      []GitHubOrgConfig
}

// GitHubOrgConfig configures a single organization scrape target.
type GitHubOrgConfig struct {
	Org               string `yaml:"org"`
	AppID             int64  `yaml:"app_id"`
	InstallationID    int64  `yaml:"installation_id"`
	PrivateKeyPath    string `yaml:"private_key_path"`
	ScrapeInterval    time.Duration
	RepoAllowlist     []string `yaml:"repo_allowlist"`
	PerOrgConcurrency int      `yaml:"per_org_concurrency"`
}

// RateLimitConfig configures rate-limit controls.
type RateLimitConfig struct {
	MinRemainingThreshold int
	MinResetBuffer        time.Duration
	SecondaryLimitBackoff time.Duration
}

// RetryConfig configures retries.
type RetryConfig struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Jitter         bool
}

// LOCConfig configures lines-of-code metrics.
type LOCConfig struct {
	Source                                    string
	RefreshInterval                           time.Duration
	FallbackEnabled                           bool
	FallbackMaxCommitsPerRepoPerWeek          int
	FallbackMaxCommitDetailCallsPerOrgPerHour int
	LargeRepoZeroDetectionWindows             int
	LargeRepoCooldown                         time.Duration
}

// BackfillConfig configures backfill queue behavior.
type BackfillConfig struct {
	Enabled                    bool
	MaxMessageAge              time.Duration
	ConsumerCount              int
	RequeueDelays              []time.Duration
	CoalesceWindow             time.Duration
	DedupTTL                   time.Duration
	MaxEnqueuesPerOrgPerMinute int
}

// AMQPConfig configures RabbitMQ/AMQP connectivity.
type AMQPConfig struct {
	URL      string `yaml:"url"`
	Exchange string `yaml:"exchange"`
	Queue    string `yaml:"queue"`
	DLQ      string `yaml:"dlq"`
}

// StoreConfig configures metric storage.
type StoreConfig struct {
	Backend               string
	RedisMode             string
	RedisAddr             string
	RedisMasterSet        string
	RedisSentinelAddrs    []string
	RedisPassword         string
	RedisDB               int
	Retention             time.Duration
	MetricRefreshInterval time.Duration
	IndexShards           int
	ExportCacheMode       string
	MaxSeriesBudget       int
}

// HealthConfig configures health probe behavior.
type HealthConfig struct {
	GitHubProbeInterval           time.Duration
	GitHubRecoverSuccessThreshold int
}

// TelemetryConfig configures OpenTelemetry behavior.
type TelemetryConfig struct {
	OTELEnabled          bool
	OTELExporterEndpoint string
	OTELTraceMode        string
	OTELTraceSampleRatio float64
}

// Load reads configuration from YAML and validates the result.
func Load(reader io.Reader) (*Config, error) {
	if reader == nil {
		return nil, fmt.Errorf("config reader is nil")
	}

	decoder := yaml.NewDecoder(reader)
	decoder.KnownFields(true)

	var raw rawConfig
	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("unmarshal yaml: %w", err)
	}

	cfg := raw.toConfig()
	applyDefaults(cfg)

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate validates configuration values.
func (c *Config) Validate() error {
	var errs []string

	if !slices.Contains(validLogLevels, c.Server.LogLevel) {
		errs = append(errs, "server.log_level must be one of debug|info|warn|error")
	}

	if c.Metrics.Topology != "single_service_target" {
		errs = append(errs, "metrics.topology must be single_service_target")
	}

	if len(c.GitHub.Orgs) == 0 {
		errs = append(errs, "github.orgs must contain at least one organization")
	}

	seenOrgs := make(map[string]struct{}, len(c.GitHub.Orgs))
	for i, org := range c.GitHub.Orgs {
		prefix := fmt.Sprintf("github.orgs[%d]", i)
		if org.Org == "" {
			errs = append(errs, prefix+".org is required")
		}
		if org.AppID <= 0 {
			errs = append(errs, prefix+".app_id must be > 0")
		}
		if org.InstallationID <= 0 {
			errs = append(errs, prefix+".installation_id must be > 0")
		}
		if org.PrivateKeyPath == "" {
			errs = append(errs, prefix+".private_key_path is required")
		}
		if org.ScrapeInterval <= 0 {
			errs = append(errs, prefix+".scrape_interval must be > 0")
		}
		if _, ok := seenOrgs[org.Org]; ok {
			errs = append(errs, "github.orgs contains duplicate org: "+org.Org)
		}
		seenOrgs[org.Org] = struct{}{}
	}

	if c.Store.RedisMode != "standalone" && c.Store.RedisMode != "sentinel" {
		errs = append(errs, "store.redis_mode must be standalone or sentinel")
	}
	if c.Store.RedisMode == "sentinel" && len(c.Store.RedisSentinelAddrs) == 0 {
		errs = append(errs, "store.redis_sentinel_addrs is required when store.redis_mode=sentinel")
	}

	if c.LOC.FallbackEnabled {
		if c.LOC.FallbackMaxCommitsPerRepoPerWeek <= 0 {
			errs = append(errs, "loc.fallback_max_commits_per_repo_per_week must be > 0 when loc.fallback_enabled=true")
		}
		if c.LOC.FallbackMaxCommitDetailCallsPerOrgPerHour <= 0 {
			errs = append(errs, "loc.fallback_max_commit_detail_calls_per_org_per_hour must be > 0 when loc.fallback_enabled=true")
		}
	}

	if len(c.Backfill.RequeueDelays) == 0 {
		errs = append(errs, "backfill.requeue_delays must contain at least one duration")
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func applyDefaults(cfg *Config) {
	if cfg.Server.LogLevel == "" {
		cfg.Server.LogLevel = "info"
	}
	if cfg.Metrics.Topology == "" {
		cfg.Metrics.Topology = "single_service_target"
	}
	if cfg.Store.RedisMode == "" {
		cfg.Store.RedisMode = "standalone"
	}
	if cfg.Store.Backend == "" {
		cfg.Store.Backend = "redis"
	}
	if cfg.Store.ExportCacheMode == "" {
		cfg.Store.ExportCacheMode = "incremental"
	}
}

type duration struct {
	time.Duration
}

func (d *duration) UnmarshalYAML(value *yaml.Node) error {
	if value == nil || value.Kind == 0 || strings.TrimSpace(value.Value) == "" {
		d.Duration = 0
		return nil
	}

	var raw string
	if err := value.Decode(&raw); err != nil {
		return fmt.Errorf("decode duration: %w", err)
	}

	parsed, err := parseFlexibleDuration(raw)
	if err != nil {
		return err
	}
	d.Duration = parsed
	return nil
}

func parseFlexibleDuration(raw string) (time.Duration, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, nil
	}

	if standard, err := time.ParseDuration(trimmed); err == nil {
		return standard, nil
	}

	if strings.HasSuffix(trimmed, "d") {
		return parseDurationWithMultiplier(strings.TrimSuffix(trimmed, "d"), 24)
	}
	if strings.HasSuffix(trimmed, "w") {
		return parseDurationWithMultiplier(strings.TrimSuffix(trimmed, "w"), 24*7)
	}

	return 0, fmt.Errorf("parse duration %q: invalid unit", raw)
}

func parseDurationWithMultiplier(numeric string, multiplierHours float64) (time.Duration, error) {
	value, err := strconv.ParseFloat(strings.TrimSpace(numeric), 64)
	if err != nil {
		return 0, fmt.Errorf("parse duration value %q: %w", numeric, err)
	}

	nanos := value * multiplierHours * float64(time.Hour)
	if nanos > math.MaxInt64 || nanos < math.MinInt64 {
		return 0, fmt.Errorf("parse duration value %q: out of range", numeric)
	}
	return time.Duration(nanos), nil
}

type rawConfig struct {
	Server         ServerConfig      `yaml:"server"`
	Metrics        MetricsConfig     `yaml:"metrics"`
	LeaderElection rawLeaderElection `yaml:"leader_election"`
	GitHub         rawGitHub         `yaml:"github"`
	RateLimit      rawRateLimit      `yaml:"rate_limit"`
	Retry          rawRetry          `yaml:"retry"`
	LOC            rawLOC            `yaml:"loc"`
	Backfill       rawBackfill       `yaml:"backfill"`
	AMQP           AMQPConfig        `yaml:"amqp"`
	Store          rawStore          `yaml:"store"`
	Health         rawHealth         `yaml:"health"`
	Telemetry      rawTelemetry      `yaml:"telemetry"`
}

type rawLeaderElection struct {
	Enabled       bool     `yaml:"enabled"`
	Namespace     string   `yaml:"namespace"`
	LeaseName     string   `yaml:"lease_name"`
	LeaseDuration duration `yaml:"lease_duration"`
	RenewDeadline duration `yaml:"renew_deadline"`
	RetryPeriod   duration `yaml:"retry_period"`
}

type rawGitHub struct {
	APIBaseURL                string         `yaml:"api_base_url"`
	RequestTimeout            duration       `yaml:"request_timeout"`
	UnhealthyFailureThreshold int            `yaml:"unhealthy_failure_threshold"`
	UnhealthyCooldown         duration       `yaml:"unhealthy_cooldown"`
	Orgs                      []rawGitHubOrg `yaml:"orgs"`
}

type rawGitHubOrg struct {
	Org               string   `yaml:"org"`
	AppID             int64    `yaml:"app_id"`
	InstallationID    int64    `yaml:"installation_id"`
	PrivateKeyPath    string   `yaml:"private_key_path"`
	ScrapeInterval    duration `yaml:"scrape_interval"`
	RepoAllowlist     []string `yaml:"repo_allowlist"`
	PerOrgConcurrency int      `yaml:"per_org_concurrency"`
}

type rawRateLimit struct {
	MinRemainingThreshold int      `yaml:"min_remaining_threshold"`
	MinResetBuffer        duration `yaml:"min_reset_buffer"`
	SecondaryLimitBackoff duration `yaml:"secondary_limit_backoff"`
}

type rawRetry struct {
	MaxAttempts    int      `yaml:"max_attempts"`
	InitialBackoff duration `yaml:"initial_backoff"`
	MaxBackoff     duration `yaml:"max_backoff"`
	Jitter         bool     `yaml:"jitter"`
}

type rawLOC struct {
	Source                                    string   `yaml:"source"`
	RefreshInterval                           duration `yaml:"refresh_interval"`
	FallbackEnabled                           bool     `yaml:"fallback_enabled"`
	FallbackMaxCommitsPerRepoPerWeek          int      `yaml:"fallback_max_commits_per_repo_per_week"`
	FallbackMaxCommitDetailCallsPerOrgPerHour int      `yaml:"fallback_max_commit_detail_calls_per_org_per_hour"`
	LargeRepoZeroDetectionWindows             int      `yaml:"large_repo_zero_detection_windows"`
	LargeRepoCooldown                         duration `yaml:"large_repo_cooldown"`
}

type rawBackfill struct {
	Enabled                    bool       `yaml:"enabled"`
	MaxMessageAge              duration   `yaml:"max_message_age"`
	ConsumerCount              int        `yaml:"consumer_count"`
	RequeueDelays              []duration `yaml:"requeue_delays"`
	CoalesceWindow             duration   `yaml:"coalesce_window"`
	DedupTTL                   duration   `yaml:"dedup_ttl"`
	MaxEnqueuesPerOrgPerMinute int        `yaml:"max_enqueues_per_org_per_minute"`
}

type rawStore struct {
	Backend               string   `yaml:"backend"`
	RedisMode             string   `yaml:"redis_mode"`
	RedisAddr             string   `yaml:"redis_addr"`
	RedisMasterSet        string   `yaml:"redis_master_set"`
	RedisSentinelAddrs    []string `yaml:"redis_sentinel_addrs"`
	RedisPassword         string   `yaml:"redis_password"`
	RedisDB               int      `yaml:"redis_db"`
	Retention             duration `yaml:"retention"`
	MetricRefreshInterval duration `yaml:"metric_refresh_interval"`
	IndexShards           int      `yaml:"index_shards"`
	ExportCacheMode       string   `yaml:"export_cache_mode"`
	MaxSeriesBudget       int      `yaml:"max_series_budget"`
}

type rawHealth struct {
	GitHubProbeInterval           duration `yaml:"github_probe_interval"`
	GitHubRecoverSuccessThreshold int      `yaml:"github_recover_success_threshold"`
}

type rawTelemetry struct {
	OTELEnabled          bool    `yaml:"otel_enabled"`
	OTELExporterEndpoint string  `yaml:"otel_exporter_otlp_endpoint"`
	OTELTraceMode        string  `yaml:"otel_trace_mode"`
	OTELTraceSampleRatio float64 `yaml:"otel_trace_sample_ratio"`
}

func (r rawConfig) toConfig() *Config {
	cfg := &Config{
		Server:  r.Server,
		Metrics: r.Metrics,
		LeaderElection: LeaderElectionConfig{
			Enabled:       r.LeaderElection.Enabled,
			Namespace:     r.LeaderElection.Namespace,
			LeaseName:     r.LeaderElection.LeaseName,
			LeaseDuration: r.LeaderElection.LeaseDuration.Duration,
			RenewDeadline: r.LeaderElection.RenewDeadline.Duration,
			RetryPeriod:   r.LeaderElection.RetryPeriod.Duration,
		},
		GitHub: GitHubConfig{
			APIBaseURL:                r.GitHub.APIBaseURL,
			RequestTimeout:            r.GitHub.RequestTimeout.Duration,
			UnhealthyFailureThreshold: r.GitHub.UnhealthyFailureThreshold,
			UnhealthyCooldown:         r.GitHub.UnhealthyCooldown.Duration,
			Orgs:                      make([]GitHubOrgConfig, 0, len(r.GitHub.Orgs)),
		},
		RateLimit: RateLimitConfig{
			MinRemainingThreshold: r.RateLimit.MinRemainingThreshold,
			MinResetBuffer:        r.RateLimit.MinResetBuffer.Duration,
			SecondaryLimitBackoff: r.RateLimit.SecondaryLimitBackoff.Duration,
		},
		Retry: RetryConfig{
			MaxAttempts:    r.Retry.MaxAttempts,
			InitialBackoff: r.Retry.InitialBackoff.Duration,
			MaxBackoff:     r.Retry.MaxBackoff.Duration,
			Jitter:         r.Retry.Jitter,
		},
		LOC: LOCConfig{
			Source:                           r.LOC.Source,
			RefreshInterval:                  r.LOC.RefreshInterval.Duration,
			FallbackEnabled:                  r.LOC.FallbackEnabled,
			FallbackMaxCommitsPerRepoPerWeek: r.LOC.FallbackMaxCommitsPerRepoPerWeek,
			FallbackMaxCommitDetailCallsPerOrgPerHour: r.LOC.FallbackMaxCommitDetailCallsPerOrgPerHour,
			LargeRepoZeroDetectionWindows:             r.LOC.LargeRepoZeroDetectionWindows,
			LargeRepoCooldown:                         r.LOC.LargeRepoCooldown.Duration,
		},
		Backfill: BackfillConfig{
			Enabled:                    r.Backfill.Enabled,
			MaxMessageAge:              r.Backfill.MaxMessageAge.Duration,
			ConsumerCount:              r.Backfill.ConsumerCount,
			RequeueDelays:              make([]time.Duration, 0, len(r.Backfill.RequeueDelays)),
			CoalesceWindow:             r.Backfill.CoalesceWindow.Duration,
			DedupTTL:                   r.Backfill.DedupTTL.Duration,
			MaxEnqueuesPerOrgPerMinute: r.Backfill.MaxEnqueuesPerOrgPerMinute,
		},
		AMQP: r.AMQP,
		Store: StoreConfig{
			Backend:               r.Store.Backend,
			RedisMode:             r.Store.RedisMode,
			RedisAddr:             r.Store.RedisAddr,
			RedisMasterSet:        r.Store.RedisMasterSet,
			RedisSentinelAddrs:    r.Store.RedisSentinelAddrs,
			RedisPassword:         r.Store.RedisPassword,
			RedisDB:               r.Store.RedisDB,
			Retention:             r.Store.Retention.Duration,
			MetricRefreshInterval: r.Store.MetricRefreshInterval.Duration,
			IndexShards:           r.Store.IndexShards,
			ExportCacheMode:       r.Store.ExportCacheMode,
			MaxSeriesBudget:       r.Store.MaxSeriesBudget,
		},
		Health: HealthConfig{
			GitHubProbeInterval:           r.Health.GitHubProbeInterval.Duration,
			GitHubRecoverSuccessThreshold: r.Health.GitHubRecoverSuccessThreshold,
		},
		Telemetry: TelemetryConfig{
			OTELEnabled:          r.Telemetry.OTELEnabled,
			OTELExporterEndpoint: r.Telemetry.OTELExporterEndpoint,
			OTELTraceMode:        r.Telemetry.OTELTraceMode,
			OTELTraceSampleRatio: r.Telemetry.OTELTraceSampleRatio,
		},
	}

	for _, org := range r.GitHub.Orgs {
		cfg.GitHub.Orgs = append(cfg.GitHub.Orgs, GitHubOrgConfig{
			Org:               org.Org,
			AppID:             org.AppID,
			InstallationID:    org.InstallationID,
			PrivateKeyPath:    org.PrivateKeyPath,
			ScrapeInterval:    org.ScrapeInterval.Duration,
			RepoAllowlist:     org.RepoAllowlist,
			PerOrgConcurrency: org.PerOrgConcurrency,
		})
	}
	for _, delay := range r.Backfill.RequeueDelays {
		cfg.Backfill.RequeueDelays = append(cfg.Backfill.RequeueDelays, delay.Duration)
	}

	return cfg
}
