package app

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/cam3ron2/github-stats/internal/backfill"
	"github.com/cam3ron2/github-stats/internal/config"
	"github.com/cam3ron2/github-stats/internal/exporter"
	"github.com/cam3ron2/github-stats/internal/health"
	"github.com/cam3ron2/github-stats/internal/scrape"
	"github.com/cam3ron2/github-stats/internal/store"
	"go.uber.org/zap"
)

type runtimeStore interface {
	UpsertMetric(role store.RuntimeRole, source store.WriteSource, point store.MetricPoint) error
	AcquireJobLock(jobID string, ttl time.Duration, now time.Time) bool
	Acquire(key string, ttl time.Duration, now time.Time) bool
	Snapshot() []store.MetricPoint
	GC(now time.Time)
}

type runtimeQueue interface {
	Publish(msg backfill.Message) error
	Consume(ctx context.Context, handler func(backfill.Message) error, maxMessageAge time.Duration, nowFn func() time.Time)
	Depth() int
}

// Runtime is the application runtime orchestrator.
type Runtime struct {
	cfg         *config.Config
	store       runtimeStore
	queue       runtimeQueue
	dispatcher  *backfill.Dispatcher
	scrapeMgr   *scrape.Manager
	evaluator   *health.StatusEvaluator
	logger      *zap.Logger
	noopScraper bool

	mu                  sync.RWMutex
	role                health.Role
	redisHealthy        bool
	amqpHealthy         bool
	schedulerHealthy    bool
	githubClientUsable  bool
	consumerHealthy     bool
	exporterHealthy     bool
	githubHealthy       bool
	githubCooldownUntil time.Time
	githubFailureStreak int
	githubRecoverStreak int

	leaderCancel   context.CancelFunc
	followerCancel context.CancelFunc

	// Now is injected for deterministic tests.
	Now func() time.Time
}

// NewRuntime creates a runtime instance.
func NewRuntime(cfg *config.Config, orgScraper scrape.OrgScraper, logger ...*zap.Logger) *Runtime {
	if cfg == nil {
		cfg = &config.Config{}
	}
	noopScraper := false
	if orgScraper == nil {
		orgScraper = &scrape.NoopOrgScraper{}
		noopScraper = true
	} else {
		_, noopScraper = orgScraper.(*scrape.NoopOrgScraper)
	}
	baseLogger := zap.NewNop()
	if len(logger) > 0 && logger[0] != nil {
		baseLogger = logger[0]
	}

	retention := cfg.Store.Retention
	if retention <= 0 {
		retention = 24 * time.Hour
	}
	maxSeries := cfg.Store.MaxSeriesBudget
	if maxSeries <= 0 {
		maxSeries = 1_000_000
	}

	storeBackend, queueBackend := newRuntimeBackends(cfg, baseLogger, retention, maxSeries)
	dispatcher := backfill.NewDispatcher(backfill.Config{
		CoalesceWindow:             cfg.Backfill.CoalesceWindow,
		DedupTTL:                   cfg.Backfill.DedupTTL,
		MaxEnqueuesPerOrgPerMinute: cfg.Backfill.MaxEnqueuesPerOrgPerMinute,
	}, queueBackend, storeBackend)

	return &Runtime{
		cfg:                cfg,
		store:              storeBackend,
		queue:              queueBackend,
		dispatcher:         dispatcher,
		scrapeMgr:          scrape.NewManager(orgScraper, cfg.GitHub.Orgs),
		evaluator:          health.NewStatusEvaluator(),
		logger:             baseLogger,
		noopScraper:        noopScraper,
		role:               health.RoleFollower,
		redisHealthy:       true,
		amqpHealthy:        true,
		exporterHealthy:    true,
		githubHealthy:      true,
		githubClientUsable: true,
		Now:                time.Now,
	}
}

// Store exposes the runtime store.
func (r *Runtime) Store() runtimeStore {
	return r.store
}

// QueueDepth returns queued backfill messages.
func (r *Runtime) QueueDepth() int {
	return r.queue.Depth()
}

// Handler returns the combined HTTP handler.
func (r *Runtime) Handler() http.Handler {
	metricsHandler := exporter.NewOpenMetricsHandler(r.store)
	healthHandler := health.NewHandler(r)
	return NewHTTPHandler(metricsHandler, healthHandler)
}

// StartLeader starts leader responsibilities.
func (r *Runtime) StartLeader(ctx context.Context) {
	r.mu.Lock()
	if r.followerCancel != nil {
		r.followerCancel()
		r.followerCancel = nil
	}
	leaderCtx, cancel := context.WithCancel(ctx)
	r.leaderCancel = cancel
	r.role = health.RoleLeader
	r.schedulerHealthy = true
	r.githubClientUsable = true
	r.consumerHealthy = false
	r.mu.Unlock()
	interval := r.leaderInterval()
	orgCount := len(r.cfg.GitHub.Orgs)
	r.logger.Info(
		"starting leader loop",
		zap.Int("org_count", orgCount),
		zap.Duration("interval", interval),
		zap.Bool("noop_scraper", r.noopScraper),
	)
	if orgCount == 0 {
		r.logger.Warn("no github organizations configured; leader cycles will run without scrape outcomes")
	}
	if r.noopScraper {
		r.logger.Warn("runtime is using noop scraper; github activity metrics will remain empty until a real scraper is configured")
	}

	go r.runLeaderLoop(leaderCtx)
}

// StopLeader stops leader responsibilities.
func (r *Runtime) StopLeader() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.leaderCancel != nil {
		r.leaderCancel()
		r.leaderCancel = nil
	}
	r.schedulerHealthy = false
	r.logger.Info("stopped leader loop")
}

// StartFollower starts follower responsibilities.
func (r *Runtime) StartFollower(ctx context.Context) {
	r.mu.Lock()
	if r.leaderCancel != nil {
		r.leaderCancel()
		r.leaderCancel = nil
	}
	followerCtx, cancel := context.WithCancel(ctx)
	r.followerCancel = cancel
	r.role = health.RoleFollower
	r.schedulerHealthy = false
	r.githubClientUsable = false
	r.consumerHealthy = false
	r.mu.Unlock()
	r.logger.Info(
		"starting follower loop",
		zap.Int("queue_depth", r.queue.Depth()),
		zap.Duration("max_message_age", r.cfg.Backfill.MaxMessageAge),
	)

	go r.runFollowerLoop(followerCtx)
}

// StopFollower stops follower responsibilities.
func (r *Runtime) StopFollower() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.followerCancel != nil {
		r.followerCancel()
		r.followerCancel = nil
	}
	r.consumerHealthy = false
	r.logger.Info("stopped follower loop")
}

// CurrentStatus returns current health status.
func (r *Runtime) CurrentStatus(_ context.Context) health.Status {
	r.mu.RLock()
	input := health.Input{
		Role:               r.role,
		RedisHealthy:       r.redisHealthy,
		AMQPHealthy:        r.amqpHealthy,
		SchedulerHealthy:   r.schedulerHealthy,
		GitHubClientUsable: r.githubClientUsable,
		ConsumerHealthy:    r.consumerHealthy,
		ExporterHealthy:    r.exporterHealthy,
		GitHubHealthy:      r.githubHealthy,
	}
	r.mu.RUnlock()
	return r.evaluator.Evaluate(input)
}

// RunLeaderCycle executes one leader scrape cycle.
func (r *Runtime) RunLeaderCycle(ctx context.Context) error {
	now := r.Now()
	cycleStart := time.Now()
	r.logger.Debug("leader scrape cycle started", zap.Time("now", now), zap.Int("org_count", len(r.cfg.GitHub.Orgs)))

	if r.shouldSkipGitHubScrape(now) {
		return r.runCooldownCycle(now)
	}

	outcomes := r.scrapeMgr.ScrapeAll(ctx)

	var resultErr error
	orgFailures := 0
	metricsWritten := 0
	metricsFailed := 0
	backfillEnqueued := 0
	storeWriteSuccess := 0
	storeWriteFailure := 0

	if len(outcomes) == 0 {
		r.logger.Debug("leader scrape cycle produced no outcomes")
	}
	for _, outcome := range outcomes {
		if outcome.Err != nil {
			r.recordLeaderCycleMetricBestEffort(now, "gh_exporter_scrape_runs_total", 1, map[string]string{
				"org":    outcome.Org,
				"result": "failure",
			})
			r.logger.Warn("organization scrape failed", zap.String("org", outcome.Org), zap.Error(outcome.Err))
			orgFailures++
			if resultErr == nil {
				resultErr = outcome.Err
			} else {
				resultErr = errors.Join(resultErr, outcome.Err)
			}
			enqueueResult := r.dispatcher.EnqueueMissing(backfill.MessageInput{
				Org:         outcome.Org,
				Repo:        "*",
				WindowStart: now,
				WindowEnd:   now,
				Reason:      "scrape_error",
				Now:         now,
			})
			if enqueueResult.Published {
				backfillEnqueued++
				r.recordLeaderCycleMetricBestEffort(now, "gh_exporter_backfill_jobs_enqueued_total", 1, map[string]string{
					"org":    outcome.Org,
					"reason": "scrape_error",
				})
			}
			if enqueueResult.DedupSuppressed {
				r.recordLeaderCycleMetricBestEffort(now, "gh_exporter_backfill_jobs_deduped_total", 1, map[string]string{
					"org":    outcome.Org,
					"reason": "scrape_error",
				})
			}
			if enqueueResult.DroppedByRateLimit {
				r.recordLeaderCycleMetricBestEffort(now, "gh_exporter_backfill_enqueues_dropped_total", 1, map[string]string{
					"org":    outcome.Org,
					"reason": "org_rate_cap",
				})
			}
			continue
		}
		r.recordLeaderCycleMetricBestEffort(now, "gh_exporter_scrape_runs_total", 1, map[string]string{
			"org":    outcome.Org,
			"result": "success",
		})
		r.logger.Debug("organization scrape succeeded", zap.String("org", outcome.Org), zap.Int("metric_points", len(outcome.Result.Metrics)))
		r.logger.Debug(
			"organization scrape summary",
			zap.String("org", outcome.Org),
			zap.Int("repos_discovered", outcome.Result.Summary.ReposDiscovered),
			zap.Int("repos_targeted", outcome.Result.Summary.ReposTargeted),
			zap.Int("repos_processed", outcome.Result.Summary.ReposProcessed),
			zap.Int("repos_stats_202", outcome.Result.Summary.ReposStatsAccepted),
			zap.Int("repos_stats_forbidden", outcome.Result.Summary.ReposStatsForbidden),
			zap.Int("repos_stats_not_found", outcome.Result.Summary.ReposStatsNotFound),
			zap.Int("repos_stats_conflict", outcome.Result.Summary.ReposStatsConflict),
			zap.Int("repos_stats_unprocessable", outcome.Result.Summary.ReposStatsUnprocessable),
			zap.Int("repos_stats_unavailable", outcome.Result.Summary.ReposStatsUnavailable),
			zap.Int("repos_no_complete_week", outcome.Result.Summary.ReposNoCompleteWeek),
			zap.Int("repos_fallback_used", outcome.Result.Summary.ReposFallbackUsed),
			zap.Int("repos_fallback_truncated", outcome.Result.Summary.ReposFallbackTruncated),
			zap.Int("missed_windows", outcome.Result.Summary.MissedWindows),
			zap.Int("metrics_produced", outcome.Result.Summary.MetricsProduced),
		)

		for _, missed := range outcome.Result.MissedWindow {
			messageOrg := missed.Org
			if messageOrg == "" {
				messageOrg = outcome.Org
			}
			messageRepo := missed.Repo
			if messageRepo == "" {
				messageRepo = "*"
			}
			windowStart := missed.WindowStart
			if windowStart.IsZero() {
				windowStart = now
			}
			windowEnd := missed.WindowEnd
			if windowEnd.IsZero() {
				windowEnd = now
			}
			reason := missed.Reason
			if reason == "" {
				reason = "partial_scrape_error"
			}

			enqueueResult := r.dispatcher.EnqueueMissing(backfill.MessageInput{
				Org:         messageOrg,
				Repo:        messageRepo,
				WindowStart: windowStart,
				WindowEnd:   windowEnd,
				Reason:      reason,
				Now:         now,
			})
			if enqueueResult.Published {
				backfillEnqueued++
				r.recordLeaderCycleMetricBestEffort(now, "gh_exporter_backfill_jobs_enqueued_total", 1, map[string]string{
					"org":    messageOrg,
					"reason": reason,
				})
			}
			if enqueueResult.DedupSuppressed {
				r.recordLeaderCycleMetricBestEffort(now, "gh_exporter_backfill_jobs_deduped_total", 1, map[string]string{
					"org":    messageOrg,
					"reason": reason,
				})
				r.logger.Debug("backfill message dedup-suppressed", zap.String("org", messageOrg), zap.String("repo", messageRepo), zap.String("reason", reason))
			}
			if enqueueResult.DroppedByRateLimit {
				r.recordLeaderCycleMetricBestEffort(now, "gh_exporter_backfill_enqueues_dropped_total", 1, map[string]string{
					"org":    messageOrg,
					"reason": "org_rate_cap",
				})
				r.logger.Warn("backfill message dropped by dispatcher rate limit", zap.String("org", messageOrg), zap.String("repo", messageRepo), zap.String("reason", reason))
			}
		}

		for _, metric := range outcome.Result.Metrics {
			if metric.UpdatedAt.IsZero() {
				metric.UpdatedAt = now
			}
			if err := r.store.UpsertMetric(store.RoleLeader, store.SourceLeaderScrape, metric); err != nil {
				r.logger.Warn("failed to upsert leader metric", zap.String("org", outcome.Org), zap.String("metric", metric.Name), zap.Error(err))
				metricsFailed++
				storeWriteFailure++
				if resultErr == nil {
					resultErr = err
				} else {
					resultErr = errors.Join(resultErr, err)
				}
				continue
			}
			metricsWritten++
			storeWriteSuccess++
		}
	}
	if err := r.recordLeaderCycleMetric(now, "gh_exporter_leader_cycle_last_run_unixtime", float64(now.Unix()), nil); err != nil {
		r.logger.Warn("failed to persist internal leader cycle metric", zap.String("metric", "gh_exporter_leader_cycle_last_run_unixtime"), zap.Error(err))
		if resultErr == nil {
			resultErr = err
		} else {
			resultErr = errors.Join(resultErr, err)
		}
	}
	r.recordLeaderCycleMetricBestEffort(now, "gh_exporter_store_write_total", float64(storeWriteSuccess), map[string]string{
		"source": string(store.SourceLeaderScrape),
		"result": "success",
	})
	r.recordLeaderCycleMetricBestEffort(now, "gh_exporter_store_write_total", float64(storeWriteFailure), map[string]string{
		"source": string(store.SourceLeaderScrape),
		"result": "failure",
	})

	r.mu.Lock()
	r.updateGitHubHealthLocked(now, orgFailures == 0)
	r.mu.Unlock()
	r.mu.RLock()
	currentGitHubHealthy := r.githubHealthy
	r.mu.RUnlock()

	r.recordDependencyHealthMetrics(now)
	r.store.GC(now)

	r.logger.Info(
		"leader scrape cycle completed",
		zap.Int("orgs_scraped", len(outcomes)),
		zap.Int("org_failures", orgFailures),
		zap.Int("metrics_written", metricsWritten),
		zap.Int("metrics_failed", metricsFailed),
		zap.Int("backfill_enqueued", backfillEnqueued),
		zap.Int("queue_depth", r.queue.Depth()),
		zap.Bool("github_healthy", currentGitHubHealthy),
		zap.Duration("duration", time.Since(cycleStart)),
	)
	return resultErr
}

func (r *Runtime) runLeaderLoop(ctx context.Context) {
	interval := r.leaderInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	if err := r.RunLeaderCycle(ctx); err != nil {
		r.logger.Warn("leader scrape cycle finished with errors", zap.Error(err))
	}
	for {
		select {
		case <-ctx.Done():
			r.logger.Debug("leader loop stopped")
			return
		case <-ticker.C:
			if err := r.RunLeaderCycle(ctx); err != nil {
				r.logger.Warn("leader scrape cycle finished with errors", zap.Error(err))
			}
		}
	}
}

func (r *Runtime) runFollowerLoop(ctx context.Context) {
	r.mu.Lock()
	r.consumerHealthy = true
	r.mu.Unlock()

	maxAge := r.cfg.Backfill.MaxMessageAge
	if maxAge <= 0 {
		maxAge = 24 * time.Hour
	}
	lockTTL := r.cfg.Backfill.DedupTTL
	if lockTTL <= 0 {
		lockTTL = maxAge
	}

	r.queue.Consume(ctx, func(msg backfill.Message) error {
		if msg.JobID != "" && !r.store.AcquireJobLock(msg.JobID, lockTTL, r.Now()) {
			r.logger.Debug("follower skipped duplicate backfill message", zap.String("job_id", msg.JobID))
			return nil
		}
		err := r.store.UpsertMetric(store.RoleFollower, store.SourceWorkerBackfill, store.MetricPoint{
			Name: "gh_exporter_backfill_jobs_processed_total",
			Labels: map[string]string{
				"org":    msg.Org,
				"repo":   msg.Repo,
				"result": "processed",
			},
			Value:     1,
			UpdatedAt: r.Now(),
		})
		if err != nil {
			r.recordFollowerMetricBestEffort(r.Now(), "gh_exporter_store_write_total", 1, map[string]string{
				"source": string(store.SourceWorkerBackfill),
				"result": "failure",
			})
			r.logger.Warn("follower failed to persist backfill metric", zap.String("org", msg.Org), zap.String("repo", msg.Repo), zap.Error(err))
			return err
		}
		r.recordFollowerMetricBestEffort(r.Now(), "gh_exporter_store_write_total", 1, map[string]string{
			"source": string(store.SourceWorkerBackfill),
			"result": "success",
		})
		r.logger.Debug("follower processed backfill message", zap.String("org", msg.Org), zap.String("repo", msg.Repo), zap.Int("queue_depth", r.queue.Depth()))
		return nil
	}, maxAge, r.Now)

	r.mu.Lock()
	r.consumerHealthy = false
	r.mu.Unlock()
}

func (r *Runtime) leaderInterval() time.Duration {
	interval := 5 * time.Minute
	if len(r.cfg.GitHub.Orgs) > 0 && r.cfg.GitHub.Orgs[0].ScrapeInterval > 0 {
		interval = r.cfg.GitHub.Orgs[0].ScrapeInterval
	}
	return interval
}

func (r *Runtime) recordLeaderCycleMetric(now time.Time, name string, value float64, labels map[string]string) error {
	return r.store.UpsertMetric(store.RoleLeader, store.SourceLeaderScrape, store.MetricPoint{
		Name:      name,
		Labels:    labels,
		Value:     value,
		UpdatedAt: now,
	})
}

func (r *Runtime) recordFollowerMetric(now time.Time, name string, value float64, labels map[string]string) error {
	return r.store.UpsertMetric(store.RoleFollower, store.SourceWorkerBackfill, store.MetricPoint{
		Name:      name,
		Labels:    labels,
		Value:     value,
		UpdatedAt: now,
	})
}

func (r *Runtime) recordLeaderCycleMetricBestEffort(
	now time.Time,
	name string,
	value float64,
	labels map[string]string,
) {
	if err := r.recordLeaderCycleMetric(now, name, value, labels); err != nil {
		r.logger.Warn(
			"failed to persist leader operational metric",
			zap.String("metric", name),
			zap.Error(err),
		)
	}
}

func (r *Runtime) recordFollowerMetricBestEffort(
	now time.Time,
	name string,
	value float64,
	labels map[string]string,
) {
	if err := r.recordFollowerMetric(now, name, value, labels); err != nil {
		r.logger.Warn(
			"failed to persist follower operational metric",
			zap.String("metric", name),
			zap.Error(err),
		)
	}
}

func (r *Runtime) shouldSkipGitHubScrape(now time.Time) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return !r.githubCooldownUntil.IsZero() && now.Before(r.githubCooldownUntil)
}

func (r *Runtime) runCooldownCycle(now time.Time) error {
	for _, org := range r.cfg.GitHub.Orgs {
		r.recordLeaderCycleMetricBestEffort(now, "gh_exporter_scrape_runs_total", 1, map[string]string{
			"org":    org.Org,
			"result": "skipped_unhealthy",
		})
		enqueueResult := r.dispatcher.EnqueueMissing(backfill.MessageInput{
			Org:         org.Org,
			Repo:        "*",
			WindowStart: now,
			WindowEnd:   now,
			Reason:      "github_unhealthy",
			Now:         now,
		})
		if enqueueResult.Published {
			r.recordLeaderCycleMetricBestEffort(now, "gh_exporter_backfill_jobs_enqueued_total", 1, map[string]string{
				"org":    org.Org,
				"reason": "github_unhealthy",
			})
		}
		if enqueueResult.DedupSuppressed {
			r.recordLeaderCycleMetricBestEffort(now, "gh_exporter_backfill_jobs_deduped_total", 1, map[string]string{
				"org":    org.Org,
				"reason": "github_unhealthy",
			})
		}
		if enqueueResult.DroppedByRateLimit {
			r.recordLeaderCycleMetricBestEffort(now, "gh_exporter_backfill_enqueues_dropped_total", 1, map[string]string{
				"org":    org.Org,
				"reason": "org_rate_cap",
			})
		}
	}

	r.recordLeaderCycleMetricBestEffort(now, "gh_exporter_leader_cycle_last_run_unixtime", float64(now.Unix()), nil)
	r.recordDependencyHealthMetrics(now)
	r.store.GC(now)
	return nil
}

func (r *Runtime) updateGitHubHealthLocked(now time.Time, cycleSuccessful bool) {
	threshold := r.cfg.GitHub.UnhealthyFailureThreshold
	if threshold <= 0 {
		threshold = 1
	}
	cooldown := r.cfg.GitHub.UnhealthyCooldown
	if cooldown <= 0 {
		cooldown = time.Minute
	}
	recoverThreshold := r.cfg.Health.GitHubRecoverSuccessThreshold
	if recoverThreshold <= 0 {
		recoverThreshold = 1
	}

	if cycleSuccessful {
		r.githubFailureStreak = 0
		if r.githubHealthy {
			r.githubRecoverStreak = 0
			r.githubCooldownUntil = time.Time{}
			return
		}
		r.githubRecoverStreak++
		if r.githubRecoverStreak >= recoverThreshold {
			r.githubHealthy = true
			r.githubRecoverStreak = 0
			r.githubCooldownUntil = time.Time{}
		}
		return
	}

	r.githubRecoverStreak = 0
	r.githubFailureStreak++
	if r.githubFailureStreak >= threshold {
		r.githubHealthy = false
		r.githubCooldownUntil = now.Add(cooldown)
	}
}

func (r *Runtime) recordDependencyHealthMetrics(now time.Time) {
	r.mu.RLock()
	components := map[string]bool{
		"redis":     r.redisHealthy,
		"amqp":      r.amqpHealthy,
		"scheduler": r.schedulerHealthy,
		"github":    r.githubHealthy,
		"consumer":  r.consumerHealthy,
		"exporter":  r.exporterHealthy,
	}
	r.mu.RUnlock()

	for dependency, healthy := range components {
		value := 0.0
		if healthy {
			value = 1
		}
		r.recordLeaderCycleMetricBestEffort(now, "gh_exporter_dependency_health", value, map[string]string{
			"dependency": dependency,
		})
	}
}
