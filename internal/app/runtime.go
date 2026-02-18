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

// Runtime is the application runtime orchestrator.
type Runtime struct {
	cfg         *config.Config
	store       *store.MemoryStore
	queue       *backfill.InMemoryQueue
	dispatcher  *backfill.Dispatcher
	scrapeMgr   *scrape.Manager
	evaluator   *health.StatusEvaluator
	logger      *zap.Logger
	noopScraper bool

	mu                 sync.RWMutex
	role               health.Role
	redisHealthy       bool
	amqpHealthy        bool
	schedulerHealthy   bool
	githubClientUsable bool
	consumerHealthy    bool
	exporterHealthy    bool
	githubHealthy      bool

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

	memStore := store.NewMemoryStore(retention, maxSeries)
	queue := backfill.NewInMemoryQueue(10000)
	dispatcher := backfill.NewDispatcher(backfill.Config{
		CoalesceWindow:             cfg.Backfill.CoalesceWindow,
		DedupTTL:                   cfg.Backfill.DedupTTL,
		MaxEnqueuesPerOrgPerMinute: cfg.Backfill.MaxEnqueuesPerOrgPerMinute,
	}, queue, memStore)

	return &Runtime{
		cfg:                cfg,
		store:              memStore,
		queue:              queue,
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
func (r *Runtime) Store() *store.MemoryStore {
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

	outcomes := r.scrapeMgr.ScrapeAll(ctx)

	var resultErr error
	githubHealthy := true
	orgFailures := 0
	metricsWritten := 0
	metricsFailed := 0
	backfillEnqueued := 0

	if len(outcomes) == 0 {
		r.logger.Debug("leader scrape cycle produced no outcomes")
	}
	for _, outcome := range outcomes {
		if outcome.Err != nil {
			r.logger.Warn("organization scrape failed", zap.String("org", outcome.Org), zap.Error(outcome.Err))
			githubHealthy = false
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
			}
			continue
		}
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
			}
			if enqueueResult.DedupSuppressed {
				r.logger.Debug("backfill message dedup-suppressed", zap.String("org", messageOrg), zap.String("repo", messageRepo), zap.String("reason", reason))
			}
			if enqueueResult.DroppedByRateLimit {
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
				if resultErr == nil {
					resultErr = err
				} else {
					resultErr = errors.Join(resultErr, err)
				}
				continue
			}
			metricsWritten++
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

	r.mu.Lock()
	r.githubHealthy = githubHealthy
	r.mu.Unlock()
	r.logger.Info(
		"leader scrape cycle completed",
		zap.Int("orgs_scraped", len(outcomes)),
		zap.Int("org_failures", orgFailures),
		zap.Int("metrics_written", metricsWritten),
		zap.Int("metrics_failed", metricsFailed),
		zap.Int("backfill_enqueued", backfillEnqueued),
		zap.Int("queue_depth", r.queue.Depth()),
		zap.Bool("github_healthy", githubHealthy),
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

	r.queue.Consume(ctx, func(msg backfill.Message) error {
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
			r.logger.Warn("follower failed to persist backfill metric", zap.String("org", msg.Org), zap.String("repo", msg.Repo), zap.Error(err))
			return err
		}
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
