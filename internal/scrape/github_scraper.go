package scrape

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cam3ron2/github-stats-exporter/internal/config"
	"github.com/cam3ron2/github-stats-exporter/internal/githubapi"
	"github.com/cam3ron2/github-stats-exporter/internal/store"
)

const (
	dayFormat = "2006-01-02"

	repoMissReasonContributorStats = "contributor_stats_failed"
	repoMissReasonFallbackList     = "fallback_list_commits_failed"
	repoMissReasonFallbackDetail   = "fallback_commit_detail_failed"
	repoMissReasonActivityCommits  = "activity_commits_failed"
	repoMissReasonActivityPulls    = "activity_pulls_failed"
	repoMissReasonActivityReviews  = "activity_reviews_failed"
	repoMissReasonActivityComments = "activity_issue_comments_failed"
	repoMissReasonCopilotFetch     = "copilot_report_fetch_error"
	repoMissReasonCopilotDownload  = "copilot_report_download_error"
	repoMissReasonCopilotParse     = "copilot_report_parse_error"
	userLabelUnlinkedGitAuthor     = "unlinked_git_author"
	userLabelUnattributedCommit    = "unattributed_commit"
	locModeStatsContributors       = "stats_contributors"
	locModeSampledCommitStats      = "sampled_commit_stats"
	internalMetricLOCSourceMode    = "gh_exporter_loc_source_mode"
	internalMetricLOCFallbackIncom = "gh_exporter_loc_fallback_incomplete"
	copilotScopeOrg                = "org"
	copilotScopeUsers              = "users"
	copilotScopeEnterprise         = "enterprise"
	copilotScopeEnterpriseUsers    = "enterprise_users"
	copilotWindow1d                = "1d"
	copilotWindow28d               = "28d"
)

// GitHubDataClient is the typed GitHub API interface consumed by the org scraper.
type GitHubDataClient interface {
	ListOrgRepos(ctx context.Context, org string) (githubapi.OrgReposResult, error)
	GetContributorStats(ctx context.Context, owner, repo string) (githubapi.ContributorStatsResult, error)
	ListRepoCommitsWindow(ctx context.Context, owner, repo string, since, until time.Time, maxCommits int) (githubapi.CommitListResult, error)
	ListRepoPullRequestsWindow(ctx context.Context, owner, repo string, since, until time.Time) (githubapi.PullRequestListResult, error)
	ListPullReviews(ctx context.Context, owner, repo string, pullNumber int, since, until time.Time) (githubapi.PullReviewsResult, error)
	ListIssueCommentsWindow(ctx context.Context, owner, repo string, since, until time.Time) (githubapi.IssueCommentsResult, error)
	GetCommit(ctx context.Context, owner, repo, sha string) (githubapi.CommitDetail, error)
	GetOrgCopilotOrganization1DayReportLink(ctx context.Context, org string, day time.Time) (githubapi.CopilotReportLinkResult, error)
	GetOrgCopilotOrganization28DayLatestReportLink(ctx context.Context, org string) (githubapi.CopilotReportLinkResult, error)
	GetOrgCopilotUsers1DayReportLink(ctx context.Context, org string, day time.Time) (githubapi.CopilotReportLinkResult, error)
	GetOrgCopilotUsers28DayLatestReportLink(ctx context.Context, org string) (githubapi.CopilotReportLinkResult, error)
	GetEnterpriseCopilotEnterprise1DayReportLink(
		ctx context.Context,
		enterprise string,
		day time.Time,
	) (githubapi.CopilotReportLinkResult, error)
	GetEnterpriseCopilotEnterprise28DayLatestReportLink(
		ctx context.Context,
		enterprise string,
	) (githubapi.CopilotReportLinkResult, error)
	GetEnterpriseCopilotUsers1DayReportLink(
		ctx context.Context,
		enterprise string,
		day time.Time,
	) (githubapi.CopilotReportLinkResult, error)
	GetEnterpriseCopilotUsers28DayLatestReportLink(
		ctx context.Context,
		enterprise string,
	) (githubapi.CopilotReportLinkResult, error)
	StreamCopilotReportNDJSON(
		ctx context.Context,
		signedReportURL string,
		handler func(record map[string]any) error,
	) (githubapi.CopilotReportStreamResult, error)
}

// GitHubOrgScraperConfig configures GitHub-backed org scraping behavior.
type GitHubOrgScraperConfig struct {
	LOCRefreshInterval                        time.Duration
	FallbackEnabled                           bool
	FallbackMaxCommitsPerRepoPerWeek          int
	FallbackMaxCommitDetailCallsPerOrgPerHour int
	LargeRepoZeroDetectionWindows             int
	LargeRepoCooldown                         time.Duration
	Copilot                                   config.CopilotConfig
	CopilotEnterpriseClient                   GitHubDataClient
	CopilotEnterprisePrimaryOrg               string
	Checkpoints                               CheckpointStore
	Now                                       func() time.Time
	Sleep                                     func(time.Duration)
}

// GitHubOrgScraper implements OrgScraper using typed GitHub API clients.
type GitHubOrgScraper struct {
	clients map[string]GitHubDataClient
	cfg     GitHubOrgScraperConfig

	stateMachine githubapi.LOCStateMachine
	checkpoints  CheckpointStore
	copilotCfg   config.CopilotConfig
	enterprise   GitHubDataClient
	primaryOrg   string

	mu                    sync.Mutex
	locStateByRepo        map[string]githubapi.LOCState
	fallbackOrgBudgetByHr map[string]orgFallbackBudget
}

type orgFallbackBudget struct {
	windowStart time.Time
	used        int
}

type repositoryOutcome struct {
	metrics      []store.MetricPoint
	missed       []MissedWindow
	summaryDelta repoSummaryDelta
}

type orgRateLimitSummary struct {
	mu           sync.Mutex
	minRemaining int
	resetUnix    int64
	secondaryHit int
	requests     map[string]int
}

type repoSummaryDelta struct {
	processed          int
	statsAccepted      int
	statsForbidden     int
	statsNotFound      int
	statsConflict      int
	statsUnprocessable int
	statsUnavailable   int
	noCompleteWeek     int
	fallbackUsed       int
	fallbackTruncated  int
	fallbackBudgetHits int
}

// NewGitHubOrgScraper creates a production org scraper over per-org GitHub clients.
func NewGitHubOrgScraper(clients map[string]GitHubDataClient, cfg GitHubOrgScraperConfig) *GitHubOrgScraper {
	normalizedClients := make(map[string]GitHubDataClient, len(clients))
	for org, client := range clients {
		normalizedClients[strings.TrimSpace(org)] = client
	}

	if cfg.LOCRefreshInterval <= 0 {
		cfg.LOCRefreshInterval = 24 * time.Hour
	}
	if cfg.FallbackMaxCommitsPerRepoPerWeek <= 0 {
		cfg.FallbackMaxCommitsPerRepoPerWeek = 500
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Sleep == nil {
		cfg.Sleep = time.Sleep
	}

	return &GitHubOrgScraper{
		clients: normalizedClients,
		cfg:     cfg,
		stateMachine: githubapi.LOCStateMachine{
			RefreshInterval:       cfg.LOCRefreshInterval,
			StaleWindowMultiplier: 2,
			ZeroDetectionWindows:  cfg.LargeRepoZeroDetectionWindows,
			FallbackCooldown:      cfg.LargeRepoCooldown,
		},
		checkpoints:           cfg.Checkpoints,
		copilotCfg:            cfg.Copilot,
		enterprise:            cfg.CopilotEnterpriseClient,
		primaryOrg:            strings.TrimSpace(cfg.CopilotEnterprisePrimaryOrg),
		locStateByRepo:        make(map[string]githubapi.LOCState),
		fallbackOrgBudgetByHr: make(map[string]orgFallbackBudget),
	}
}

// SetCheckpointStore injects or replaces checkpoint persistence for the scraper.
func (s *GitHubOrgScraper) SetCheckpointStore(checkpoints CheckpointStore) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpoints = checkpoints
}

// ScrapeOrg scrapes one organization and returns metrics plus missed windows for partial failures.
func (s *GitHubOrgScraper) ScrapeOrg(ctx context.Context, org config.GitHubOrgConfig) (OrgResult, error) {
	if s == nil {
		return OrgResult{}, fmt.Errorf("github org scraper is nil")
	}

	orgName := strings.TrimSpace(org.Org)
	client, ok := s.clients[orgName]
	if !ok || client == nil {
		return OrgResult{}, fmt.Errorf("no github client configured for org %q", orgName)
	}
	rateSummary := orgRateLimitSummary{
		minRemaining: -1,
		requests:     make(map[string]int),
	}

	reposResult, err := client.ListOrgRepos(ctx, orgName)
	if err != nil {
		rateSummary.observeRequest("list_org_repos", "error")
		return OrgResult{}, fmt.Errorf("list org repos for %q: %w", orgName, err)
	}
	rateSummary.observeRequest("list_org_repos", endpointStatusClass(reposResult.Status))
	s.applyRateLimitPacing(reposResult.Metadata, &rateSummary)

	if reposResult.Status != githubapi.EndpointStatusOK {
		return OrgResult{}, fmt.Errorf("list org repos for %q returned status %q", orgName, reposResult.Status)
	}

	result := OrgResult{
		Summary: OrgSummary{
			ReposDiscovered: len(reposResult.Repos),
		},
	}

	repos := filterRepositories(reposResult.Repos, org.RepoAllowlist)
	result.Summary.ReposTargeted = len(repos)
	if len(repos) == 0 {
		copilotOutcome := s.scrapeCopilotForOrg(ctx, orgName, client, &rateSummary)
		result.Metrics = append(result.Metrics, copilotOutcome.metrics...)
		result.MissedWindow = append(result.MissedWindow, copilotOutcome.missed...)
		minRemaining, resetUnix, secondaryHits, requestTotals := rateSummary.snapshot()
		result.Summary.MissedWindows = len(result.MissedWindow)
		result.Summary.MetricsProduced = len(result.Metrics)
		result.Summary.RateLimitMinRemaining = minRemaining
		result.Summary.RateLimitResetUnix = resetUnix
		result.Summary.SecondaryLimitHits = secondaryHits
		result.Summary.GitHubRequestTotals = requestTotals
		return result, nil
	}

	workerCount := org.PerOrgConcurrency
	if workerCount <= 0 {
		workerCount = 1
	}

	jobs := make(chan githubapi.Repository, len(repos))
	outcomes := make(chan repositoryOutcome, len(repos))

	var wg sync.WaitGroup
	for range workerCount {
		wg.Go(func() {
			for repo := range jobs {
				outcome := s.scrapeRepository(ctx, client, orgName, repo.Name, &rateSummary)
				outcomes <- outcome
			}
		})
	}

	for _, repo := range repos {
		jobs <- repo
	}
	close(jobs)

	wg.Wait()
	close(outcomes)

	for outcome := range outcomes {
		result.Metrics = append(result.Metrics, outcome.metrics...)
		result.MissedWindow = append(result.MissedWindow, outcome.missed...)
		result.Summary.ReposProcessed += outcome.summaryDelta.processed
		result.Summary.ReposStatsAccepted += outcome.summaryDelta.statsAccepted
		result.Summary.ReposStatsForbidden += outcome.summaryDelta.statsForbidden
		result.Summary.ReposStatsNotFound += outcome.summaryDelta.statsNotFound
		result.Summary.ReposStatsConflict += outcome.summaryDelta.statsConflict
		result.Summary.ReposStatsUnprocessable += outcome.summaryDelta.statsUnprocessable
		result.Summary.ReposStatsUnavailable += outcome.summaryDelta.statsUnavailable
		result.Summary.ReposNoCompleteWeek += outcome.summaryDelta.noCompleteWeek
		result.Summary.ReposFallbackUsed += outcome.summaryDelta.fallbackUsed
		result.Summary.ReposFallbackTruncated += outcome.summaryDelta.fallbackTruncated
		result.Summary.LOCFallbackBudgetHits += outcome.summaryDelta.fallbackBudgetHits
	}

	copilotOutcome := s.scrapeCopilotForOrg(ctx, orgName, client, &rateSummary)
	result.Metrics = append(result.Metrics, copilotOutcome.metrics...)
	result.MissedWindow = append(result.MissedWindow, copilotOutcome.missed...)

	result.Summary.MissedWindows = len(result.MissedWindow)
	result.Summary.MetricsProduced = len(result.Metrics)
	minRemaining, resetUnix, secondaryHits, requestTotals := rateSummary.snapshot()
	result.Summary.RateLimitMinRemaining = minRemaining
	result.Summary.RateLimitResetUnix = resetUnix
	result.Summary.SecondaryLimitHits = secondaryHits
	result.Summary.GitHubRequestTotals = requestTotals

	return result, nil
}

func (s *GitHubOrgScraper) scrapeRepository(
	ctx context.Context,
	client GitHubDataClient,
	org string,
	repo string,
	rateSummary *orgRateLimitSummary,
) repositoryOutcome {
	now := s.cfg.Now().UTC()
	locWindowStart := now.Add(-7 * 24 * time.Hour)
	activityWindowStart := now.Add(-24 * time.Hour)
	windowEnd := now
	if checkpoint, found := s.readCheckpoint(org, repo); found {
		if checkpoint.Before(activityWindowStart) {
			activityWindowStart = checkpoint
		}
		if checkpoint.Before(locWindowStart) {
			locWindowStart = checkpoint
		}
	}
	outcome := repositoryOutcome{
		summaryDelta: repoSummaryDelta{processed: 1},
	}

	activityOutcome := s.scrapeRepositoryActivity(
		ctx,
		client,
		org,
		repo,
		activityWindowStart,
		windowEnd,
		rateSummary,
	)
	locOutcome := s.scrapeRepositoryLOC(ctx, client, org, repo, locWindowStart, windowEnd, now, rateSummary)

	merged := mergeRepositoryOutcome(outcome, mergeRepositoryOutcome(activityOutcome, locOutcome))
	if len(merged.missed) == 0 {
		s.advanceCheckpoint(org, repo, windowEnd)
	}
	return merged
}

func (s *GitHubOrgScraper) scrapeRepositoryLOC(
	ctx context.Context,
	client GitHubDataClient,
	org string,
	repo string,
	windowStart time.Time,
	windowEnd time.Time,
	observedAt time.Time,
	rateSummary *orgRateLimitSummary,
) repositoryOutcome {
	outcome := repositoryOutcome{}

	statsResult, err := client.GetContributorStats(ctx, org, repo)
	if err != nil {
		rateSummary.observeRequest("get_contributor_stats", "error")
		outcome.summaryDelta.statsUnavailable++
		return mergeRepositoryOutcome(outcome, repositoryOutcome{
			missed: []MissedWindow{{
				Org:         org,
				Repo:        repo,
				WindowStart: windowStart,
				WindowEnd:   windowEnd,
				Reason:      repoMissReasonContributorStats,
			}},
		})
	}
	rateSummary.observeRequest("get_contributor_stats", endpointStatusClass(statsResult.Status))
	s.applyRateLimitPacing(statsResult.Metadata, rateSummary)

	switch statsResult.Status {
	case githubapi.EndpointStatusAccepted:
		outcome.summaryDelta.statsAccepted++
		s.recordLOCState(org, repo, githubapi.LOCEvent{
			ObservedAt: observedAt,
			HTTPStatus: 202,
		})
		return outcome
	case githubapi.EndpointStatusForbidden, githubapi.EndpointStatusUnavailable, githubapi.EndpointStatusUnknown:
		if statsResult.Status == githubapi.EndpointStatusForbidden {
			outcome.summaryDelta.statsForbidden++
		} else {
			outcome.summaryDelta.statsUnavailable++
		}
		return mergeRepositoryOutcome(outcome, repositoryOutcome{
			missed: []MissedWindow{{
				Org:         org,
				Repo:        repo,
				WindowStart: windowStart,
				WindowEnd:   windowEnd,
				Reason:      repoMissReasonContributorStats,
			}},
		})
	case githubapi.EndpointStatusNotFound, githubapi.EndpointStatusConflict, githubapi.EndpointStatusUnprocessable:
		switch statsResult.Status {
		case githubapi.EndpointStatusNotFound:
			outcome.summaryDelta.statsNotFound++
		case githubapi.EndpointStatusConflict:
			outcome.summaryDelta.statsConflict++
		case githubapi.EndpointStatusUnprocessable:
			outcome.summaryDelta.statsUnprocessable++
		}
		return outcome
	case githubapi.EndpointStatusOK:
		// Continue below.
	default:
		outcome.summaryDelta.statsUnavailable++
		return mergeRepositoryOutcome(outcome, repositoryOutcome{
			missed: []MissedWindow{{
				Org:         org,
				Repo:        repo,
				WindowStart: windowStart,
				WindowEnd:   windowEnd,
				Reason:      repoMissReasonContributorStats,
			}},
		})
	}

	weeklyByUser, _, contributionsNonZero, additionsDeletionsZero := extractLatestCompleteWeek(statsResult.Contributors, observedAt)
	state := s.recordLOCState(org, repo, githubapi.LOCEvent{
		ObservedAt:             observedAt,
		HTTPStatus:             200,
		StatsPresent:           true,
		ContributionsNonZero:   contributionsNonZero,
		AdditionsDeletionsZero: additionsDeletionsZero,
	})

	if len(weeklyByUser) == 0 {
		outcome.summaryDelta.noCompleteWeek++
	}

	if s.cfg.FallbackEnabled && state.Mode == githubapi.LOCModeFallback {
		fallbackOutcome := s.scrapeRepositoryFallback(ctx, client, org, repo, windowStart, windowEnd, rateSummary)
		fallbackOutcome.summaryDelta.fallbackUsed++
		fallbackIncomplete := 0.0
		if fallbackOutcome.summaryDelta.fallbackTruncated > 0 {
			fallbackIncomplete = 1
		}
		fallbackOutcome.metrics = append(
			fallbackOutcome.metrics,
			buildLOCSourceModeMetrics(org, repo, locModeSampledCommitStats, fallbackIncomplete, observedAt)...,
		)
		return mergeRepositoryOutcome(outcome, fallbackOutcome)
	}

	metrics := make([]store.MetricPoint, 0, len(weeklyByUser)*2)
	for user, weekly := range weeklyByUser {
		addMetric, addErr := NewProductivityMetric(
			MetricActivityLOCAddedWeekly,
			org,
			repo,
			user,
			float64(weekly.added),
			observedAt,
		)
		if addErr == nil {
			metrics = append(metrics, addMetric)
		}
		removedMetric, removeErr := NewProductivityMetric(
			MetricActivityLOCRemovedWeekly,
			org,
			repo,
			user,
			float64(weekly.removed),
			observedAt,
		)
		if removeErr == nil {
			metrics = append(metrics, removedMetric)
		}
	}
	metrics = append(
		metrics,
		buildLOCSourceModeMetrics(org, repo, locModeStatsContributors, 0, observedAt)...,
	)

	return mergeRepositoryOutcome(outcome, repositoryOutcome{metrics: metrics})
}

func (s *GitHubOrgScraper) scrapeRepositoryActivity(
	ctx context.Context,
	client GitHubDataClient,
	org string,
	repo string,
	windowStart time.Time,
	windowEnd time.Time,
	rateSummary *orgRateLimitSummary,
) repositoryOutcome {
	metrics := make([]store.MetricPoint, 0, 16)
	missed := make([]MissedWindow, 0, 4)
	summary := repoSummaryDelta{}

	commitsByUser := make(map[string]int)
	prsOpenedByUser := make(map[string]int)
	prsMergedByUser := make(map[string]int)
	reviewsByUser := make(map[string]int)
	issueCommentsByUser := make(map[string]int)
	lastEventByUser := make(map[string]time.Time)

	addMissedWindow := func(reason string) {
		missed = append(missed, MissedWindow{
			Org:         org,
			Repo:        repo,
			WindowStart: windowStart,
			WindowEnd:   windowEnd,
			Reason:      reason,
		})
	}

	commitsResult, err := client.ListRepoCommitsWindow(ctx, org, repo, windowStart, windowEnd, 0)
	if err != nil {
		rateSummary.observeRequest("list_repo_commits_window", "error")
		summary.statsUnavailable++
		addMissedWindow(repoMissReasonActivityCommits)
	} else {
		rateSummary.observeRequest("list_repo_commits_window", endpointStatusClass(commitsResult.Status))
		s.applyRateLimitPacing(commitsResult.Metadata, rateSummary)
		switch commitsResult.Status {
		case githubapi.EndpointStatusOK:
			for _, commit := range commitsResult.Commits {
				user := resolveCommitActor(commit)
				commitsByUser[user]++
				if commit.CommittedAt.After(lastEventByUser[user]) {
					lastEventByUser[user] = commit.CommittedAt
				}
			}
		case githubapi.EndpointStatusForbidden:
			summary.statsForbidden++
			addMissedWindow(repoMissReasonActivityCommits)
		case githubapi.EndpointStatusNotFound:
			summary.statsNotFound++
		case githubapi.EndpointStatusConflict:
			summary.statsConflict++
		case githubapi.EndpointStatusUnprocessable:
			summary.statsUnprocessable++
		default:
			summary.statsUnavailable++
			addMissedWindow(repoMissReasonActivityCommits)
		}
	}

	pullsResult, err := client.ListRepoPullRequestsWindow(ctx, org, repo, windowStart, windowEnd)
	if err != nil {
		rateSummary.observeRequest("list_repo_pull_requests_window", "error")
		summary.statsUnavailable++
		addMissedWindow(repoMissReasonActivityPulls)
	} else {
		rateSummary.observeRequest("list_repo_pull_requests_window", endpointStatusClass(pullsResult.Status))
		s.applyRateLimitPacing(pullsResult.Metadata, rateSummary)
		switch pullsResult.Status {
		case githubapi.EndpointStatusOK:
			for _, pr := range pullsResult.PullRequests {
				user := normalizeActor(pr.User)
				if withinWindow(pr.CreatedAt, windowStart, windowEnd) {
					prsOpenedByUser[user]++
				}
				if withinWindow(pr.MergedAt, windowStart, windowEnd) {
					prsMergedByUser[user]++
				}
				latest := maxTime(pr.UpdatedAt, maxTime(pr.CreatedAt, pr.MergedAt))
				if latest.After(lastEventByUser[user]) {
					lastEventByUser[user] = latest
				}

				reviewsResult, reviewErr := client.ListPullReviews(ctx, org, repo, pr.Number, windowStart, windowEnd)
				if reviewErr != nil {
					rateSummary.observeRequest("list_pull_reviews", "error")
					summary.statsUnavailable++
					addMissedWindow(repoMissReasonActivityReviews)
					continue
				}
				rateSummary.observeRequest("list_pull_reviews", endpointStatusClass(reviewsResult.Status))
				s.applyRateLimitPacing(reviewsResult.Metadata, rateSummary)

				switch reviewsResult.Status {
				case githubapi.EndpointStatusOK:
					for _, review := range reviewsResult.Reviews {
						reviewer := normalizeActor(review.User)
						reviewsByUser[reviewer]++
						if review.SubmittedAt.After(lastEventByUser[reviewer]) {
							lastEventByUser[reviewer] = review.SubmittedAt
						}
					}
				case githubapi.EndpointStatusNotFound, githubapi.EndpointStatusConflict, githubapi.EndpointStatusUnprocessable:
					continue
				case githubapi.EndpointStatusForbidden:
					summary.statsForbidden++
					addMissedWindow(repoMissReasonActivityReviews)
				default:
					summary.statsUnavailable++
					addMissedWindow(repoMissReasonActivityReviews)
				}
			}
		case githubapi.EndpointStatusForbidden:
			summary.statsForbidden++
			addMissedWindow(repoMissReasonActivityPulls)
		case githubapi.EndpointStatusNotFound:
			summary.statsNotFound++
		case githubapi.EndpointStatusConflict:
			summary.statsConflict++
		case githubapi.EndpointStatusUnprocessable:
			summary.statsUnprocessable++
		default:
			summary.statsUnavailable++
			addMissedWindow(repoMissReasonActivityPulls)
		}
	}

	commentsResult, err := client.ListIssueCommentsWindow(ctx, org, repo, windowStart, windowEnd)
	if err != nil {
		rateSummary.observeRequest("list_issue_comments", "error")
		summary.statsUnavailable++
		addMissedWindow(repoMissReasonActivityComments)
	} else {
		rateSummary.observeRequest("list_issue_comments", endpointStatusClass(commentsResult.Status))
		s.applyRateLimitPacing(commentsResult.Metadata, rateSummary)
		switch commentsResult.Status {
		case githubapi.EndpointStatusOK:
			for _, comment := range commentsResult.Comments {
				user := normalizeActor(comment.User)
				issueCommentsByUser[user]++
				if comment.CreatedAt.After(lastEventByUser[user]) {
					lastEventByUser[user] = comment.CreatedAt
				}
			}
		case githubapi.EndpointStatusForbidden:
			summary.statsForbidden++
			addMissedWindow(repoMissReasonActivityComments)
		case githubapi.EndpointStatusNotFound:
			summary.statsNotFound++
		case githubapi.EndpointStatusConflict:
			summary.statsConflict++
		case githubapi.EndpointStatusUnprocessable:
			summary.statsUnprocessable++
		default:
			summary.statsUnavailable++
			addMissedWindow(repoMissReasonActivityComments)
		}
	}

	for user, count := range commitsByUser {
		point, metricErr := NewProductivityMetric(MetricActivityCommits24h, org, repo, user, float64(count), windowEnd)
		if metricErr == nil {
			metrics = append(metrics, point)
		}
	}
	for user, count := range prsOpenedByUser {
		point, metricErr := NewProductivityMetric(MetricActivityPROpened24h, org, repo, user, float64(count), windowEnd)
		if metricErr == nil {
			metrics = append(metrics, point)
		}
	}
	for user, count := range prsMergedByUser {
		point, metricErr := NewProductivityMetric(MetricActivityPRMerged24h, org, repo, user, float64(count), windowEnd)
		if metricErr == nil {
			metrics = append(metrics, point)
		}
	}
	for user, count := range reviewsByUser {
		point, metricErr := NewProductivityMetric(MetricActivityReviewsSubmitted24h, org, repo, user, float64(count), windowEnd)
		if metricErr == nil {
			metrics = append(metrics, point)
		}
	}
	for user, count := range issueCommentsByUser {
		point, metricErr := NewProductivityMetric(MetricActivityIssueComments24h, org, repo, user, float64(count), windowEnd)
		if metricErr == nil {
			metrics = append(metrics, point)
		}
	}
	for user, ts := range lastEventByUser {
		point, metricErr := NewProductivityMetric(MetricActivityLastEventUnixTime, org, repo, user, float64(ts.Unix()), windowEnd)
		if metricErr == nil {
			metrics = append(metrics, point)
		}
	}

	return repositoryOutcome{
		metrics:      metrics,
		missed:       dedupeMissedWindows(missed),
		summaryDelta: summary,
	}
}

func (s *GitHubOrgScraper) scrapeRepositoryFallback(
	ctx context.Context,
	client GitHubDataClient,
	org string,
	repo string,
	windowStart time.Time,
	windowEnd time.Time,
	rateSummary *orgRateLimitSummary,
) repositoryOutcome {
	summary := repoSummaryDelta{}

	commitListResult, err := client.ListRepoCommitsWindow(ctx, org, repo, windowStart, windowEnd, s.cfg.FallbackMaxCommitsPerRepoPerWeek)
	if err != nil {
		rateSummary.observeRequest("list_repo_commits_window", "error")
		summary.statsUnavailable++
		return repositoryOutcome{
			missed: []MissedWindow{{
				Org:         org,
				Repo:        repo,
				WindowStart: windowStart,
				WindowEnd:   windowEnd,
				Reason:      repoMissReasonFallbackList,
			}},
			summaryDelta: summary,
		}
	}
	rateSummary.observeRequest("list_repo_commits_window", endpointStatusClass(commitListResult.Status))
	s.applyRateLimitPacing(commitListResult.Metadata, rateSummary)

	switch commitListResult.Status {
	case githubapi.EndpointStatusOK:
		// continue
	case githubapi.EndpointStatusNotFound, githubapi.EndpointStatusConflict, githubapi.EndpointStatusUnprocessable:
		return repositoryOutcome{summaryDelta: summary}
	default:
		summary.statsUnavailable++
		return repositoryOutcome{
			missed: []MissedWindow{{
				Org:         org,
				Repo:        repo,
				WindowStart: windowStart,
				WindowEnd:   windowEnd,
				Reason:      repoMissReasonFallbackList,
			}},
			summaryDelta: summary,
		}
	}

	allowedDetails, budgetExhausted := s.consumeFallbackBudget(org, len(commitListResult.Commits), windowEnd)
	if allowedDetails < len(commitListResult.Commits) {
		commitListResult.Commits = commitListResult.Commits[:allowedDetails]
		commitListResult.Truncated = true
		if budgetExhausted {
			summary.fallbackBudgetHits++
		}
	}

	addedByUser := make(map[string]int)
	removedByUser := make(map[string]int)
	missed := make([]MissedWindow, 0, 1)

	for _, commit := range commitListResult.Commits {
		detailResult, err := client.GetCommit(ctx, org, repo, commit.SHA)
		if err != nil {
			rateSummary.observeRequest("get_commit", "error")
			summary.statsUnavailable++
			missed = append(missed, MissedWindow{
				Org:         org,
				Repo:        repo,
				WindowStart: windowStart,
				WindowEnd:   windowEnd,
				Reason:      repoMissReasonFallbackDetail,
			})
			continue
		}
		rateSummary.observeRequest("get_commit", endpointStatusClass(detailResult.Status))
		s.applyRateLimitPacing(detailResult.Metadata, rateSummary)

		switch detailResult.Status {
		case githubapi.EndpointStatusOK:
			// continue
		case githubapi.EndpointStatusNotFound, githubapi.EndpointStatusConflict, githubapi.EndpointStatusUnprocessable:
			continue
		default:
			summary.statsUnavailable++
			missed = append(missed, MissedWindow{
				Org:         org,
				Repo:        repo,
				WindowStart: windowStart,
				WindowEnd:   windowEnd,
				Reason:      repoMissReasonFallbackDetail,
			})
			continue
		}

		user := detailResult.Author
		if strings.TrimSpace(user) == "" {
			user = resolveCommitActor(commit)
		}

		addedByUser[user] += detailResult.Additions
		removedByUser[user] += detailResult.Deletions
	}

	metrics := make([]store.MetricPoint, 0, len(addedByUser)*2)
	for user, added := range addedByUser {
		addMetric, err := NewProductivityMetric(MetricActivityLOCAddedWeekly, org, repo, user, float64(added), windowEnd)
		if err == nil {
			metrics = append(metrics, addMetric)
		}
		removedMetric, err := NewProductivityMetric(MetricActivityLOCRemovedWeekly, org, repo, user, float64(removedByUser[user]), windowEnd)
		if err == nil {
			metrics = append(metrics, removedMetric)
		}
	}

	if commitListResult.Truncated {
		summary.fallbackTruncated++
		missed = append(missed, MissedWindow{
			Org:         org,
			Repo:        repo,
			WindowStart: windowStart,
			WindowEnd:   windowEnd,
			Reason:      "fallback_truncated",
		})
	}

	return repositoryOutcome{
		metrics:      metrics,
		missed:       missed,
		summaryDelta: summary,
	}
}

// ScrapeBackfill re-scrapes one missed org/repo window.
func (s *GitHubOrgScraper) ScrapeBackfill(
	ctx context.Context,
	org config.GitHubOrgConfig,
	repo string,
	windowStart time.Time,
	windowEnd time.Time,
	reason string,
) (OrgResult, error) {
	if s == nil {
		return OrgResult{}, fmt.Errorf("github org scraper is nil")
	}

	orgName := strings.TrimSpace(org.Org)
	client, ok := s.clients[orgName]
	if !ok || client == nil {
		return OrgResult{}, fmt.Errorf("no github client configured for org %q", orgName)
	}

	now := s.cfg.Now().UTC()
	if windowStart.IsZero() {
		windowStart = now.Add(-24 * time.Hour)
	}
	if windowEnd.IsZero() {
		windowEnd = now
	}
	if windowEnd.Before(windowStart) {
		return OrgResult{}, fmt.Errorf("window_end must be >= window_start")
	}

	rateSummary := orgRateLimitSummary{
		minRemaining: -1,
		requests:     make(map[string]int),
	}
	result := OrgResult{}
	repoName := strings.TrimSpace(repo)
	if strings.HasPrefix(repoName, "__copilot__") || strings.HasPrefix(strings.TrimSpace(reason), "copilot_report_") {
		copilotRepoKey := repoName
		if copilotRepoKey == "" || copilotRepoKey == "*" {
			copilotRepoKey = copilotCheckpointRepoKey("org", "28d")
		}
		copilotOutcome := s.scrapeCopilotBackfill(
			ctx,
			orgName,
			client,
			copilotRepoKey,
			windowStart,
			windowEnd,
			&rateSummary,
		)
		result.Metrics = append(result.Metrics, copilotOutcome.metrics...)
		result.MissedWindow = append(result.MissedWindow, copilotOutcome.missed...)
		result.Summary.MissedWindows = len(result.MissedWindow)
		result.Summary.MetricsProduced = len(result.Metrics)
		minRemaining, resetUnix, secondaryHits, requestTotals := rateSummary.snapshot()
		result.Summary.RateLimitMinRemaining = minRemaining
		result.Summary.RateLimitResetUnix = resetUnix
		result.Summary.SecondaryLimitHits = secondaryHits
		result.Summary.GitHubRequestTotals = requestTotals
		return result, nil
	}

	repoNames := make([]string, 0, 1)

	if repoName == "" || repoName == "*" {
		reposResult, err := client.ListOrgRepos(ctx, orgName)
		if err != nil {
			rateSummary.observeRequest("list_org_repos", "error")
			return OrgResult{}, fmt.Errorf("list org repos for %q: %w", orgName, err)
		}
		rateSummary.observeRequest("list_org_repos", endpointStatusClass(reposResult.Status))
		s.applyRateLimitPacing(reposResult.Metadata, &rateSummary)
		if reposResult.Status != githubapi.EndpointStatusOK {
			return OrgResult{}, fmt.Errorf("list org repos for %q returned status %q", orgName, reposResult.Status)
		}

		result.Summary.ReposDiscovered = len(reposResult.Repos)
		repos := filterRepositories(reposResult.Repos, org.RepoAllowlist)
		result.Summary.ReposTargeted = len(repos)
		for _, discovered := range repos {
			repoNames = append(repoNames, discovered.Name)
		}
	} else {
		repoNames = append(repoNames, repoName)
		result.Summary.ReposDiscovered = 1
		result.Summary.ReposTargeted = 1
	}

	for _, repository := range repoNames {
		outcome := s.scrapeRepositoryBackfill(ctx, client, orgName, repository, windowStart, windowEnd, reason, &rateSummary)
		result.Metrics = append(result.Metrics, outcome.metrics...)
		result.MissedWindow = append(result.MissedWindow, outcome.missed...)
		result.Summary.ReposProcessed += outcome.summaryDelta.processed
		result.Summary.ReposStatsAccepted += outcome.summaryDelta.statsAccepted
		result.Summary.ReposStatsForbidden += outcome.summaryDelta.statsForbidden
		result.Summary.ReposStatsNotFound += outcome.summaryDelta.statsNotFound
		result.Summary.ReposStatsConflict += outcome.summaryDelta.statsConflict
		result.Summary.ReposStatsUnprocessable += outcome.summaryDelta.statsUnprocessable
		result.Summary.ReposStatsUnavailable += outcome.summaryDelta.statsUnavailable
		result.Summary.ReposNoCompleteWeek += outcome.summaryDelta.noCompleteWeek
		result.Summary.ReposFallbackUsed += outcome.summaryDelta.fallbackUsed
		result.Summary.ReposFallbackTruncated += outcome.summaryDelta.fallbackTruncated
		result.Summary.LOCFallbackBudgetHits += outcome.summaryDelta.fallbackBudgetHits
	}

	result.Summary.MissedWindows = len(result.MissedWindow)
	result.Summary.MetricsProduced = len(result.Metrics)
	minRemaining, resetUnix, secondaryHits, requestTotals := rateSummary.snapshot()
	result.Summary.RateLimitMinRemaining = minRemaining
	result.Summary.RateLimitResetUnix = resetUnix
	result.Summary.SecondaryLimitHits = secondaryHits
	result.Summary.GitHubRequestTotals = requestTotals

	return result, nil
}

func (s *GitHubOrgScraper) scrapeRepositoryBackfill(
	ctx context.Context,
	client GitHubDataClient,
	org string,
	repo string,
	windowStart time.Time,
	windowEnd time.Time,
	reason string,
	rateSummary *orgRateLimitSummary,
) repositoryOutcome {
	normalizedReason := strings.TrimSpace(reason)
	outcome := repositoryOutcome{
		summaryDelta: repoSummaryDelta{processed: 1},
	}
	now := s.cfg.Now().UTC()

	switch normalizedReason {
	case repoMissReasonActivityCommits, repoMissReasonActivityPulls, repoMissReasonActivityReviews, repoMissReasonActivityComments:
		activityOutcome := s.scrapeRepositoryActivity(ctx, client, org, repo, windowStart, windowEnd, rateSummary)
		merged := mergeRepositoryOutcome(outcome, activityOutcome)
		if len(merged.missed) == 0 && !windowEnd.IsZero() {
			s.advanceCheckpoint(org, repo, windowEnd)
		}
		return merged
	case repoMissReasonContributorStats, repoMissReasonFallbackList, repoMissReasonFallbackDetail, "fallback_truncated":
		locOutcome := s.scrapeRepositoryLOC(ctx, client, org, repo, windowStart, windowEnd, now, rateSummary)
		merged := mergeRepositoryOutcome(outcome, locOutcome)
		if len(merged.missed) == 0 && !windowEnd.IsZero() {
			s.advanceCheckpoint(org, repo, windowEnd)
		}
		return merged
	default:
		activityOutcome := s.scrapeRepositoryActivity(ctx, client, org, repo, windowStart, windowEnd, rateSummary)
		locOutcome := s.scrapeRepositoryLOC(ctx, client, org, repo, windowStart, windowEnd, now, rateSummary)
		merged := mergeRepositoryOutcome(outcome, mergeRepositoryOutcome(activityOutcome, locOutcome))
		if len(merged.missed) == 0 && !windowEnd.IsZero() {
			s.advanceCheckpoint(org, repo, windowEnd)
		}
		return merged
	}
}

func (s *GitHubOrgScraper) scrapeCopilotForOrg(
	ctx context.Context,
	org string,
	orgClient GitHubDataClient,
	rateSummary *orgRateLimitSummary,
) repositoryOutcome {
	if !s.copilotCfg.Enabled {
		return repositoryOutcome{}
	}

	now := s.cfg.Now().UTC()
	outcome := repositoryOutcome{}
	if s.copilotCfg.IncludeOrg28d {
		specOutcome := s.scrapeCopilotReport(
			ctx,
			orgClient,
			org,
			copilotScopeOrg,
			copilotWindow28d,
			copilotCheckpointRepoKey(copilotScopeOrg, copilotWindow28d),
			func(callCtx context.Context) (githubapi.CopilotReportLinkResult, error) {
				return orgClient.GetOrgCopilotOrganization28DayLatestReportLink(callCtx, org)
			},
			now,
			rateSummary,
			true,
		)
		outcome = mergeRepositoryOutcome(outcome, specOutcome)
	}
	if s.copilotCfg.IncludeOrgUsers28d {
		specOutcome := s.scrapeCopilotReport(
			ctx,
			orgClient,
			org,
			copilotScopeUsers,
			copilotWindow28d,
			copilotCheckpointRepoKey(copilotScopeUsers, copilotWindow28d),
			func(callCtx context.Context) (githubapi.CopilotReportLinkResult, error) {
				return orgClient.GetOrgCopilotUsers28DayLatestReportLink(callCtx, org)
			},
			now,
			rateSummary,
			true,
		)
		outcome = mergeRepositoryOutcome(outcome, specOutcome)
	}

	if !s.shouldScrapeEnterpriseForOrg(org) {
		outcome.missed = dedupeMissedWindows(outcome.missed)
		return outcome
	}

	enterpriseClient := s.enterprise
	if enterpriseClient == nil {
		outcome.missed = append(outcome.missed, MissedWindow{
			Org:         strings.TrimSpace(s.copilotCfg.Enterprise.Slug),
			Repo:        copilotCheckpointRepoKey(copilotScopeEnterprise, copilotWindow28d),
			WindowStart: now,
			WindowEnd:   now,
			Reason:      repoMissReasonCopilotFetch,
		})
		outcome.missed = dedupeMissedWindows(outcome.missed)
		return outcome
	}

	enterpriseSlug := strings.TrimSpace(s.copilotCfg.Enterprise.Slug)
	if s.copilotCfg.IncludeEnterprise28d {
		specOutcome := s.scrapeCopilotReport(
			ctx,
			enterpriseClient,
			enterpriseSlug,
			copilotScopeEnterprise,
			copilotWindow28d,
			copilotCheckpointRepoKey(copilotScopeEnterprise, copilotWindow28d),
			func(callCtx context.Context) (githubapi.CopilotReportLinkResult, error) {
				return enterpriseClient.GetEnterpriseCopilotEnterprise28DayLatestReportLink(callCtx, enterpriseSlug)
			},
			now,
			rateSummary,
			true,
		)
		outcome = mergeRepositoryOutcome(outcome, specOutcome)
	}
	if s.copilotCfg.IncludeEnterpriseUsers28d {
		specOutcome := s.scrapeCopilotReport(
			ctx,
			enterpriseClient,
			enterpriseSlug,
			copilotScopeUsers,
			copilotWindow28d,
			copilotCheckpointRepoKey(copilotScopeEnterpriseUsers, copilotWindow28d),
			func(callCtx context.Context) (githubapi.CopilotReportLinkResult, error) {
				return enterpriseClient.GetEnterpriseCopilotUsers28DayLatestReportLink(callCtx, enterpriseSlug)
			},
			now,
			rateSummary,
			true,
		)
		outcome = mergeRepositoryOutcome(outcome, specOutcome)
	}

	outcome.missed = dedupeMissedWindows(outcome.missed)
	return outcome
}

func (s *GitHubOrgScraper) scrapeCopilotBackfill(
	ctx context.Context,
	org string,
	orgClient GitHubDataClient,
	repoKey string,
	windowStart time.Time,
	windowEnd time.Time,
	rateSummary *orgRateLimitSummary,
) repositoryOutcome {
	if !s.copilotCfg.Enabled {
		return repositoryOutcome{}
	}
	scopeKey, _, ok := parseCopilotCheckpointRepoKey(repoKey)
	if !ok {
		return repositoryOutcome{}
	}

	now := s.cfg.Now().UTC()
	day := windowEnd
	if day.IsZero() {
		day = windowStart
	}
	if day.IsZero() {
		day = now
	}
	day = time.Date(day.UTC().Year(), day.UTC().Month(), day.UTC().Day(), 0, 0, 0, 0, time.UTC)

	switch scopeKey {
	case copilotScopeOrg:
		return s.scrapeCopilotReport(
			ctx,
			orgClient,
			org,
			copilotScopeOrg,
			copilotWindow1d,
			repoKey,
			func(callCtx context.Context) (githubapi.CopilotReportLinkResult, error) {
				return orgClient.GetOrgCopilotOrganization1DayReportLink(callCtx, org, day)
			},
			now,
			rateSummary,
			false,
		)
	case copilotScopeUsers:
		return s.scrapeCopilotReport(
			ctx,
			orgClient,
			org,
			copilotScopeUsers,
			copilotWindow1d,
			repoKey,
			func(callCtx context.Context) (githubapi.CopilotReportLinkResult, error) {
				return orgClient.GetOrgCopilotUsers1DayReportLink(callCtx, org, day)
			},
			now,
			rateSummary,
			false,
		)
	case copilotScopeEnterprise:
		if s.enterprise == nil {
			return repositoryOutcome{
				missed: []MissedWindow{{
					Org:         strings.TrimSpace(s.copilotCfg.Enterprise.Slug),
					Repo:        repoKey,
					WindowStart: day,
					WindowEnd:   day,
					Reason:      repoMissReasonCopilotFetch,
				}},
			}
		}
		enterpriseSlug := strings.TrimSpace(s.copilotCfg.Enterprise.Slug)
		return s.scrapeCopilotReport(
			ctx,
			s.enterprise,
			enterpriseSlug,
			copilotScopeEnterprise,
			copilotWindow1d,
			repoKey,
			func(callCtx context.Context) (githubapi.CopilotReportLinkResult, error) {
				return s.enterprise.GetEnterpriseCopilotEnterprise1DayReportLink(callCtx, enterpriseSlug, day)
			},
			now,
			rateSummary,
			false,
		)
	case copilotScopeEnterpriseUsers:
		if s.enterprise == nil {
			return repositoryOutcome{
				missed: []MissedWindow{{
					Org:         strings.TrimSpace(s.copilotCfg.Enterprise.Slug),
					Repo:        repoKey,
					WindowStart: day,
					WindowEnd:   day,
					Reason:      repoMissReasonCopilotFetch,
				}},
			}
		}
		enterpriseSlug := strings.TrimSpace(s.copilotCfg.Enterprise.Slug)
		return s.scrapeCopilotReport(
			ctx,
			s.enterprise,
			enterpriseSlug,
			copilotScopeUsers,
			copilotWindow1d,
			repoKey,
			func(callCtx context.Context) (githubapi.CopilotReportLinkResult, error) {
				return s.enterprise.GetEnterpriseCopilotUsers1DayReportLink(callCtx, enterpriseSlug, day)
			},
			now,
			rateSummary,
			false,
		)
	default:
		return repositoryOutcome{}
	}
}

func (s *GitHubOrgScraper) scrapeCopilotReport(
	ctx context.Context,
	client GitHubDataClient,
	orgLabel string,
	scope string,
	window string,
	repoKey string,
	fetchLink func(context.Context) (githubapi.CopilotReportLinkResult, error),
	now time.Time,
	rateSummary *orgRateLimitSummary,
	respectInterval bool,
) repositoryOutcome {
	internalMetrics := make([]store.MetricPoint, 0, 12)
	appendInternal := func(name string, value float64, labels map[string]string) {
		internalMetrics = append(internalMetrics, store.MetricPoint{
			Name:      name,
			Labels:    labels,
			Value:     value,
			UpdatedAt: now,
		})
	}

	if respectInterval && s.copilotCfg.ScrapeInterval > 0 {
		lastRunKey := copilotLastRunRepoKey(scope, window)
		if lastRunAt, found := s.readCheckpoint(orgLabel, lastRunKey); found {
			nextAllowed := lastRunAt.Add(s.copilotCfg.ScrapeInterval)
			if now.Before(nextAllowed) {
				appendInternal("gh_exporter_copilot_scrape_runs_total", 1, map[string]string{
					"scope":  scope,
					"result": "skipped_interval",
				})
				return repositoryOutcome{metrics: internalMetrics}
			}
		}
	}
	if respectInterval {
		s.advanceCheckpoint(orgLabel, copilotLastRunRepoKey(scope, window), now)
	}

	linkCtx := ctx
	if s.copilotCfg.RequestTimeout > 0 {
		var cancel context.CancelFunc
		linkCtx, cancel = context.WithTimeout(ctx, s.copilotCfg.RequestTimeout)
		defer cancel()
	}

	linkResult, err := fetchLink(linkCtx)
	if err != nil {
		if rateSummary != nil {
			rateSummary.observeRequest("copilot_report_link", "error")
		}
		appendInternal("gh_exporter_copilot_scrape_runs_total", 1, map[string]string{
			"scope":  scope,
			"result": "fetch_error",
		})
		return repositoryOutcome{
			metrics: internalMetrics,
			missed: []MissedWindow{{
				Org:         orgLabel,
				Repo:        repoKey,
				WindowStart: now,
				WindowEnd:   now,
				Reason:      repoMissReasonCopilotFetch,
			}},
		}
	}

	if rateSummary != nil {
		rateSummary.observeRequest("copilot_report_link", endpointStatusClass(linkResult.Status))
		s.applyRateLimitPacing(linkResult.Metadata, rateSummary)
	}
	if linkResult.Status != githubapi.EndpointStatusOK {
		appendInternal("gh_exporter_copilot_scrape_runs_total", 1, map[string]string{
			"scope":  scope,
			"result": "fetch_status_error",
		})
		return repositoryOutcome{
			metrics: internalMetrics,
			missed: []MissedWindow{{
				Org:         orgLabel,
				Repo:        repoKey,
				WindowStart: now,
				WindowEnd:   now,
				Reason:      repoMissReasonCopilotFetch,
			}},
		}
	}

	if !s.copilotCfg.RefreshIfReportUnchanged && !linkResult.ReportEndDay.IsZero() {
		if checkpoint, found := s.readCheckpoint(orgLabel, repoKey); found {
			if !linkResult.ReportEndDay.After(checkpoint) {
				appendInternal("gh_exporter_copilot_scrape_runs_total", 1, map[string]string{
					"scope":  scope,
					"result": "skipped_unchanged",
				})
				return repositoryOutcome{metrics: internalMetrics}
			}
		}
	}

	reportURL := strings.TrimSpace(linkResult.URL)
	if reportURL == "" && len(linkResult.DownloadLinks) > 0 {
		reportURL = strings.TrimSpace(linkResult.DownloadLinks[0])
	}
	if reportURL == "" {
		appendInternal("gh_exporter_copilot_scrape_runs_total", 1, map[string]string{
			"scope":  scope,
			"result": "fetch_error",
		})
		return repositoryOutcome{
			metrics: internalMetrics,
			missed: []MissedWindow{{
				Org:         orgLabel,
				Repo:        repoKey,
				WindowStart: now,
				WindowEnd:   now,
				Reason:      repoMissReasonCopilotFetch,
			}},
		}
	}

	metrics := make([]store.MetricPoint, 0, 32)
	recordsSeen := 0
	seenUsers := make(map[string]struct{})
	droppedByRecordLimit := 0
	droppedByUserLimit := 0
	downloadCtx := ctx
	if s.copilotCfg.DownloadTimeout > 0 {
		var cancel context.CancelFunc
		downloadCtx, cancel = context.WithTimeout(ctx, s.copilotCfg.DownloadTimeout)
		defer cancel()
	}
	streamResult, err := client.StreamCopilotReportNDJSON(
		downloadCtx,
		reportURL,
		func(record map[string]any) error {
			recordsSeen++
			if s.copilotCfg.MaxRecordsPerReport > 0 && recordsSeen > s.copilotCfg.MaxRecordsPerReport {
				droppedByRecordLimit++
				return nil
			}

			userLabel := s.resolveCopilotUserLabel(record, scope)
			if scope == copilotScopeUsers && s.copilotCfg.MaxUsersPerReport > 0 {
				if _, exists := seenUsers[userLabel]; !exists {
					if len(seenUsers) >= s.copilotCfg.MaxUsersPerReport {
						droppedByUserLimit++
						return nil
					}
					seenUsers[userLabel] = struct{}{}
				}
			}

			recordMetrics := s.buildCopilotMetricsForRecord(
				record,
				orgLabel,
				scope,
				window,
				userLabel,
				linkResult,
				now,
			)
			metrics = append(metrics, recordMetrics...)
			return nil
		},
	)
	if err != nil {
		if rateSummary != nil {
			rateSummary.observeRequest("copilot_report_download", "error")
		}
		appendInternal("gh_exporter_copilot_scrape_runs_total", 1, map[string]string{
			"scope":  scope,
			"result": "download_error",
		})
		appendInternal("gh_exporter_copilot_report_download_total", 1, map[string]string{
			"scope":  scope,
			"result": "failure",
		})
		return repositoryOutcome{
			metrics: internalMetrics,
			missed: []MissedWindow{{
				Org:         orgLabel,
				Repo:        repoKey,
				WindowStart: now,
				WindowEnd:   now,
				Reason:      repoMissReasonCopilotDownload,
			}},
		}
	}
	if rateSummary != nil {
		rateSummary.observeRequest("copilot_report_download", endpointStatusClass(streamResult.Status))
		s.applyRateLimitPacing(streamResult.Metadata, rateSummary)
	}
	if streamResult.Status != githubapi.EndpointStatusOK {
		appendInternal("gh_exporter_copilot_scrape_runs_total", 1, map[string]string{
			"scope":  scope,
			"result": "download_status_error",
		})
		appendInternal("gh_exporter_copilot_report_download_total", 1, map[string]string{
			"scope":  scope,
			"result": "failure",
		})
		return repositoryOutcome{
			metrics: internalMetrics,
			missed: []MissedWindow{{
				Org:         orgLabel,
				Repo:        repoKey,
				WindowStart: now,
				WindowEnd:   now,
				Reason:      repoMissReasonCopilotDownload,
			}},
		}
	}

	outcome := repositoryOutcome{
		metrics: metrics,
	}
	appendInternal("gh_exporter_copilot_report_download_total", 1, map[string]string{
		"scope":  scope,
		"result": "success",
	})
	appendInternal("gh_exporter_copilot_records_parsed_total", float64(streamResult.RecordsParsed), map[string]string{
		"scope": scope,
	})
	if droppedByRecordLimit > 0 {
		appendInternal("gh_exporter_copilot_records_dropped_total", float64(droppedByRecordLimit), map[string]string{
			"scope":  scope,
			"reason": "max_records_per_report",
		})
	}
	if droppedByUserLimit > 0 {
		appendInternal("gh_exporter_copilot_records_dropped_total", float64(droppedByUserLimit), map[string]string{
			"scope":  scope,
			"reason": "max_users_per_report",
		})
	}
	appendInternal("gh_exporter_copilot_last_success_unixtime", float64(now.Unix()), map[string]string{
		"scope": scope,
	})
	if !linkResult.ReportEndDay.IsZero() {
		appendInternal("gh_exporter_copilot_report_staleness_seconds", now.Sub(linkResult.ReportEndDay).Seconds(), map[string]string{
			"scope": scope,
		})
	}
	if streamResult.ParseErrors > 0 && streamResult.RecordsParsed == 0 {
		appendInternal("gh_exporter_copilot_scrape_runs_total", 1, map[string]string{
			"scope":  scope,
			"result": "parse_error",
		})
		outcome.missed = append(outcome.missed, MissedWindow{
			Org:         orgLabel,
			Repo:        repoKey,
			WindowStart: now,
			WindowEnd:   now,
			Reason:      repoMissReasonCopilotParse,
		})
	} else {
		appendInternal("gh_exporter_copilot_scrape_runs_total", 1, map[string]string{
			"scope":  scope,
			"result": "success",
		})
	}
	if !linkResult.ReportEndDay.IsZero() {
		s.advanceCheckpoint(orgLabel, repoKey, linkResult.ReportEndDay)
	}
	outcome.metrics = append(outcome.metrics, internalMetrics...)
	return outcome
}

func (s *GitHubOrgScraper) shouldScrapeEnterpriseForOrg(org string) bool {
	if !s.copilotCfg.Enabled || !s.copilotCfg.Enterprise.Enabled {
		return false
	}
	if !s.copilotCfg.IncludeEnterprise28d && !s.copilotCfg.IncludeEnterpriseUsers28d {
		return false
	}
	primary := strings.TrimSpace(s.primaryOrg)
	if primary == "" {
		return false
	}
	return strings.TrimSpace(org) == primary
}

func (s *GitHubOrgScraper) resolveCopilotUserLabel(record map[string]any, scope string) string {
	if scope != copilotScopeUsers {
		return "all"
	}

	mode := strings.TrimSpace(s.copilotCfg.UserLabelMode)
	if mode == "" {
		mode = "login"
	}
	login := firstString(record, "user_login", "user", "login", "username")
	userID := firstString(record, "user_id", "id")
	switch mode {
	case "id":
		if strings.TrimSpace(userID) != "" {
			return normalizeActor(userID)
		}
		if strings.TrimSpace(login) != "" {
			return normalizeActor(login)
		}
	case "hashed":
		seed := strings.TrimSpace(login)
		if seed == "" {
			seed = strings.TrimSpace(userID)
		}
		if seed == "" {
			return UnknownLabelValue
		}
		sum := sha256.Sum256([]byte(seed))
		return hex.EncodeToString(sum[:])
	case "none":
		return "redacted"
	default:
		if strings.TrimSpace(login) != "" {
			return normalizeActor(login)
		}
		if strings.TrimSpace(userID) != "" {
			return normalizeActor(userID)
		}
	}
	return UnknownLabelValue
}

func (s *GitHubOrgScraper) buildCopilotMetricsForRecord(
	record map[string]any,
	org string,
	scope string,
	window string,
	user string,
	linkResult githubapi.CopilotReportLinkResult,
	observedAt time.Time,
) []store.MetricPoint {
	labels := map[string]string{
		LabelScope:  scope,
		LabelWindow: window,
	}
	if dayLabel := s.copilotDayLabel(record, linkResult); dayLabel != "" {
		labels[LabelDay] = dayLabel
	}
	if s.copilotCfg.IncludeBreakdownFeature {
		if feature := firstString(record, LabelFeature); feature != "" {
			labels[LabelFeature] = feature
		}
	}
	if s.copilotCfg.IncludeBreakdownIDE {
		if ide := firstString(record, LabelIDE, "editor_name"); ide != "" {
			labels[LabelIDE] = ide
		}
	}
	if s.copilotCfg.IncludeBreakdownLanguage {
		if language := firstString(record, LabelLanguage); language != "" {
			labels[LabelLanguage] = language
		}
	}
	if s.copilotCfg.IncludeBreakdownModel {
		if model := firstString(record, LabelModel); model != "" {
			labels[LabelModel] = model
		}
	}
	if chatMode := firstString(record, LabelChatMode); chatMode != "" {
		labels[LabelChatMode] = chatMode
	}

	type metricSpec struct {
		name   string
		value  float64
		exists bool
	}
	metricSpecs := []metricSpec{
		{
			name:   MetricCopilotUserInitiatedInteractions,
			value:  sumNumbers(record, "user_initiated_interaction_count", "total_chats", "total_chat_copy_events", "total_chat_insertion_events"),
			exists: hasAnyNumber(record, "user_initiated_interaction_count", "total_chats", "total_chat_copy_events", "total_chat_insertion_events"),
		},
		{
			name:   MetricCopilotCodeGenerationActivity,
			value:  firstNumberOrZero(record, "code_generation_activity_count", "total_code_suggestions"),
			exists: hasAnyNumber(record, "code_generation_activity_count", "total_code_suggestions"),
		},
		{
			name:   MetricCopilotCodeAcceptanceActivity,
			value:  firstNumberOrZero(record, "code_acceptance_activity_count", "total_code_acceptances"),
			exists: hasAnyNumber(record, "code_acceptance_activity_count", "total_code_acceptances"),
		},
		{
			name:   MetricCopilotLOCSuggestedToAdd,
			value:  firstNumberOrZero(record, "loc_suggested_to_add_sum", "total_code_lines_suggested", "total_code_lines_suggested_to_add"),
			exists: hasAnyNumber(record, "loc_suggested_to_add_sum", "total_code_lines_suggested", "total_code_lines_suggested_to_add"),
		},
		{
			name:   MetricCopilotLOCSuggestedToDelete,
			value:  firstNumberOrZero(record, "loc_suggested_to_delete_sum", "total_code_lines_suggested_to_delete"),
			exists: hasAnyNumber(record, "loc_suggested_to_delete_sum", "total_code_lines_suggested_to_delete"),
		},
		{
			name:   MetricCopilotLOCAdded,
			value:  firstNumberOrZero(record, "loc_added_sum", "total_code_lines_accepted", "total_code_lines_added"),
			exists: hasAnyNumber(record, "loc_added_sum", "total_code_lines_accepted", "total_code_lines_added"),
		},
		{
			name:   MetricCopilotLOCDeleted,
			value:  firstNumberOrZero(record, "loc_deleted_sum", "total_code_lines_deleted", "total_code_lines_removed"),
			exists: hasAnyNumber(record, "loc_deleted_sum", "total_code_lines_deleted", "total_code_lines_removed"),
		},
	}
	if s.copilotCfg.IncludePullRequestActivity {
		metricSpecs = append(metricSpecs,
			metricSpec{
				name:   MetricCopilotPRTotalCreated,
				value:  firstNumberOrZero(record, "pull_requests_total_created", "total_pull_requests_created"),
				exists: hasAnyNumber(record, "pull_requests_total_created", "total_pull_requests_created"),
			},
			metricSpec{
				name:   MetricCopilotPRTotalReviewed,
				value:  firstNumberOrZero(record, "pull_requests_total_reviewed", "total_pull_requests_reviewed"),
				exists: hasAnyNumber(record, "pull_requests_total_reviewed", "total_pull_requests_reviewed"),
			},
			metricSpec{
				name: MetricCopilotPRTotalCreatedByCopilot,
				value: firstNumberOrZero(
					record,
					"pull_requests_total_created_by_copilot",
					"total_pull_requests_created_by_copilot",
					"total_pr_summaries_created",
				),
				exists: hasAnyNumber(
					record,
					"pull_requests_total_created_by_copilot",
					"total_pull_requests_created_by_copilot",
					"total_pr_summaries_created",
				),
			},
			metricSpec{
				name: MetricCopilotPRTotalReviewedByCopilot,
				value: firstNumberOrZero(
					record,
					"pull_requests_total_reviewed_by_copilot",
					"total_pull_requests_reviewed_by_copilot",
					"total_pr_summaries_created_users",
				),
				exists: hasAnyNumber(
					record,
					"pull_requests_total_reviewed_by_copilot",
					"total_pull_requests_reviewed_by_copilot",
					"total_pr_summaries_created_users",
				),
			},
		)
	}

	metrics := make([]store.MetricPoint, 0, len(metricSpecs))
	for _, spec := range metricSpecs {
		if !spec.exists {
			continue
		}
		point, err := NewCopilotMetric(spec.name, org, user, spec.value, observedAt, labels)
		if err != nil {
			continue
		}
		metrics = append(metrics, point)
	}
	return metrics
}

func (s *GitHubOrgScraper) copilotDayLabel(
	record map[string]any,
	linkResult githubapi.CopilotReportLinkResult,
) string {
	if !s.copilotCfg.EmitDayLabel {
		return ""
	}
	if day := firstString(record, "day", "report_day"); day != "" {
		return day
	}
	if !linkResult.ReportDay.IsZero() {
		return linkResult.ReportDay.UTC().Format(dayFormat)
	}
	if !linkResult.ReportEndDay.IsZero() {
		return linkResult.ReportEndDay.UTC().Format(dayFormat)
	}
	return ""
}

func copilotCheckpointRepoKey(scope, window string) string {
	return "__copilot__" + strings.TrimSpace(scope) + "__" + strings.TrimSpace(window)
}

func copilotLastRunRepoKey(scope, window string) string {
	return "__copilot_run__" + strings.TrimSpace(scope) + "__" + strings.TrimSpace(window)
}

func parseCopilotCheckpointRepoKey(repo string) (scope, window string, ok bool) {
	trimmed := strings.TrimSpace(repo)
	if !strings.HasPrefix(trimmed, "__copilot__") {
		return "", "", false
	}
	parts := strings.Split(trimmed, "__")
	if len(parts) != 4 {
		return "", "", false
	}
	if strings.TrimSpace(parts[2]) == "" || strings.TrimSpace(parts[3]) == "" {
		return "", "", false
	}
	return parts[2], parts[3], true
}

func firstString(record map[string]any, keys ...string) string {
	for _, key := range keys {
		raw, ok := record[key]
		if !ok {
			continue
		}
		switch value := raw.(type) {
		case string:
			if strings.TrimSpace(value) != "" {
				return strings.TrimSpace(value)
			}
		case float64:
			return strconv.FormatInt(int64(value), 10)
		case int:
			return strconv.Itoa(value)
		case int64:
			return strconv.FormatInt(value, 10)
		}
	}
	return ""
}

func firstNumberOrZero(record map[string]any, keys ...string) float64 {
	value, _ := firstNumber(record, keys...)
	return value
}

func hasAnyNumber(record map[string]any, keys ...string) bool {
	_, ok := firstNumber(record, keys...)
	return ok
}

func sumNumbers(record map[string]any, keys ...string) float64 {
	total := 0.0
	for _, key := range keys {
		if value, ok := firstNumber(record, key); ok {
			total += value
		}
	}
	return total
}

func firstNumber(record map[string]any, keys ...string) (float64, bool) {
	for _, key := range keys {
		raw, ok := record[key]
		if !ok {
			continue
		}
		switch value := raw.(type) {
		case float64:
			return value, true
		case float32:
			return float64(value), true
		case int:
			return float64(value), true
		case int64:
			return float64(value), true
		case json.Number:
			parsed, err := value.Float64()
			if err == nil {
				return parsed, true
			}
		case string:
			parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
			if err == nil {
				return parsed, true
			}
		}
	}
	return 0, false
}

func (s *GitHubOrgScraper) recordLOCState(org, repo string, event githubapi.LOCEvent) githubapi.LOCState {
	key := org + "/" + repo

	s.mu.Lock()
	defer s.mu.Unlock()

	previous := s.locStateByRepo[key]
	next := s.stateMachine.Apply(previous, event)
	s.locStateByRepo[key] = next
	return next
}

func (s *GitHubOrgScraper) readCheckpoint(org, repo string) (time.Time, bool) {
	checkpoints := s.checkpointStore()
	if checkpoints == nil {
		return time.Time{}, false
	}
	checkpoint, found, err := checkpoints.GetCheckpoint(org, repo)
	if err != nil || !found || checkpoint.IsZero() {
		return time.Time{}, false
	}
	return checkpoint.UTC(), true
}

func (s *GitHubOrgScraper) advanceCheckpoint(org, repo string, checkpoint time.Time) {
	if checkpoint.IsZero() {
		return
	}
	checkpoints := s.checkpointStore()
	if checkpoints == nil {
		return
	}
	current, found, err := checkpoints.GetCheckpoint(org, repo)
	if err == nil && found && !checkpoint.After(current) {
		return
	}
	if setErr := checkpoints.SetCheckpoint(org, repo, checkpoint.UTC()); setErr != nil {
		return
	}
}

func (s *GitHubOrgScraper) checkpointStore() CheckpointStore {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checkpoints
}

func (s *GitHubOrgScraper) consumeFallbackBudget(org string, requested int, now time.Time) (int, bool) {
	if requested <= 0 {
		return 0, false
	}
	if s.cfg.FallbackMaxCommitDetailCallsPerOrgPerHour <= 0 {
		return requested, false
	}

	hourStart := now.UTC().Truncate(time.Hour)

	s.mu.Lock()
	defer s.mu.Unlock()

	budget := s.fallbackOrgBudgetByHr[org]
	if budget.windowStart.IsZero() || !budget.windowStart.Equal(hourStart) {
		budget = orgFallbackBudget{
			windowStart: hourStart,
			used:        0,
		}
	}

	remaining := s.cfg.FallbackMaxCommitDetailCallsPerOrgPerHour - budget.used
	if remaining <= 0 {
		s.fallbackOrgBudgetByHr[org] = budget
		return 0, true
	}
	budgetExhausted := false
	if requested > remaining {
		requested = remaining
		budgetExhausted = true
	}
	budget.used += requested
	s.fallbackOrgBudgetByHr[org] = budget
	return requested, budgetExhausted
}

func (s *GitHubOrgScraper) applyRateLimitPacing(metadata githubapi.CallMetadata, summary *orgRateLimitSummary) {
	if summary != nil {
		summary.observe(metadata)
	}
	if metadata.LastDecision.Allow {
		return
	}
	if metadata.LastDecision.WaitFor <= 0 {
		return
	}
	s.cfg.Sleep(metadata.LastDecision.WaitFor)
}

func (s *orgRateLimitSummary) observe(metadata githubapi.CallMetadata) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	headers := metadata.LastRateHeaders
	if headers.Remaining > 0 && (s.minRemaining < 0 || headers.Remaining < s.minRemaining) {
		s.minRemaining = headers.Remaining
	}
	if headers.ResetUnix > s.resetUnix {
		s.resetUnix = headers.ResetUnix
	}
	if headers.SecondaryLimited || metadata.LastDecision.Reason == "secondary_limit" {
		s.secondaryHit++
	}
}

func (s *orgRateLimitSummary) observeRequest(endpoint, statusClass string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.requests == nil {
		s.requests = make(map[string]int)
	}
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		endpoint = UnknownLabelValue
	}
	statusClass = strings.TrimSpace(statusClass)
	if statusClass == "" {
		statusClass = UnknownLabelValue
	}
	key := endpoint + "|" + statusClass
	s.requests[key]++
}

func (s *orgRateLimitSummary) snapshot() (int, int64, int, map[string]int) {
	if s == nil {
		return -1, 0, 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	requestTotals := make(map[string]int, len(s.requests))
	for key, value := range s.requests {
		requestTotals[key] = value
	}
	return s.minRemaining, s.resetUnix, s.secondaryHit, requestTotals
}

func endpointStatusClass(status githubapi.EndpointStatus) string {
	switch status {
	case githubapi.EndpointStatusOK, githubapi.EndpointStatusAccepted:
		return "2xx"
	case githubapi.EndpointStatusForbidden, githubapi.EndpointStatusNotFound, githubapi.EndpointStatusConflict, githubapi.EndpointStatusUnprocessable:
		return "4xx"
	case githubapi.EndpointStatusUnavailable:
		return "5xx"
	default:
		return "unknown"
	}
}

func mergeRepositoryOutcome(left, right repositoryOutcome) repositoryOutcome {
	merged := repositoryOutcome{
		metrics: append(append([]store.MetricPoint{}, left.metrics...), right.metrics...),
		missed:  append(append([]MissedWindow{}, left.missed...), right.missed...),
		summaryDelta: repoSummaryDelta{
			processed:          left.summaryDelta.processed + right.summaryDelta.processed,
			statsAccepted:      left.summaryDelta.statsAccepted + right.summaryDelta.statsAccepted,
			statsForbidden:     left.summaryDelta.statsForbidden + right.summaryDelta.statsForbidden,
			statsNotFound:      left.summaryDelta.statsNotFound + right.summaryDelta.statsNotFound,
			statsConflict:      left.summaryDelta.statsConflict + right.summaryDelta.statsConflict,
			statsUnprocessable: left.summaryDelta.statsUnprocessable + right.summaryDelta.statsUnprocessable,
			statsUnavailable:   left.summaryDelta.statsUnavailable + right.summaryDelta.statsUnavailable,
			noCompleteWeek:     left.summaryDelta.noCompleteWeek + right.summaryDelta.noCompleteWeek,
			fallbackUsed:       left.summaryDelta.fallbackUsed + right.summaryDelta.fallbackUsed,
			fallbackTruncated:  left.summaryDelta.fallbackTruncated + right.summaryDelta.fallbackTruncated,
			fallbackBudgetHits: left.summaryDelta.fallbackBudgetHits + right.summaryDelta.fallbackBudgetHits,
		},
	}
	return merged
}

func buildLOCSourceModeMetrics(org, repo, mode string, incomplete float64, updatedAt time.Time) []store.MetricPoint {
	statsValue := 0.0
	fallbackValue := 0.0
	if mode == locModeSampledCommitStats {
		fallbackValue = 1
	} else {
		statsValue = 1
	}

	return []store.MetricPoint{
		{
			Name: internalMetricLOCSourceMode,
			Labels: map[string]string{
				LabelOrg:  normalizeRequiredLabel(org),
				LabelRepo: normalizeRequiredLabel(repo),
				"mode":    locModeStatsContributors,
			},
			Value:     statsValue,
			UpdatedAt: updatedAt,
		},
		{
			Name: internalMetricLOCSourceMode,
			Labels: map[string]string{
				LabelOrg:  normalizeRequiredLabel(org),
				LabelRepo: normalizeRequiredLabel(repo),
				"mode":    locModeSampledCommitStats,
			},
			Value:     fallbackValue,
			UpdatedAt: updatedAt,
		},
		{
			Name: internalMetricLOCFallbackIncom,
			Labels: map[string]string{
				LabelOrg:  normalizeRequiredLabel(org),
				LabelRepo: normalizeRequiredLabel(repo),
			},
			Value:     incomplete,
			UpdatedAt: updatedAt,
		},
	}
}

func dedupeMissedWindows(windows []MissedWindow) []MissedWindow {
	if len(windows) <= 1 {
		return windows
	}
	seen := make(map[string]struct{}, len(windows))
	uniq := make([]MissedWindow, 0, len(windows))
	for _, window := range windows {
		key := window.Org + "|" + window.Repo + "|" + window.Reason + "|" + window.WindowStart.UTC().Format(time.RFC3339) + "|" + window.WindowEnd.UTC().Format(time.RFC3339)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		uniq = append(uniq, window)
	}
	return uniq
}

func normalizeActor(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return UnknownLabelValue
	}
	return trimmed
}

func maxTime(left, right time.Time) time.Time {
	if left.After(right) {
		return left
	}
	return right
}

func withinWindow(ts, start, end time.Time) bool {
	if ts.IsZero() {
		return false
	}
	if !start.IsZero() && ts.Before(start) {
		return false
	}
	if !end.IsZero() && ts.After(end) {
		return false
	}
	return true
}

func resolveCommitActor(commit githubapi.RepoCommit) string {
	if author := strings.TrimSpace(commit.Author); author != "" {
		return author
	}
	if committer := strings.TrimSpace(commit.Committer); committer != "" {
		return committer
	}
	if inferred := inferGitHubLoginFromNoReplyEmail(commit.AuthorEmail); inferred != "" {
		return inferred
	}
	if inferred := inferGitHubLoginFromNoReplyEmail(commit.CommitterEmail); inferred != "" {
		return inferred
	}
	if inferred := inferActorFromEmail(commit.AuthorEmail); inferred != "" {
		return inferred
	}
	if inferred := inferActorFromEmail(commit.CommitterEmail); inferred != "" {
		return inferred
	}
	if strings.TrimSpace(commit.AuthorName) != "" || strings.TrimSpace(commit.CommitterName) != "" {
		return userLabelUnlinkedGitAuthor
	}
	return userLabelUnattributedCommit
}

func inferGitHubLoginFromNoReplyEmail(email string) string {
	const suffix = "@users.noreply.github.com"

	trimmed := strings.TrimSpace(email)
	if trimmed == "" {
		return ""
	}
	lowered := strings.ToLower(trimmed)
	if !strings.HasSuffix(lowered, suffix) {
		return ""
	}

	localPart := strings.TrimSuffix(trimmed, suffix)
	if localPart == trimmed {
		// Case mismatch on suffix, fallback to lowercase parsing.
		localPart = strings.TrimSuffix(lowered, suffix)
	}
	localPart = strings.TrimSpace(localPart)
	if localPart == "" {
		return ""
	}

	parts := strings.SplitN(localPart, "+", 2)
	candidate := parts[len(parts)-1]
	candidate = strings.TrimSpace(candidate)
	if candidate == "" {
		return ""
	}
	return candidate
}

var nonUserLabelRunes = regexp.MustCompile(`[^a-z0-9._-]+`)

func inferActorFromEmail(email string) string {
	trimmed := strings.TrimSpace(email)
	if trimmed == "" {
		return ""
	}
	parts := strings.Split(trimmed, "@")
	if len(parts) != 2 {
		return ""
	}
	localPart := strings.TrimSpace(parts[0])
	if localPart == "" {
		return ""
	}
	normalized := strings.ToLower(localPart)
	normalized = nonUserLabelRunes.ReplaceAllString(normalized, "-")
	normalized = strings.Trim(normalized, "-")
	if normalized == "" {
		return ""
	}
	return normalized
}

func filterRepositories(repos []githubapi.Repository, allowlist []string) []githubapi.Repository {
	if len(repos) == 0 {
		return nil
	}
	normalizedAllowlist := make(map[string]struct{}, len(allowlist))
	allowAll := false
	for _, item := range allowlist {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		if trimmed == "*" {
			allowAll = true
			break
		}
		normalizedAllowlist[trimmed] = struct{}{}
	}

	if allowAll || len(normalizedAllowlist) == 0 {
		filtered := make([]githubapi.Repository, 0, len(repos))
		filtered = append(filtered, repos...)
		return filtered
	}

	filtered := make([]githubapi.Repository, 0, len(repos))
	for _, repo := range repos {
		if _, ok := normalizedAllowlist[repo.Name]; ok {
			filtered = append(filtered, repo)
		}
	}
	return filtered
}

type weeklyLOC struct {
	added   int
	removed int
	commits int
}

func extractLatestCompleteWeek(contributors []githubapi.ContributorStats, now time.Time) (map[string]weeklyLOC, time.Time, bool, bool) {
	selectedWeekStart := time.Time{}
	weeklyByUser := make(map[string]weeklyLOC)
	contributionsNonZero := false
	additionsDeletionsZero := false

	type weekEntry struct {
		user string
		week githubapi.ContributorWeek
	}
	latestByUser := make(map[string]weekEntry)

	for _, contributor := range contributors {
		user := strings.TrimSpace(contributor.User)
		if user == "" {
			user = UnknownLabelValue
		}
		for _, week := range contributor.Weeks {
			if week.WeekStart.IsZero() {
				continue
			}
			weekStart := week.WeekStart.UTC()
			weekEnd := weekStart.Add(7 * 24 * time.Hour)
			if weekEnd.After(now) {
				continue
			}

			entry, exists := latestByUser[user]
			if !exists || weekStart.After(entry.week.WeekStart) {
				latestByUser[user] = weekEntry{
					user: user,
					week: githubapi.ContributorWeek{
						WeekStart: weekStart,
						Additions: week.Additions,
						Deletions: week.Deletions,
						Commits:   week.Commits,
					},
				}
			}
		}
	}

	for user, entry := range latestByUser {
		if entry.week.WeekStart.After(selectedWeekStart) {
			selectedWeekStart = entry.week.WeekStart
		}
		weeklyByUser[user] = weeklyLOC{
			added:   entry.week.Additions,
			removed: entry.week.Deletions,
			commits: entry.week.Commits,
		}
		if entry.week.Commits > 0 {
			contributionsNonZero = true
		}
	}

	if len(weeklyByUser) == 0 {
		return weeklyByUser, time.Time{}, false, false
	}

	totalAdded := 0
	totalRemoved := 0
	for _, values := range weeklyByUser {
		totalAdded += values.added
		totalRemoved += values.removed
	}
	additionsDeletionsZero = (totalAdded == 0 && totalRemoved == 0 && contributionsNonZero)

	return weeklyByUser, selectedWeekStart.Add(7 * 24 * time.Hour), contributionsNonZero, additionsDeletionsZero
}
