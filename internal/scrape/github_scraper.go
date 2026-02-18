package scrape

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/cam3ron2/github-stats/internal/config"
	"github.com/cam3ron2/github-stats/internal/githubapi"
	"github.com/cam3ron2/github-stats/internal/store"
)

const (
	repoMissReasonContributorStats = "contributor_stats_failed"
	repoMissReasonFallbackList     = "fallback_list_commits_failed"
	repoMissReasonFallbackDetail   = "fallback_commit_detail_failed"
	repoMissReasonActivityCommits  = "activity_commits_failed"
	repoMissReasonActivityPulls    = "activity_pulls_failed"
	repoMissReasonActivityReviews  = "activity_reviews_failed"
	repoMissReasonActivityComments = "activity_issue_comments_failed"
	userLabelUnlinkedGitAuthor     = "unlinked_git_author"
	userLabelUnattributedCommit    = "unattributed_commit"
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
}

// GitHubOrgScraperConfig configures GitHub-backed org scraping behavior.
type GitHubOrgScraperConfig struct {
	LOCRefreshInterval                        time.Duration
	FallbackEnabled                           bool
	FallbackMaxCommitsPerRepoPerWeek          int
	FallbackMaxCommitDetailCallsPerOrgPerHour int
	LargeRepoZeroDetectionWindows             int
	LargeRepoCooldown                         time.Duration
	Now                                       func() time.Time
	Sleep                                     func(time.Duration)
}

// GitHubOrgScraper implements OrgScraper using typed GitHub API clients.
type GitHubOrgScraper struct {
	clients map[string]GitHubDataClient
	cfg     GitHubOrgScraperConfig

	stateMachine githubapi.LOCStateMachine

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
		locStateByRepo:        make(map[string]githubapi.LOCState),
		fallbackOrgBudgetByHr: make(map[string]orgFallbackBudget),
	}
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

	reposResult, err := client.ListOrgRepos(ctx, orgName)
	if err != nil {
		return OrgResult{}, fmt.Errorf("list org repos for %q: %w", orgName, err)
	}
	s.applyRateLimitPacing(reposResult.Metadata)

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
				outcome := s.scrapeRepository(ctx, client, orgName, repo.Name)
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
	}
	result.Summary.MissedWindows = len(result.MissedWindow)
	result.Summary.MetricsProduced = len(result.Metrics)

	return result, nil
}

func (s *GitHubOrgScraper) scrapeRepository(ctx context.Context, client GitHubDataClient, org, repo string) repositoryOutcome {
	now := s.cfg.Now().UTC()
	windowStart := now.Add(-7 * 24 * time.Hour)
	activityWindowStart := now.Add(-24 * time.Hour)
	windowEnd := now
	summary := repoSummaryDelta{processed: 1}
	outcome := repositoryOutcome{
		summaryDelta: summary,
	}

	activityOutcome := s.scrapeRepositoryActivity(ctx, client, org, repo, activityWindowStart, windowEnd)
	outcome = mergeRepositoryOutcome(outcome, activityOutcome)

	statsResult, err := client.GetContributorStats(ctx, org, repo)
	if err != nil {
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
	s.applyRateLimitPacing(statsResult.Metadata)

	switch statsResult.Status {
	case githubapi.EndpointStatusAccepted:
		outcome.summaryDelta.statsAccepted++
		s.recordLOCState(org, repo, githubapi.LOCEvent{
			ObservedAt: now,
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

	weeklyByUser, _, contributionsNonZero, additionsDeletionsZero := extractLatestCompleteWeek(statsResult.Contributors, now)
	state := s.recordLOCState(org, repo, githubapi.LOCEvent{
		ObservedAt:             now,
		HTTPStatus:             200,
		StatsPresent:           true,
		ContributionsNonZero:   contributionsNonZero,
		AdditionsDeletionsZero: additionsDeletionsZero,
	})

	if len(weeklyByUser) == 0 {
		outcome.summaryDelta.noCompleteWeek++
	}

	if s.cfg.FallbackEnabled && state.Mode == githubapi.LOCModeFallback {
		fallbackOutcome := s.scrapeRepositoryFallback(ctx, client, org, repo, windowStart, windowEnd)
		fallbackOutcome.summaryDelta.fallbackUsed++
		return mergeRepositoryOutcome(outcome, fallbackOutcome)
	}

	metrics := make([]store.MetricPoint, 0, len(weeklyByUser)*2)
	for user, weekly := range weeklyByUser {
		addMetric, err := NewProductivityMetric(MetricActivityLOCAddedWeekly, org, repo, user, float64(weekly.added), now)
		if err == nil {
			metrics = append(metrics, addMetric)
		}
		removedMetric, err := NewProductivityMetric(MetricActivityLOCRemovedWeekly, org, repo, user, float64(weekly.removed), now)
		if err == nil {
			metrics = append(metrics, removedMetric)
		}
	}

	return mergeRepositoryOutcome(outcome, repositoryOutcome{metrics: metrics})
}

func (s *GitHubOrgScraper) scrapeRepositoryActivity(
	ctx context.Context,
	client GitHubDataClient,
	org string,
	repo string,
	windowStart time.Time,
	windowEnd time.Time,
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
		summary.statsUnavailable++
		addMissedWindow(repoMissReasonActivityCommits)
	} else {
		s.applyRateLimitPacing(commitsResult.Metadata)
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
		summary.statsUnavailable++
		addMissedWindow(repoMissReasonActivityPulls)
	} else {
		s.applyRateLimitPacing(pullsResult.Metadata)
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
					summary.statsUnavailable++
					addMissedWindow(repoMissReasonActivityReviews)
					continue
				}
				s.applyRateLimitPacing(reviewsResult.Metadata)

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
		summary.statsUnavailable++
		addMissedWindow(repoMissReasonActivityComments)
	} else {
		s.applyRateLimitPacing(commentsResult.Metadata)
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
) repositoryOutcome {
	summary := repoSummaryDelta{}

	commitListResult, err := client.ListRepoCommitsWindow(ctx, org, repo, windowStart, windowEnd, s.cfg.FallbackMaxCommitsPerRepoPerWeek)
	if err != nil {
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
	s.applyRateLimitPacing(commitListResult.Metadata)

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

	allowedDetails := s.consumeFallbackBudget(org, len(commitListResult.Commits), windowEnd)
	if allowedDetails < len(commitListResult.Commits) {
		commitListResult.Commits = commitListResult.Commits[:allowedDetails]
		commitListResult.Truncated = true
	}

	addedByUser := make(map[string]int)
	removedByUser := make(map[string]int)
	missed := make([]MissedWindow, 0, 1)

	for _, commit := range commitListResult.Commits {
		detailResult, err := client.GetCommit(ctx, org, repo, commit.SHA)
		if err != nil {
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
		s.applyRateLimitPacing(detailResult.Metadata)

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

func (s *GitHubOrgScraper) recordLOCState(org, repo string, event githubapi.LOCEvent) githubapi.LOCState {
	key := org + "/" + repo

	s.mu.Lock()
	defer s.mu.Unlock()

	previous := s.locStateByRepo[key]
	next := s.stateMachine.Apply(previous, event)
	s.locStateByRepo[key] = next
	return next
}

func (s *GitHubOrgScraper) consumeFallbackBudget(org string, requested int, now time.Time) int {
	if requested <= 0 {
		return 0
	}
	if s.cfg.FallbackMaxCommitDetailCallsPerOrgPerHour <= 0 {
		return requested
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
		return 0
	}
	if requested > remaining {
		requested = remaining
	}
	budget.used += requested
	s.fallbackOrgBudgetByHr[org] = budget
	return requested
}

func (s *GitHubOrgScraper) applyRateLimitPacing(metadata githubapi.CallMetadata) {
	if metadata.LastDecision.Allow {
		return
	}
	if metadata.LastDecision.WaitFor <= 0 {
		return
	}
	s.cfg.Sleep(metadata.LastDecision.WaitFor)
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
		},
	}
	return merged
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
