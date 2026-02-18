package githubapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const defaultGitHubAPIBaseURL = "https://api.github.com/"

// EndpointStatus represents a normalized GitHub API endpoint outcome.
type EndpointStatus string

const (
	// EndpointStatusOK indicates a successful response.
	EndpointStatusOK EndpointStatus = "ok"
	// EndpointStatusAccepted indicates GitHub accepted the request and is still computing results.
	EndpointStatusAccepted EndpointStatus = "accepted"
	// EndpointStatusForbidden indicates authorization failure or restricted access.
	EndpointStatusForbidden EndpointStatus = "forbidden"
	// EndpointStatusNotFound indicates the resource does not exist or is hidden.
	EndpointStatusNotFound EndpointStatus = "not_found"
	// EndpointStatusConflict indicates a state conflict, like unsupported stats on empty repositories.
	EndpointStatusConflict EndpointStatus = "conflict"
	// EndpointStatusUnprocessable indicates request validation/processing failure.
	EndpointStatusUnprocessable EndpointStatus = "unprocessable"
	// EndpointStatusUnavailable indicates a temporary service-side failure.
	EndpointStatusUnavailable EndpointStatus = "unavailable"
	// EndpointStatusUnknown indicates an unclassified non-success status.
	EndpointStatusUnknown EndpointStatus = "unknown"
)

// Repository is one GitHub repository in an organization.
type Repository struct {
	Name          string
	FullName      string
	DefaultBranch string
	Archived      bool
	Disabled      bool
	Fork          bool
}

// OrgReposResult is the typed result for listing organization repositories.
type OrgReposResult struct {
	Status   EndpointStatus
	Repos    []Repository
	Metadata CallMetadata
}

// ContributorWeek is one contributor weekly summary from contributor stats.
type ContributorWeek struct {
	WeekStart time.Time
	Additions int
	Deletions int
	Commits   int
}

// ContributorStats is one contributor's aggregate stats payload.
type ContributorStats struct {
	User         string
	TotalCommits int
	Weeks        []ContributorWeek
}

// ContributorStatsResult is the typed result for `/stats/contributors`.
type ContributorStatsResult struct {
	Status       EndpointStatus
	Contributors []ContributorStats
	Metadata     CallMetadata
}

// RepoCommit is one commit summary from the commit list endpoint.
type RepoCommit struct {
	SHA         string
	Author      string
	CommittedAt time.Time
}

// CommitListResult is the typed result for listing repository commits in a window.
type CommitListResult struct {
	Status    EndpointStatus
	Commits   []RepoCommit
	Truncated bool
	Metadata  CallMetadata
}

// CommitDetail is a typed commit detail response.
type CommitDetail struct {
	Status    EndpointStatus
	SHA       string
	Author    string
	Additions int
	Deletions int
	Total     int
	Metadata  CallMetadata
}

// DataClient is a typed GitHub REST data client for scrape-relevant endpoints.
type DataClient struct {
	baseURL       *url.URL
	requestClient *Client
}

// NewDataClient creates a typed data client over the generic retry/rate-limit request client.
func NewDataClient(baseURL string, requestClient *Client) (*DataClient, error) {
	if requestClient == nil {
		return nil, fmt.Errorf("request client is required")
	}

	parsed, err := parseAPIBaseURL(baseURL)
	if err != nil {
		return nil, err
	}

	return &DataClient{
		baseURL:       parsed,
		requestClient: requestClient,
	}, nil
}

// ListOrgRepos lists repositories in one GitHub organization with pagination support.
func (c *DataClient) ListOrgRepos(ctx context.Context, org string) (OrgReposResult, error) {
	trimmedOrg := strings.TrimSpace(org)
	if trimmedOrg == "" {
		return OrgReposResult{}, fmt.Errorf("organization is required")
	}

	result := OrgReposResult{
		Status: EndpointStatusOK,
	}
	page := 1
	for {
		reqURL := c.cloneBaseURL()
		reqURL.Path = joinURLPath(reqURL.Path, "orgs", url.PathEscape(trimmedOrg), "repos")
		query := reqURL.Query()
		query.Set("per_page", "100")
		query.Set("page", strconv.Itoa(page))
		query.Set("type", "all")
		reqURL.RawQuery = query.Encode()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL.String(), nil)
		if err != nil {
			return OrgReposResult{}, fmt.Errorf("build list org repos request: %w", err)
		}

		resp, metadata, err := c.requestClient.Do(req)
		result.Metadata = mergeMetadata(result.Metadata, metadata)
		if err != nil {
			return OrgReposResult{}, fmt.Errorf("list org repos request failed: %w", err)
		}
		if resp == nil {
			return OrgReposResult{}, fmt.Errorf("list org repos request failed: nil response")
		}

		status := endpointStatusFromHTTP(resp.StatusCode)
		if status != EndpointStatusOK {
			_ = resp.Body.Close()
			result.Status = status
			return result, nil
		}

		var payload []repositoryPayload
		if err := decodeJSONAndClose(resp, &payload); err != nil {
			return OrgReposResult{}, fmt.Errorf("decode list org repos response: %w", err)
		}

		for _, repo := range payload {
			result.Repos = append(result.Repos, Repository{
				Name:          repo.Name,
				FullName:      repo.FullName,
				DefaultBranch: repo.DefaultBranch,
				Archived:      repo.Archived,
				Disabled:      repo.Disabled,
				Fork:          repo.Fork,
			})
		}

		if !hasNextPage(resp.Header.Get("Link")) {
			break
		}
		page++
	}

	return result, nil
}

// GetContributorStats reads contributor weekly stats for one repository.
func (c *DataClient) GetContributorStats(ctx context.Context, owner, repo string) (ContributorStatsResult, error) {
	trimmedOwner := strings.TrimSpace(owner)
	trimmedRepo := strings.TrimSpace(repo)
	if trimmedOwner == "" {
		return ContributorStatsResult{}, fmt.Errorf("owner is required")
	}
	if trimmedRepo == "" {
		return ContributorStatsResult{}, fmt.Errorf("repo is required")
	}

	reqURL := c.cloneBaseURL()
	reqURL.Path = joinURLPath(
		reqURL.Path,
		"repos",
		url.PathEscape(trimmedOwner),
		url.PathEscape(trimmedRepo),
		"stats",
		"contributors",
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL.String(), nil)
	if err != nil {
		return ContributorStatsResult{}, fmt.Errorf("build contributor stats request: %w", err)
	}

	resp, metadata, err := c.requestClient.Do(req)
	if err != nil {
		return ContributorStatsResult{}, fmt.Errorf("contributor stats request failed: %w", err)
	}
	if resp == nil {
		return ContributorStatsResult{}, fmt.Errorf("contributor stats request failed: nil response")
	}

	status := endpointStatusFromHTTP(resp.StatusCode)
	result := ContributorStatsResult{
		Status:   status,
		Metadata: metadata,
	}
	if status != EndpointStatusOK {
		_ = resp.Body.Close()
		return result, nil
	}

	var payload []contributorStatsPayload
	if err := decodeJSONAndClose(resp, &payload); err != nil {
		return ContributorStatsResult{}, fmt.Errorf("decode contributor stats response: %w", err)
	}
	for _, contributor := range payload {
		typed := ContributorStats{
			TotalCommits: contributor.Total,
		}
		if contributor.Author != nil {
			typed.User = contributor.Author.Login
		}
		for _, week := range contributor.Weeks {
			typed.Weeks = append(typed.Weeks, ContributorWeek{
				WeekStart: time.Unix(week.UnixWeek, 0).UTC(),
				Additions: week.Additions,
				Deletions: week.Deletions,
				Commits:   week.Commits,
			})
		}
		result.Contributors = append(result.Contributors, typed)
	}
	return result, nil
}

// ListRepoCommitsWindow lists repository commits in a time window with pagination and optional cap.
func (c *DataClient) ListRepoCommitsWindow(ctx context.Context, owner, repo string, since, until time.Time, maxCommits int) (CommitListResult, error) {
	trimmedOwner := strings.TrimSpace(owner)
	trimmedRepo := strings.TrimSpace(repo)
	if trimmedOwner == "" {
		return CommitListResult{}, fmt.Errorf("owner is required")
	}
	if trimmedRepo == "" {
		return CommitListResult{}, fmt.Errorf("repo is required")
	}
	if !until.IsZero() && !since.IsZero() && until.Before(since) {
		return CommitListResult{}, fmt.Errorf("until must not be before since")
	}

	result := CommitListResult{
		Status: EndpointStatusOK,
	}
	page := 1
	for {
		reqURL := c.cloneBaseURL()
		reqURL.Path = joinURLPath(reqURL.Path, "repos", url.PathEscape(trimmedOwner), url.PathEscape(trimmedRepo), "commits")
		query := reqURL.Query()
		query.Set("per_page", "100")
		query.Set("page", strconv.Itoa(page))
		if !since.IsZero() {
			query.Set("since", since.UTC().Format(time.RFC3339))
		}
		if !until.IsZero() {
			query.Set("until", until.UTC().Format(time.RFC3339))
		}
		reqURL.RawQuery = query.Encode()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL.String(), nil)
		if err != nil {
			return CommitListResult{}, fmt.Errorf("build list commits request: %w", err)
		}

		resp, metadata, err := c.requestClient.Do(req)
		result.Metadata = mergeMetadata(result.Metadata, metadata)
		if err != nil {
			return CommitListResult{}, fmt.Errorf("list commits request failed: %w", err)
		}
		if resp == nil {
			return CommitListResult{}, fmt.Errorf("list commits request failed: nil response")
		}

		status := endpointStatusFromHTTP(resp.StatusCode)
		if status != EndpointStatusOK {
			_ = resp.Body.Close()
			result.Status = status
			return result, nil
		}

		var payload []commitListPayload
		if err := decodeJSONAndClose(resp, &payload); err != nil {
			return CommitListResult{}, fmt.Errorf("decode list commits response: %w", err)
		}

		for _, commit := range payload {
			typed := RepoCommit{
				SHA: commit.SHA,
			}
			if commit.Author != nil {
				typed.Author = commit.Author.Login
			}
			typed.CommittedAt = parseRFC3339(commit.Commit.Author.Date)
			result.Commits = append(result.Commits, typed)

			if maxCommits > 0 && len(result.Commits) >= maxCommits {
				result.Commits = result.Commits[:maxCommits]
				result.Truncated = true
				return result, nil
			}
		}

		if len(payload) == 0 || !hasNextPage(resp.Header.Get("Link")) {
			break
		}
		page++
	}

	return result, nil
}

// GetCommit reads commit detail including additions/deletions.
func (c *DataClient) GetCommit(ctx context.Context, owner, repo, sha string) (CommitDetail, error) {
	trimmedOwner := strings.TrimSpace(owner)
	trimmedRepo := strings.TrimSpace(repo)
	trimmedSHA := strings.TrimSpace(sha)
	if trimmedOwner == "" {
		return CommitDetail{}, fmt.Errorf("owner is required")
	}
	if trimmedRepo == "" {
		return CommitDetail{}, fmt.Errorf("repo is required")
	}
	if trimmedSHA == "" {
		return CommitDetail{}, fmt.Errorf("sha is required")
	}

	reqURL := c.cloneBaseURL()
	reqURL.Path = joinURLPath(reqURL.Path, "repos", url.PathEscape(trimmedOwner), url.PathEscape(trimmedRepo), "commits", url.PathEscape(trimmedSHA))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL.String(), nil)
	if err != nil {
		return CommitDetail{}, fmt.Errorf("build commit detail request: %w", err)
	}

	resp, metadata, err := c.requestClient.Do(req)
	if err != nil {
		return CommitDetail{}, fmt.Errorf("commit detail request failed: %w", err)
	}
	if resp == nil {
		return CommitDetail{}, fmt.Errorf("commit detail request failed: nil response")
	}

	status := endpointStatusFromHTTP(resp.StatusCode)
	result := CommitDetail{
		Status:   status,
		Metadata: metadata,
	}
	if status != EndpointStatusOK {
		_ = resp.Body.Close()
		return result, nil
	}

	var payload commitDetailPayload
	if err := decodeJSONAndClose(resp, &payload); err != nil {
		return CommitDetail{}, fmt.Errorf("decode commit detail response: %w", err)
	}

	result.SHA = payload.SHA
	if payload.Author != nil {
		result.Author = payload.Author.Login
	}
	result.Additions = payload.Stats.Additions
	result.Deletions = payload.Stats.Deletions
	result.Total = payload.Stats.Total
	return result, nil
}

func parseAPIBaseURL(raw string) (*url.URL, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		trimmed = defaultGitHubAPIBaseURL
	}

	parsed, err := url.Parse(trimmed)
	if err != nil {
		return nil, fmt.Errorf("parse github api base url: %w", err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("parse github api base url: missing scheme or host")
	}
	if !strings.HasSuffix(parsed.Path, "/") {
		parsed.Path += "/"
	}
	return parsed, nil
}

func (c *DataClient) cloneBaseURL() *url.URL {
	cloned := *c.baseURL
	return &cloned
}

func joinURLPath(base string, segments ...string) string {
	trimmedBase := strings.TrimSuffix(base, "/")
	builder := strings.Builder{}
	builder.WriteString(trimmedBase)
	for _, segment := range segments {
		builder.WriteString("/")
		builder.WriteString(strings.TrimPrefix(segment, "/"))
	}
	return builder.String()
}

func endpointStatusFromHTTP(statusCode int) EndpointStatus {
	switch statusCode {
	case http.StatusAccepted:
		return EndpointStatusAccepted
	case http.StatusForbidden:
		return EndpointStatusForbidden
	case http.StatusNotFound:
		return EndpointStatusNotFound
	case http.StatusConflict:
		return EndpointStatusConflict
	case http.StatusUnprocessableEntity:
		return EndpointStatusUnprocessable
	}
	if statusCode >= 200 && statusCode <= 299 {
		return EndpointStatusOK
	}
	if statusCode >= 500 {
		return EndpointStatusUnavailable
	}
	return EndpointStatusUnknown
}

func decodeJSONAndClose(resp *http.Response, target any) error {
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return nil
}

func hasNextPage(linkHeader string) bool {
	if strings.TrimSpace(linkHeader) == "" {
		return false
	}
	parts := strings.Split(linkHeader, ",")
	for _, part := range parts {
		if strings.Contains(part, `rel="next"`) {
			return true
		}
	}
	return false
}

func parseRFC3339(raw string) time.Time {
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}
	}
	return parsed.UTC()
}

func mergeMetadata(current CallMetadata, incoming CallMetadata) CallMetadata {
	current.Attempts += incoming.Attempts
	current.LastDecision = incoming.LastDecision
	current.LastRateHeaders = incoming.LastRateHeaders
	return current
}

type repositoryPayload struct {
	Name          string `json:"name"`
	FullName      string `json:"full_name"`
	DefaultBranch string `json:"default_branch"`
	Archived      bool   `json:"archived"`
	Disabled      bool   `json:"disabled"`
	Fork          bool   `json:"fork"`
}

type contributorStatsPayload struct {
	Total  int                  `json:"total"`
	Author *userPayload         `json:"author"`
	Weeks  []contributorWeekDTO `json:"weeks"`
}

type contributorWeekDTO struct {
	UnixWeek  int64 `json:"w"`
	Additions int   `json:"a"`
	Deletions int   `json:"d"`
	Commits   int   `json:"c"`
}

type commitListPayload struct {
	SHA    string          `json:"sha"`
	Author *userPayload    `json:"author"`
	Commit commitCoreBlock `json:"commit"`
}

type commitCoreBlock struct {
	Author commitAuthorBlock `json:"author"`
}

type commitAuthorBlock struct {
	Date string `json:"date"`
}

type commitDetailPayload struct {
	SHA    string       `json:"sha"`
	Author *userPayload `json:"author"`
	Stats  struct {
		Additions int `json:"additions"`
		Deletions int `json:"deletions"`
		Total     int `json:"total"`
	} `json:"stats"`
}

type userPayload struct {
	Login string `json:"login"`
}
