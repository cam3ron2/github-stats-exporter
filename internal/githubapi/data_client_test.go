package githubapi

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func newTestRequestClient(doer HTTPDoer) *Client {
	policy := RateLimitPolicy{
		MinRemainingThreshold: 0,
		Now: func() time.Time {
			return time.Unix(1739836800, 0)
		},
	}
	return NewClient(doer, RetryConfig{
		MaxAttempts:    1,
		InitialBackoff: time.Second,
		MaxBackoff:     time.Second,
	}, policy)
}

func TestNewDataClient(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		baseURL     string
		client      *Client
		wantErr     bool
		errContains string
	}{
		{
			name:    "uses_default_base_url",
			baseURL: "",
			client:  newTestRequestClient(&fakeDoer{}),
		},
		{
			name:    "accepts_custom_base_url",
			baseURL: "https://github.example.com/api/v3",
			client:  newTestRequestClient(&fakeDoer{}),
		},
		{
			name:        "rejects_invalid_base_url",
			baseURL:     "://bad-url",
			client:      newTestRequestClient(&fakeDoer{}),
			wantErr:     true,
			errContains: "parse github api base url",
		},
		{
			name:        "rejects_nil_client",
			baseURL:     "https://api.github.com",
			client:      nil,
			wantErr:     true,
			errContains: "request client is required",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, err := NewDataClient(tc.baseURL, tc.client)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("NewDataClient() expected error, got nil")
				}
				if tc.errContains != "" && !contains(err.Error(), tc.errContains) {
					t.Fatalf("error = %q, missing %q", err.Error(), tc.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewDataClient() unexpected error: %v", err)
			}
			if client == nil {
				t.Fatalf("NewDataClient() returned nil client")
			}
		})
	}
}

func TestDataClientListOrgRepos(t *testing.T) {
	t.Parallel()

	doer := &fakeDoer{
		responses: []*http.Response{
			newResponse(http.StatusOK, map[string]string{
				"Link": `<https://api.github.com/orgs/test/repos?per_page=100&page=2>; rel="next"`,
			}, `[
				{"name":"repo-a","full_name":"test/repo-a","default_branch":"main","archived":false,"disabled":false,"fork":false}
			]`),
			newResponse(http.StatusOK, map[string]string{}, `[
				{"name":"repo-b","full_name":"test/repo-b","default_branch":"main","archived":true,"disabled":false,"fork":false}
			]`),
		},
	}
	client, err := NewDataClient("", newTestRequestClient(doer))
	if err != nil {
		t.Fatalf("NewDataClient() unexpected error: %v", err)
	}

	got, err := client.ListOrgRepos(context.Background(), "test")
	if err != nil {
		t.Fatalf("ListOrgRepos() unexpected error: %v", err)
	}
	if got.Status != EndpointStatusOK {
		t.Fatalf("Status = %q, want %q", got.Status, EndpointStatusOK)
	}
	if len(got.Repos) != 2 {
		t.Fatalf("len(Repos) = %d, want 2", len(got.Repos))
	}
	if got.Repos[0].Name != "repo-a" || got.Repos[1].Name != "repo-b" {
		t.Fatalf("repos = %#v, want repo-a/repo-b", got.Repos)
	}
}

func TestDataClientListOrgReposStatusHandling(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		statusCode int
		wantStatus EndpointStatus
	}{
		{name: "forbidden", statusCode: http.StatusForbidden, wantStatus: EndpointStatusForbidden},
		{name: "not_found", statusCode: http.StatusNotFound, wantStatus: EndpointStatusNotFound},
		{name: "conflict", statusCode: http.StatusConflict, wantStatus: EndpointStatusConflict},
		{name: "unprocessable", statusCode: http.StatusUnprocessableEntity, wantStatus: EndpointStatusUnprocessable},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			doer := &fakeDoer{
				responses: []*http.Response{
					newResponse(tc.statusCode, map[string]string{}, `{"message":"nope"}`),
				},
			}
			client, err := NewDataClient("", newTestRequestClient(doer))
			if err != nil {
				t.Fatalf("NewDataClient() unexpected error: %v", err)
			}

			got, err := client.ListOrgRepos(context.Background(), "test")
			if err != nil {
				t.Fatalf("ListOrgRepos() unexpected error: %v", err)
			}
			if got.Status != tc.wantStatus {
				t.Fatalf("Status = %q, want %q", got.Status, tc.wantStatus)
			}
			if len(got.Repos) != 0 {
				t.Fatalf("len(Repos) = %d, want 0", len(got.Repos))
			}
		})
	}
}

func TestDataClientGetContributorStats(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		response    *http.Response
		wantStatus  EndpointStatus
		wantContrib int
		wantErr     bool
	}{
		{
			name:        "accepted_202",
			response:    newResponse(http.StatusAccepted, map[string]string{}, ``),
			wantStatus:  EndpointStatusAccepted,
			wantContrib: 0,
		},
		{
			name: "success_200",
			response: newResponse(http.StatusOK, map[string]string{}, `[
				{"total":10,"author":{"login":"alice"},"weeks":[{"w":1739750400,"a":12,"d":3,"c":2}]}
			]`),
			wantStatus:  EndpointStatusOK,
			wantContrib: 1,
		},
		{
			name:        "forbidden_403",
			response:    newResponse(http.StatusForbidden, map[string]string{}, `{"message":"forbidden"}`),
			wantStatus:  EndpointStatusForbidden,
			wantContrib: 0,
		},
		{
			name:        "not_found_404",
			response:    newResponse(http.StatusNotFound, map[string]string{}, `{"message":"missing"}`),
			wantStatus:  EndpointStatusNotFound,
			wantContrib: 0,
		},
		{
			name:        "conflict_409",
			response:    newResponse(http.StatusConflict, map[string]string{}, `{"message":"empty repository"}`),
			wantStatus:  EndpointStatusConflict,
			wantContrib: 0,
		},
		{
			name:        "unprocessable_422",
			response:    newResponse(http.StatusUnprocessableEntity, map[string]string{}, `{"message":"unprocessable"}`),
			wantStatus:  EndpointStatusUnprocessable,
			wantContrib: 0,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			doer := &fakeDoer{responses: []*http.Response{tc.response}}
			client, err := NewDataClient("", newTestRequestClient(doer))
			if err != nil {
				t.Fatalf("NewDataClient() unexpected error: %v", err)
			}

			got, err := client.GetContributorStats(context.Background(), "test", "repo-a")
			if tc.wantErr {
				if err == nil {
					t.Fatalf("GetContributorStats() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("GetContributorStats() unexpected error: %v", err)
			}
			if got.Status != tc.wantStatus {
				t.Fatalf("Status = %q, want %q", got.Status, tc.wantStatus)
			}
			if len(got.Contributors) != tc.wantContrib {
				t.Fatalf("len(Contributors) = %d, want %d", len(got.Contributors), tc.wantContrib)
			}
		})
	}
}

func TestDataClientListRepoCommitsWindow(t *testing.T) {
	t.Parallel()

	since := time.Unix(1739836800, 0)
	until := since.Add(24 * time.Hour)
	doer := &fakeDoer{
		responses: []*http.Response{
			newResponse(http.StatusOK, map[string]string{
				"Link": `<https://api.github.com/repos/test/repo-a/commits?per_page=100&page=2>; rel="next"`,
			}, `[
				{"sha":"a1","author":{"login":"alice"},"commit":{"author":{"date":"2025-02-17T00:00:00Z"}}},
				{"sha":"b2","author":{"login":"bob"},"commit":{"author":{"date":"2025-02-17T01:00:00Z"}}}
			]`),
			newResponse(http.StatusOK, map[string]string{}, `[
				{"sha":"c3","author":{"login":"carol"},"commit":{"author":{"date":"2025-02-17T02:00:00Z"}}}
			]`),
		},
	}
	client, err := NewDataClient("", newTestRequestClient(doer))
	if err != nil {
		t.Fatalf("NewDataClient() unexpected error: %v", err)
	}

	got, err := client.ListRepoCommitsWindow(context.Background(), "test", "repo-a", since, until, 2)
	if err != nil {
		t.Fatalf("ListRepoCommitsWindow() unexpected error: %v", err)
	}
	if got.Status != EndpointStatusOK {
		t.Fatalf("Status = %q, want %q", got.Status, EndpointStatusOK)
	}
	if len(got.Commits) != 2 {
		t.Fatalf("len(Commits) = %d, want 2", len(got.Commits))
	}
	if !got.Truncated {
		t.Fatalf("Truncated = %t, want true", got.Truncated)
	}
	if got.Commits[0].SHA != "a1" || got.Commits[1].SHA != "b2" {
		t.Fatalf("commits = %#v, want a1,b2", got.Commits)
	}
}

func TestDataClientGetCommit(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		response   *http.Response
		wantStatus EndpointStatus
		wantSHA    string
		wantErr    bool
	}{
		{
			name: "success_200",
			response: newResponse(http.StatusOK, map[string]string{}, `{
				"sha":"a1",
				"author":{"login":"alice"},
				"stats":{"additions":10,"deletions":2,"total":12}
			}`),
			wantStatus: EndpointStatusOK,
			wantSHA:    "a1",
		},
		{
			name:       "forbidden_403",
			response:   newResponse(http.StatusForbidden, map[string]string{}, `{"message":"forbidden"}`),
			wantStatus: EndpointStatusForbidden,
		},
		{
			name:       "not_found_404",
			response:   newResponse(http.StatusNotFound, map[string]string{}, `{"message":"missing"}`),
			wantStatus: EndpointStatusNotFound,
		},
		{
			name:       "conflict_409",
			response:   newResponse(http.StatusConflict, map[string]string{}, `{"message":"conflict"}`),
			wantStatus: EndpointStatusConflict,
		},
		{
			name:       "unprocessable_422",
			response:   newResponse(http.StatusUnprocessableEntity, map[string]string{}, `{"message":"unprocessable"}`),
			wantStatus: EndpointStatusUnprocessable,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			doer := &fakeDoer{responses: []*http.Response{tc.response}}
			client, err := NewDataClient("", newTestRequestClient(doer))
			if err != nil {
				t.Fatalf("NewDataClient() unexpected error: %v", err)
			}

			got, err := client.GetCommit(context.Background(), "test", "repo-a", "a1")
			if tc.wantErr {
				if err == nil {
					t.Fatalf("GetCommit() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("GetCommit() unexpected error: %v", err)
			}
			if got.Status != tc.wantStatus {
				t.Fatalf("Status = %q, want %q", got.Status, tc.wantStatus)
			}
			if got.SHA != tc.wantSHA {
				t.Fatalf("SHA = %q, want %q", got.SHA, tc.wantSHA)
			}
		})
	}
}

func TestDataClientHandlesDecodeErrors(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		call func(t *testing.T, client *DataClient) error
	}{
		{
			name: "list_org_repos_decode_error",
			call: func(t *testing.T, client *DataClient) error {
				t.Helper()
				_, err := client.ListOrgRepos(context.Background(), "test")
				return err
			},
		},
		{
			name: "contributor_stats_decode_error",
			call: func(t *testing.T, client *DataClient) error {
				t.Helper()
				_, err := client.GetContributorStats(context.Background(), "test", "repo")
				return err
			},
		},
		{
			name: "list_commits_decode_error",
			call: func(t *testing.T, client *DataClient) error {
				t.Helper()
				_, err := client.ListRepoCommitsWindow(context.Background(), "test", "repo", time.Now().Add(-time.Hour), time.Now(), 10)
				return err
			},
		},
		{
			name: "get_commit_decode_error",
			call: func(t *testing.T, client *DataClient) error {
				t.Helper()
				_, err := client.GetCommit(context.Background(), "test", "repo", "sha")
				return err
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			doer := &fakeDoer{
				responses: []*http.Response{
					newResponse(http.StatusOK, map[string]string{}, `{invalid json`),
				},
			}
			client, err := NewDataClient("", newTestRequestClient(doer))
			if err != nil {
				t.Fatalf("NewDataClient() unexpected error: %v", err)
			}
			if err := tc.call(t, client); err == nil {
				t.Fatalf("expected decode error, got nil")
			}
		})
	}
}
