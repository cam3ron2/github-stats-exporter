package githubapi

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestDataClientCopilotReportLinks(t *testing.T) {
	t.Parallel()

	day := time.Date(2026, time.February, 18, 15, 4, 5, 0, time.FixedZone("UTC+2", 2*3600))
	testCases := []struct {
		name         string
		call         func(t *testing.T, client *DataClient) (CopilotReportLinkResult, error)
		wantPath     string
		wantDayQuery string
	}{
		{
			name: "org_1d",
			call: func(t *testing.T, client *DataClient) (CopilotReportLinkResult, error) {
				t.Helper()
				return client.GetOrgCopilotOrganization1DayReportLink(context.Background(), "acme", day)
			},
			wantPath:     "/orgs/acme/copilot/metrics/reports/organization-1-day",
			wantDayQuery: "2026-02-18",
		},
		{
			name: "org_28d_latest",
			call: func(t *testing.T, client *DataClient) (CopilotReportLinkResult, error) {
				t.Helper()
				return client.GetOrgCopilotOrganization28DayLatestReportLink(context.Background(), "acme")
			},
			wantPath: "/orgs/acme/copilot/metrics/reports/organization-28-day/latest",
		},
		{
			name: "org_users_1d",
			call: func(t *testing.T, client *DataClient) (CopilotReportLinkResult, error) {
				t.Helper()
				return client.GetOrgCopilotUsers1DayReportLink(context.Background(), "acme", day)
			},
			wantPath:     "/orgs/acme/copilot/metrics/reports/users-1-day",
			wantDayQuery: "2026-02-18",
		},
		{
			name: "org_users_28d_latest",
			call: func(t *testing.T, client *DataClient) (CopilotReportLinkResult, error) {
				t.Helper()
				return client.GetOrgCopilotUsers28DayLatestReportLink(context.Background(), "acme")
			},
			wantPath: "/orgs/acme/copilot/metrics/reports/users-28-day/latest",
		},
		{
			name: "enterprise_1d",
			call: func(t *testing.T, client *DataClient) (CopilotReportLinkResult, error) {
				t.Helper()
				return client.GetEnterpriseCopilotEnterprise1DayReportLink(context.Background(), "ent", day)
			},
			wantPath:     "/enterprises/ent/copilot/metrics/reports/enterprise-1-day",
			wantDayQuery: "2026-02-18",
		},
		{
			name: "enterprise_28d_latest",
			call: func(t *testing.T, client *DataClient) (CopilotReportLinkResult, error) {
				t.Helper()
				return client.GetEnterpriseCopilotEnterprise28DayLatestReportLink(context.Background(), "ent")
			},
			wantPath: "/enterprises/ent/copilot/metrics/reports/enterprise-28-day/latest",
		},
		{
			name: "enterprise_users_1d",
			call: func(t *testing.T, client *DataClient) (CopilotReportLinkResult, error) {
				t.Helper()
				return client.GetEnterpriseCopilotUsers1DayReportLink(context.Background(), "ent", day)
			},
			wantPath:     "/enterprises/ent/copilot/metrics/reports/users-1-day",
			wantDayQuery: "2026-02-18",
		},
		{
			name: "enterprise_users_28d_latest",
			call: func(t *testing.T, client *DataClient) (CopilotReportLinkResult, error) {
				t.Helper()
				return client.GetEnterpriseCopilotUsers28DayLatestReportLink(context.Background(), "ent")
			},
			wantPath: "/enterprises/ent/copilot/metrics/reports/users-28-day/latest",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			doer := &fakeDoer{
				responses: []*http.Response{
					newResponse(
						http.StatusOK,
						map[string]string{},
						`{"download_links":["https://reports.example.com/report.ndjson"],"report_end_day":"2026-02-18"}`,
					),
				},
			}
			client, err := NewDataClient("", newTestRequestClient(doer))
			if err != nil {
				t.Fatalf("NewDataClient() unexpected error: %v", err)
			}

			got, err := tc.call(t, client)
			if err != nil {
				t.Fatalf("copilot report link call unexpected error: %v", err)
			}
			if got.Status != EndpointStatusOK {
				t.Fatalf("Status = %q, want %q", got.Status, EndpointStatusOK)
			}
			if got.URL != "https://reports.example.com/report.ndjson" {
				t.Fatalf("URL = %q, want report URL", got.URL)
			}
			if len(got.DownloadLinks) != 1 {
				t.Fatalf("len(DownloadLinks) = %d, want 1", len(got.DownloadLinks))
			}
			if len(doer.requests) != 1 {
				t.Fatalf("len(requests) = %d, want 1", len(doer.requests))
			}

			requestURL := doer.requests[0].URL
			if requestURL.Path != tc.wantPath {
				t.Fatalf("request path = %q, want %q", requestURL.Path, tc.wantPath)
			}

			gotDay := requestURL.Query().Get("date")
			if gotDay != tc.wantDayQuery {
				t.Fatalf("date query = %q, want %q", gotDay, tc.wantDayQuery)
			}
		})
	}
}

func TestDataClientCopilotReportLinkStatusAndValidation(t *testing.T) {
	t.Parallel()

	day := time.Date(2026, time.February, 18, 0, 0, 0, 0, time.UTC)
	testCases := []struct {
		name          string
		doer          *fakeDoer
		call          func(t *testing.T, client *DataClient) (CopilotReportLinkResult, error)
		wantStatus    EndpointStatus
		wantErrSubstr string
	}{
		{
			name: "forbidden_maps_status",
			doer: &fakeDoer{
				responses: []*http.Response{
					newResponse(http.StatusForbidden, map[string]string{}, `{"message":"forbidden"}`),
				},
			},
			call: func(t *testing.T, client *DataClient) (CopilotReportLinkResult, error) {
				t.Helper()
				return client.GetOrgCopilotOrganization28DayLatestReportLink(context.Background(), "acme")
			},
			wantStatus: EndpointStatusForbidden,
		},
		{
			name: "not_found_maps_status",
			doer: &fakeDoer{
				responses: []*http.Response{
					newResponse(http.StatusNotFound, map[string]string{}, `{"message":"missing"}`),
				},
			},
			call: func(t *testing.T, client *DataClient) (CopilotReportLinkResult, error) {
				t.Helper()
				return client.GetOrgCopilotOrganization28DayLatestReportLink(context.Background(), "acme")
			},
			wantStatus: EndpointStatusNotFound,
		},
		{
			name: "service_failure_maps_unavailable",
			doer: &fakeDoer{
				responses: []*http.Response{
					newResponse(http.StatusBadGateway, map[string]string{}, `bad gateway`),
				},
			},
			call: func(t *testing.T, client *DataClient) (CopilotReportLinkResult, error) {
				t.Helper()
				return client.GetOrgCopilotOrganization28DayLatestReportLink(context.Background(), "acme")
			},
			wantStatus: EndpointStatusUnavailable,
		},
		{
			name: "missing_owner_rejected",
			doer: &fakeDoer{},
			call: func(t *testing.T, client *DataClient) (CopilotReportLinkResult, error) {
				t.Helper()
				return client.GetOrgCopilotOrganization28DayLatestReportLink(context.Background(), " ")
			},
			wantErrSubstr: "owner is required",
		},
		{
			name: "one_day_requires_day",
			doer: &fakeDoer{},
			call: func(t *testing.T, client *DataClient) (CopilotReportLinkResult, error) {
				t.Helper()
				return client.GetOrgCopilotOrganization1DayReportLink(context.Background(), "acme", time.Time{})
			},
			wantErrSubstr: "day is required",
		},
		{
			name: "malformed_response_fails",
			doer: &fakeDoer{
				responses: []*http.Response{
					newResponse(http.StatusOK, map[string]string{}, `{invalid-json`),
				},
			},
			call: func(t *testing.T, client *DataClient) (CopilotReportLinkResult, error) {
				t.Helper()
				return client.GetOrgCopilotOrganization28DayLatestReportLink(context.Background(), "acme")
			},
			wantErrSubstr: "decode copilot report link response",
		},
		{
			name: "url_missing_fails",
			doer: &fakeDoer{
				responses: []*http.Response{
					newResponse(http.StatusOK, map[string]string{}, `{}`),
				},
			},
			call: func(t *testing.T, client *DataClient) (CopilotReportLinkResult, error) {
				t.Helper()
				return client.GetOrgCopilotOrganization28DayLatestReportLink(context.Background(), "acme")
			},
			wantErrSubstr: "missing url",
		},
		{
			name: "alternate_download_url_field_supported",
			doer: &fakeDoer{
				responses: []*http.Response{
					newResponse(http.StatusOK, map[string]string{}, `{"download_url":"https://reports.example.com/download.ndjson","report_day":"2026-02-18"}`),
				},
			},
			call: func(t *testing.T, client *DataClient) (CopilotReportLinkResult, error) {
				t.Helper()
				return client.GetEnterpriseCopilotUsers1DayReportLink(context.Background(), "ent", day)
			},
			wantStatus: EndpointStatusOK,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, err := NewDataClient("", newTestRequestClient(tc.doer))
			if err != nil {
				t.Fatalf("NewDataClient() unexpected error: %v", err)
			}

			got, err := tc.call(t, client)
			if tc.wantErrSubstr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantErrSubstr)
				}
				if !strings.Contains(err.Error(), tc.wantErrSubstr) {
					t.Fatalf("error = %q, missing %q", err.Error(), tc.wantErrSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("call unexpected error: %v", err)
			}
			if got.Status != tc.wantStatus {
				t.Fatalf("Status = %q, want %q", got.Status, tc.wantStatus)
			}
		})
	}
}

func TestDataClientStreamCopilotReportNDJSON(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name            string
		response        *http.Response
		url             string
		handler         func(record map[string]any) error
		wantStatus      EndpointStatus
		wantRecords     int
		wantParseErrors int
		wantErrSubstr   string
	}{
		{
			name: "streams_and_counts_parse_errors",
			response: newResponse(http.StatusOK, map[string]string{}, strings.Join([]string{
				`{"day":"2026-02-18","user":"alice","value":12}`,
				``,
				`{invalid}`,
				`{"day":"2026-02-18","user":"bob","value":7}`,
				``,
			}, "\n")),
			url:             "https://reports.example.com/copilot.ndjson",
			wantStatus:      EndpointStatusOK,
			wantRecords:     2,
			wantParseErrors: 1,
		},
		{
			name:          "handler_error_stops_stream",
			response:      newResponse(http.StatusOK, map[string]string{}, `{"user":"alice"}`+"\n"),
			url:           "https://reports.example.com/copilot.ndjson",
			wantErrSubstr: "handle copilot report line 1",
			handler: func(_ map[string]any) error {
				return context.DeadlineExceeded
			},
		},
		{
			name:            "non_success_status_returns_without_error",
			response:        newResponse(http.StatusForbidden, map[string]string{}, `{"message":"forbidden"}`),
			url:             "https://reports.example.com/copilot.ndjson",
			wantStatus:      EndpointStatusForbidden,
			wantRecords:     0,
			wantParseErrors: 0,
		},
		{
			name:          "invalid_url_rejected",
			response:      newResponse(http.StatusOK, map[string]string{}, `{}`),
			url:           "://bad-url",
			wantErrSubstr: "signed report url is invalid",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			doer := &fakeDoer{
				responses: []*http.Response{tc.response},
			}
			client, err := NewDataClient("", newTestRequestClient(doer))
			if err != nil {
				t.Fatalf("NewDataClient() unexpected error: %v", err)
			}

			recordsSeen := 0
			handler := tc.handler
			if handler == nil {
				handler = func(_ map[string]any) error {
					recordsSeen++
					return nil
				}
			}

			got, err := client.StreamCopilotReportNDJSON(context.Background(), tc.url, handler)
			if tc.wantErrSubstr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantErrSubstr)
				}
				if !strings.Contains(err.Error(), tc.wantErrSubstr) {
					t.Fatalf("error = %q, missing %q", err.Error(), tc.wantErrSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("StreamCopilotReportNDJSON() unexpected error: %v", err)
			}

			if got.Status != tc.wantStatus {
				t.Fatalf("Status = %q, want %q", got.Status, tc.wantStatus)
			}
			if got.RecordsParsed != tc.wantRecords {
				t.Fatalf("RecordsParsed = %d, want %d", got.RecordsParsed, tc.wantRecords)
			}
			if got.ParseErrors != tc.wantParseErrors {
				t.Fatalf("ParseErrors = %d, want %d", got.ParseErrors, tc.wantParseErrors)
			}
			if recordsSeen != tc.wantRecords {
				t.Fatalf("handler records seen = %d, want %d", recordsSeen, tc.wantRecords)
			}
		})
	}
}
