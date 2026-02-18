package githubapi

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type fakeDoer struct {
	responses []*http.Response
	errors    []error
	callCount int
}

func (d *fakeDoer) Do(_ *http.Request) (*http.Response, error) {
	idx := d.callCount
	d.callCount++

	var resp *http.Response
	if idx < len(d.responses) {
		resp = d.responses[idx]
	}
	var err error
	if idx < len(d.errors) {
		err = d.errors[idx]
	}
	return resp, err
}

func newResponse(status int, headers map[string]string, body string) *http.Response {
	header := make(http.Header)
	for key, value := range headers {
		header.Set(key, value)
	}
	responseBody := io.NopCloser(strings.NewReader(body))
	return &http.Response{
		StatusCode: status,
		Header:     header,
		Body:       responseBody,
	}
}

func TestClientDo(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0)
	testCases := []struct {
		name          string
		doer          *fakeDoer
		retryConfig   RetryConfig
		ratePolicy    RateLimitPolicy
		wantAttempts  int
		wantErr       bool
		wantStatus    int
		wantSleepCall int
	}{
		{
			name: "retries_transient_5xx_and_succeeds",
			doer: &fakeDoer{
				responses: []*http.Response{
					newResponse(http.StatusInternalServerError, map[string]string{}, "boom"),
					newResponse(http.StatusOK, map[string]string{"X-RateLimit-Remaining": "4999"}, "ok"),
				},
			},
			retryConfig: RetryConfig{
				MaxAttempts:    3,
				InitialBackoff: 1 * time.Second,
				MaxBackoff:     5 * time.Second,
			},
			ratePolicy: RateLimitPolicy{
				MinRemainingThreshold: 200,
				MinResetBuffer:        10 * time.Second,
				SecondaryLimitBackoff: 60 * time.Second,
				Now: func() time.Time {
					return now
				},
			},
			wantAttempts:  2,
			wantErr:       false,
			wantStatus:    http.StatusOK,
			wantSleepCall: 1,
		},
		{
			name: "does_not_retry_permanent_4xx",
			doer: &fakeDoer{
				responses: []*http.Response{
					newResponse(http.StatusNotFound, map[string]string{}, "not found"),
				},
			},
			retryConfig: RetryConfig{
				MaxAttempts:    3,
				InitialBackoff: 1 * time.Second,
				MaxBackoff:     5 * time.Second,
			},
			ratePolicy: RateLimitPolicy{
				MinRemainingThreshold: 200,
				MinResetBuffer:        10 * time.Second,
				SecondaryLimitBackoff: 60 * time.Second,
				Now: func() time.Time {
					return now
				},
			},
			wantAttempts:  1,
			wantErr:       false,
			wantStatus:    http.StatusNotFound,
			wantSleepCall: 0,
		},
		{
			name: "secondary_limit_waits_then_retries",
			doer: &fakeDoer{
				responses: []*http.Response{
					newResponse(http.StatusForbidden, map[string]string{"Retry-After": "90"}, "secondary"),
					newResponse(http.StatusOK, map[string]string{"X-RateLimit-Remaining": "4999"}, "ok"),
				},
			},
			retryConfig: RetryConfig{
				MaxAttempts:    3,
				InitialBackoff: 1 * time.Second,
				MaxBackoff:     5 * time.Second,
			},
			ratePolicy: RateLimitPolicy{
				MinRemainingThreshold: 200,
				MinResetBuffer:        10 * time.Second,
				SecondaryLimitBackoff: 60 * time.Second,
				Now: func() time.Time {
					return now
				},
			},
			wantAttempts:  2,
			wantErr:       false,
			wantStatus:    http.StatusOK,
			wantSleepCall: 1,
		},
		{
			name: "network_errors_retry_until_exhausted",
			doer: &fakeDoer{
				errors: []error{
					fmt.Errorf("network down"),
					fmt.Errorf("network down"),
				},
			},
			retryConfig: RetryConfig{
				MaxAttempts:    2,
				InitialBackoff: 1 * time.Second,
				MaxBackoff:     5 * time.Second,
			},
			ratePolicy: RateLimitPolicy{
				MinRemainingThreshold: 200,
				MinResetBuffer:        10 * time.Second,
				SecondaryLimitBackoff: 60 * time.Second,
				Now: func() time.Time {
					return now
				},
			},
			wantAttempts:  2,
			wantErr:       true,
			wantStatus:    0,
			wantSleepCall: 1,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sleepCalls := 0
			client := NewClient(tc.doer, tc.retryConfig, tc.ratePolicy)
			client.Sleep = func(_ time.Duration) {
				sleepCalls++
			}

			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://api.github.com/repos", nil)
			if err != nil {
				t.Fatalf("NewRequestWithContext() unexpected error: %v", err)
			}

			resp, metadata, callErr := client.Do(req)
			if resp != nil && resp.Body != nil {
				t.Cleanup(func() {
					if closeErr := resp.Body.Close(); closeErr != nil {
						t.Fatalf("response body close failed: %v", closeErr)
					}
				})
			}
			if tc.wantErr && callErr == nil {
				t.Fatalf("Do() expected error, got nil")
			}
			if !tc.wantErr && callErr != nil {
				t.Fatalf("Do() unexpected error: %v", callErr)
			}
			if metadata.Attempts != tc.wantAttempts {
				t.Fatalf("Attempts = %d, want %d", metadata.Attempts, tc.wantAttempts)
			}
			if tc.wantStatus == 0 {
				if resp != nil {
					t.Fatalf("response = %v, want nil", resp)
				}
			} else if resp == nil || resp.StatusCode != tc.wantStatus {
				got := 0
				if resp != nil {
					got = resp.StatusCode
				}
				t.Fatalf("status = %d, want %d", got, tc.wantStatus)
			}
			if sleepCalls != tc.wantSleepCall {
				t.Fatalf("sleepCalls = %d, want %d", sleepCalls, tc.wantSleepCall)
			}
		})
	}
}
