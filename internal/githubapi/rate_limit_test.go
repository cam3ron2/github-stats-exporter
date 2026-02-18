package githubapi

import (
	"net/http"
	"testing"
	"time"
)

func TestParseRateLimitHeaders(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		statusCode int
		headers    map[string]string
		want       RateLimitHeaders
	}{
		{
			name:       "parses_standard_headers",
			statusCode: http.StatusOK,
			headers: map[string]string{
				"X-RateLimit-Remaining": "4999",
				"X-RateLimit-Reset":     "1739837000",
				"X-RateLimit-Used":      "1",
			},
			want: RateLimitHeaders{
				Remaining: 4999,
				Used:      1,
				ResetUnix: 1739837000,
			},
		},
		{
			name:       "detects_secondary_limit_from_retry_after",
			statusCode: http.StatusForbidden,
			headers: map[string]string{
				"Retry-After": "60",
			},
			want: RateLimitHeaders{
				RetryAfter:       60 * time.Second,
				SecondaryLimited: true,
			},
		},
		{
			name:       "handles_invalid_values_safely",
			statusCode: http.StatusTooManyRequests,
			headers: map[string]string{
				"X-RateLimit-Remaining": "abc",
				"X-RateLimit-Reset":     "xyz",
				"Retry-After":           "nan",
			},
			want: RateLimitHeaders{
				SecondaryLimited: true,
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			header := make(http.Header)
			for key, value := range tc.headers {
				header.Set(key, value)
			}

			got := ParseRateLimitHeaders(header, tc.statusCode)
			if got.Remaining != tc.want.Remaining {
				t.Fatalf("Remaining = %d, want %d", got.Remaining, tc.want.Remaining)
			}
			if got.Used != tc.want.Used {
				t.Fatalf("Used = %d, want %d", got.Used, tc.want.Used)
			}
			if got.ResetUnix != tc.want.ResetUnix {
				t.Fatalf("ResetUnix = %d, want %d", got.ResetUnix, tc.want.ResetUnix)
			}
			if got.RetryAfter != tc.want.RetryAfter {
				t.Fatalf("RetryAfter = %s, want %s", got.RetryAfter, tc.want.RetryAfter)
			}
			if got.SecondaryLimited != tc.want.SecondaryLimited {
				t.Fatalf("SecondaryLimited = %t, want %t", got.SecondaryLimited, tc.want.SecondaryLimited)
			}
		})
	}
}

func TestRateLimitPolicyEvaluate(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0)
	policy := RateLimitPolicy{
		MinRemainingThreshold: 200,
		MinResetBuffer:        10 * time.Second,
		SecondaryLimitBackoff: 60 * time.Second,
		Now: func() time.Time {
			return now
		},
	}

	testCases := []struct {
		name string
		in   RateLimitHeaders
		want Decision
	}{
		{
			name: "allow_when_budget_available",
			in: RateLimitHeaders{
				Remaining: 500,
				ResetUnix: now.Add(2 * time.Minute).Unix(),
			},
			want: Decision{
				Allow:   true,
				WaitFor: 0,
				Reason:  "within_budget",
			},
		},
		{
			name: "pause_when_remaining_below_threshold",
			in: RateLimitHeaders{
				Remaining: 100,
				ResetUnix: now.Add(2 * time.Minute).Unix(),
			},
			want: Decision{
				Allow:   false,
				WaitFor: 130 * time.Second,
				Reason:  "remaining_below_threshold",
			},
		},
		{
			name: "secondary_limit_uses_retry_after_when_higher",
			in: RateLimitHeaders{
				SecondaryLimited: true,
				RetryAfter:       90 * time.Second,
			},
			want: Decision{
				Allow:   false,
				WaitFor: 90 * time.Second,
				Reason:  "secondary_limit",
			},
		},
		{
			name: "secondary_limit_uses_policy_backoff_when_retry_after_missing",
			in: RateLimitHeaders{
				SecondaryLimited: true,
			},
			want: Decision{
				Allow:   false,
				WaitFor: 60 * time.Second,
				Reason:  "secondary_limit",
			},
		},
		{
			name: "allow_if_reset_time_already_elapsed",
			in: RateLimitHeaders{
				Remaining: 100,
				ResetUnix: now.Add(-1 * time.Minute).Unix(),
			},
			want: Decision{
				Allow:   true,
				WaitFor: 0,
				Reason:  "reset_elapsed",
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := policy.Evaluate(tc.in)
			if got.Allow != tc.want.Allow {
				t.Fatalf("Allow = %t, want %t", got.Allow, tc.want.Allow)
			}
			if got.WaitFor != tc.want.WaitFor {
				t.Fatalf("WaitFor = %s, want %s", got.WaitFor, tc.want.WaitFor)
			}
			if got.Reason != tc.want.Reason {
				t.Fatalf("Reason = %q, want %q", got.Reason, tc.want.Reason)
			}
		})
	}
}
