package githubapi

import (
	"net/http"
	"strconv"
	"time"
)

// RateLimitHeaders contains parsed GitHub rate-limit response headers.
type RateLimitHeaders struct {
	Remaining        int
	ResetUnix        int64
	Used             int
	RetryAfter       time.Duration
	SecondaryLimited bool
}

// Decision represents a rate-limit action decision.
type Decision struct {
	Allow   bool
	WaitFor time.Duration
	Reason  string
}

// RateLimitPolicy evaluates rate-limit actions from parsed headers.
type RateLimitPolicy struct {
	MinRemainingThreshold int
	MinResetBuffer        time.Duration
	SecondaryLimitBackoff time.Duration
	Now                   func() time.Time
}

// ParseRateLimitHeaders parses rate-limit and retry headers.
func ParseRateLimitHeaders(header http.Header, statusCode int) RateLimitHeaders {
	parsed := RateLimitHeaders{}
	parsed.Remaining = parseInt(header.Get("X-RateLimit-Remaining"))
	parsed.Used = parseInt(header.Get("X-RateLimit-Used"))
	parsed.ResetUnix = parseInt64(header.Get("X-RateLimit-Reset"))

	retryAfterSeconds := parseInt(header.Get("Retry-After"))
	if retryAfterSeconds > 0 {
		parsed.RetryAfter = time.Duration(retryAfterSeconds) * time.Second
	}

	if statusCode == http.StatusTooManyRequests {
		parsed.SecondaryLimited = true
	}
	if statusCode == http.StatusForbidden && parsed.RetryAfter > 0 {
		parsed.SecondaryLimited = true
	}

	return parsed
}

// Evaluate decides whether calls may continue or should pause.
func (p RateLimitPolicy) Evaluate(headers RateLimitHeaders) Decision {
	now := time.Now()
	if p.Now != nil {
		now = p.Now()
	}

	if headers.SecondaryLimited {
		waitFor := p.SecondaryLimitBackoff
		if headers.RetryAfter > waitFor {
			waitFor = headers.RetryAfter
		}
		return Decision{
			Allow:   false,
			WaitFor: waitFor,
			Reason:  "secondary_limit",
		}
	}

	if headers.Remaining >= p.MinRemainingThreshold {
		return Decision{
			Allow:   true,
			WaitFor: 0,
			Reason:  "within_budget",
		}
	}

	resetAt := time.Unix(headers.ResetUnix, 0)
	if !resetAt.After(now) {
		return Decision{
			Allow:   true,
			WaitFor: 0,
			Reason:  "reset_elapsed",
		}
	}

	return Decision{
		Allow:   false,
		WaitFor: resetAt.Sub(now) + p.MinResetBuffer,
		Reason:  "remaining_below_threshold",
	}
}

func parseInt(raw string) int {
	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return 0
	}
	return parsed
}

func parseInt64(raw string) int64 {
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}
