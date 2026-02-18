package githubapi

import (
	"fmt"
	"net/http"
	"time"

	"github.com/cam3ron2/github-stats/internal/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// RetryConfig configures GitHub client retry behavior.
type RetryConfig struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// HTTPDoer is implemented by http.Client.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// CallMetadata reports execution metadata for a client call.
type CallMetadata struct {
	Attempts        int
	LastRateHeaders RateLimitHeaders
	LastDecision    Decision
}

// Client wraps GitHub HTTP requests with retry and rate-limit controls.
type Client struct {
	doer       HTTPDoer
	retry      RetryConfig
	ratePolicy RateLimitPolicy
	// Sleep is injected for testability.
	Sleep func(duration time.Duration)
}

// NewClient creates a GitHub API client wrapper.
func NewClient(doer HTTPDoer, retry RetryConfig, ratePolicy RateLimitPolicy) *Client {
	if retry.MaxAttempts <= 0 {
		retry.MaxAttempts = 1
	}
	return &Client{
		doer:       doer,
		retry:      retry,
		ratePolicy: ratePolicy,
		Sleep:      time.Sleep,
	}
}

// Do executes a request with retry and rate-limit awareness.
func (c *Client) Do(req *http.Request) (*http.Response, CallMetadata, error) {
	if req == nil {
		return nil, CallMetadata{}, fmt.Errorf("request is nil")
	}

	ctx := req.Context()
	var span trace.Span
	if telemetry.ShouldTraceDependencies() {
		ctx, span = otel.Tracer("github-stats/internal/githubapi").Start(
			ctx,
			"githubapi.client.do",
			trace.WithAttributes(
				attribute.String("http.method", req.Method),
				attribute.String("http.path", req.URL.EscapedPath()),
				attribute.Int("github.max_attempts", c.retry.MaxAttempts),
			),
		)
		defer span.End()
	}

	metadata := CallMetadata{}
	for attempt := 1; attempt <= c.retry.MaxAttempts; attempt++ {
		metadata.Attempts = attempt

		nextReq := req.Clone(ctx)
		resp, err := c.doer.Do(nextReq)
		if err != nil {
			if span != nil {
				span.RecordError(err)
				span.AddEvent("attempt_failed", trace.WithAttributes(
					attribute.Int("github.attempt", attempt),
				))
			}
			if attempt == c.retry.MaxAttempts {
				if span != nil {
					span.SetStatus(codes.Error, err.Error())
				}
				return nil, metadata, err
			}
			c.Sleep(backoffForAttempt(c.retry, attempt))
			continue
		}

		headers := ParseRateLimitHeaders(resp.Header, resp.StatusCode)
		metadata.LastRateHeaders = headers
		decision := c.ratePolicy.Evaluate(headers)
		metadata.LastDecision = decision

		if span != nil {
			span.AddEvent("attempt_completed", trace.WithAttributes(
				attribute.Int("github.attempt", attempt),
				attribute.Int("http.status_code", resp.StatusCode),
				attribute.Int("github.rate_limit_remaining", headers.Remaining),
				attribute.Int64("github.rate_limit_reset_unix", headers.ResetUnix),
				attribute.Bool("github.rate_limit_allow", decision.Allow),
				attribute.String("github.rate_limit_reason", decision.Reason),
			))
		}

		if !decision.Allow {
			if resp.Body != nil {
				_ = resp.Body.Close()
			}
			if attempt == c.retry.MaxAttempts {
				if span != nil {
					span.SetStatus(codes.Error, "rate-limited")
				}
				return resp, metadata, nil
			}
			c.Sleep(decision.WaitFor)
			continue
		}

		if isTransientStatus(resp.StatusCode) {
			if attempt == c.retry.MaxAttempts {
				if span != nil {
					span.SetStatus(codes.Error, fmt.Sprintf("transient status %d", resp.StatusCode))
				}
				return resp, metadata, nil
			}
			if resp.Body != nil {
				_ = resp.Body.Close()
			}
			c.Sleep(backoffForAttempt(c.retry, attempt))
			continue
		}

		if span != nil {
			span.SetStatus(codes.Ok, "request completed")
		}
		return resp, metadata, nil
	}

	if span != nil {
		span.SetStatus(codes.Error, "request attempts exhausted")
	}
	return nil, metadata, fmt.Errorf("request attempts exhausted")
}

func isTransientStatus(statusCode int) bool {
	if statusCode == http.StatusTooManyRequests {
		return true
	}
	return statusCode >= 500 && statusCode <= 599
}

func backoffForAttempt(retry RetryConfig, attempt int) time.Duration {
	backoff := retry.InitialBackoff
	for i := 1; i < attempt; i++ {
		backoff *= 2
		if retry.MaxBackoff > 0 && backoff > retry.MaxBackoff {
			return retry.MaxBackoff
		}
	}
	if retry.MaxBackoff > 0 && backoff > retry.MaxBackoff {
		return retry.MaxBackoff
	}
	return backoff
}
