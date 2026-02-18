package queue

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestNewRabbitMQHTTPBroker(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		cfg       RabbitMQHTTPConfig
		wantError bool
	}{
		{
			name: "valid_config",
			cfg: RabbitMQHTTPConfig{
				ManagementURL: "http://rabbitmq:15672",
				VHost:         "/",
				Exchange:      "gh.backfill",
			},
		},
		{
			name: "missing_management_url",
			cfg: RabbitMQHTTPConfig{
				VHost:    "/",
				Exchange: "gh.backfill",
			},
			wantError: true,
		},
		{
			name: "missing_exchange",
			cfg: RabbitMQHTTPConfig{
				ManagementURL: "http://rabbitmq:15672",
				VHost:         "/",
			},
			wantError: true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			broker, err := NewRabbitMQHTTPBroker(tc.cfg)
			if tc.wantError {
				if err == nil {
					t.Fatalf("NewRabbitMQHTTPBroker() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("NewRabbitMQHTTPBroker() unexpected error: %v", err)
			}
			if broker == nil {
				t.Fatalf("NewRabbitMQHTTPBroker() returned nil broker")
			}
		})
	}
}

func TestRabbitMQHTTPConfigFromAMQPURL(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		amqpURL      string
		exchange     string
		wantURL      string
		wantVHost    string
		wantUser     string
		wantPassword string
		wantError    bool
	}{
		{
			name:         "default_vhost",
			amqpURL:      "amqp://user:pass@rabbitmq:5672/",
			exchange:     "gh.backfill",
			wantURL:      "http://rabbitmq:15672",
			wantVHost:    "/",
			wantUser:     "user",
			wantPassword: "pass",
		},
		{
			name:         "custom_vhost",
			amqpURL:      "amqp://user:pass@rabbitmq:5672/team-vhost",
			exchange:     "gh.backfill",
			wantURL:      "http://rabbitmq:15672",
			wantVHost:    "team-vhost",
			wantUser:     "user",
			wantPassword: "pass",
		},
		{
			name:      "invalid_url",
			amqpURL:   "://bad",
			exchange:  "gh.backfill",
			wantError: true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := RabbitMQHTTPConfigFromAMQPURL(tc.amqpURL, tc.exchange)
			if tc.wantError {
				if err == nil {
					t.Fatalf("RabbitMQHTTPConfigFromAMQPURL() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("RabbitMQHTTPConfigFromAMQPURL() unexpected error: %v", err)
			}

			if cfg.ManagementURL != tc.wantURL {
				t.Fatalf("ManagementURL = %q, want %q", cfg.ManagementURL, tc.wantURL)
			}
			if cfg.VHost != tc.wantVHost {
				t.Fatalf("VHost = %q, want %q", cfg.VHost, tc.wantVHost)
			}
			if cfg.Username != tc.wantUser {
				t.Fatalf("Username = %q, want %q", cfg.Username, tc.wantUser)
			}
			if cfg.Password != tc.wantPassword {
				t.Fatalf("Password = %q, want %q", cfg.Password, tc.wantPassword)
			}
		})
	}
}

func TestRabbitMQHTTPBrokerPublish(t *testing.T) {
	t.Parallel()

	var (
		mu           sync.Mutex
		requestCount int
		lastBody     map[string]any
	)

	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method != http.MethodPost || req.URL.EscapedPath() != "/api/exchanges/%2F/gh.backfill/publish" {
				return jsonResponse(http.StatusNotFound, map[string]any{"error": "not found"}), nil
			}
			if user, pass, ok := req.BasicAuth(); !ok || user != "user" || pass != "pass" {
				return jsonResponse(http.StatusUnauthorized, map[string]any{"error": "unauthorized"}), nil
			}

			defer req.Body.Close()
			payload := make(map[string]any)
			if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
				return jsonResponse(http.StatusBadRequest, map[string]any{"error": err.Error()}), nil
			}

			mu.Lock()
			requestCount++
			lastBody = payload
			mu.Unlock()

			return jsonResponse(http.StatusOK, map[string]any{"routed": true}), nil
		}),
	}

	broker, err := NewRabbitMQHTTPBroker(RabbitMQHTTPConfig{
		ManagementURL: "http://rabbitmq.local",
		VHost:         "/",
		Exchange:      "gh.backfill",
		Username:      "user",
		Password:      "pass",
		HTTPClient:    client,
	})
	if err != nil {
		t.Fatalf("NewRabbitMQHTTPBroker() unexpected error: %v", err)
	}

	message := Message{
		ID:        "job-1",
		Body:      []byte(`{"org":"org-a"}`),
		Headers:   map[string]string{"content_type": "application/json"},
		CreatedAt: time.Unix(1739836800, 0),
		Attempt:   2,
	}
	if publishErr := broker.Publish(context.Background(), "gh.backfill.jobs", message); publishErr != nil {
		t.Fatalf("Publish() unexpected error: %v", publishErr)
	}

	mu.Lock()
	defer mu.Unlock()
	if requestCount != 1 {
		t.Fatalf("publish request count = %d, want 1", requestCount)
	}
	if got := lastBody["routing_key"]; got != "gh.backfill.jobs" {
		t.Fatalf("routing_key = %v, want gh.backfill.jobs", got)
	}
	if got := lastBody["payload_encoding"]; got != "base64" {
		t.Fatalf("payload_encoding = %v, want base64", got)
	}
	encodedPayload, ok := lastBody["payload"].(string)
	if !ok {
		t.Fatalf("payload type = %T, want string", lastBody["payload"])
	}
	decodedPayload, err := base64.StdEncoding.DecodeString(encodedPayload)
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if string(decodedPayload) != `{"org":"org-a"}` {
		t.Fatalf("decoded payload = %q, want %q", string(decodedPayload), `{"org":"org-a"}`)
	}
}

func TestRabbitMQHTTPBrokerConsume(t *testing.T) {
	t.Parallel()

	baseMessage := map[string]any{
		"payload":          base64.StdEncoding.EncodeToString([]byte("hello")),
		"payload_encoding": "base64",
		"properties": map[string]any{
			"headers": map[string]string{
				"id": "job-1",
			},
			"timestamp": float64(1739836800),
		},
	}

	var (
		mu       sync.Mutex
		getCalls int
	)
	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodPost && req.URL.EscapedPath() == "/api/queues/%2F/gh.backfill.jobs/get" {
				mu.Lock()
				getCalls++
				currentCall := getCalls
				mu.Unlock()

				if currentCall == 1 {
					return jsonResponse(http.StatusOK, []map[string]any{baseMessage}), nil
				}
				return jsonResponse(http.StatusOK, []map[string]any{}), nil
			}
			return jsonResponse(http.StatusNotFound, map[string]any{"error": "not found"}), nil
		}),
	}

	broker, err := NewRabbitMQHTTPBroker(RabbitMQHTTPConfig{
		ManagementURL: "http://rabbitmq.local",
		VHost:         "/",
		Exchange:      "gh.backfill",
		PollInterval:  2 * time.Millisecond,
		HTTPClient:    client,
	})
	if err != nil {
		t.Fatalf("NewRabbitMQHTTPBroker() unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumed := make(chan Message, 1)
	go broker.Consume(ctx, "gh.backfill.jobs", ConsumerConfig{
		MaxMessageAge: 24 * time.Hour,
		Now: func() time.Time {
			return time.Unix(1739836800, 0)
		},
		Sleep: func(time.Duration) {},
	}, func(_ context.Context, msg Message) error {
		consumed <- msg
		cancel()
		return nil
	})

	select {
	case msg := <-consumed:
		if string(msg.Body) != "hello" {
			t.Fatalf("body = %q, want hello", string(msg.Body))
		}
		if msg.Headers["id"] != "job-1" {
			t.Fatalf("headers.id = %q, want job-1", msg.Headers["id"])
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for consumed message")
	}
}

func TestRabbitMQHTTPBrokerDepth(t *testing.T) {
	t.Parallel()

	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method != http.MethodGet || req.URL.EscapedPath() != "/api/queues/%2F/gh.backfill.jobs" {
				return jsonResponse(http.StatusNotFound, map[string]any{"error": "not found"}), nil
			}
			return jsonResponse(http.StatusOK, map[string]any{"messages": 7}), nil
		}),
	}

	broker, err := NewRabbitMQHTTPBroker(RabbitMQHTTPConfig{
		ManagementURL: "http://rabbitmq.local",
		VHost:         "/",
		Exchange:      "gh.backfill",
		HTTPClient:    client,
	})
	if err != nil {
		t.Fatalf("NewRabbitMQHTTPBroker() unexpected error: %v", err)
	}

	if got := broker.Depth("gh.backfill.jobs"); got != 7 {
		t.Fatalf("Depth() = %d, want 7", got)
	}
}

func TestRabbitMQHTTPBrokerEnsureTopology(t *testing.T) {
	t.Parallel()

	var (
		mu    sync.Mutex
		calls = make(map[string]int)
	)
	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			mu.Lock()
			calls[req.Method+" "+req.URL.EscapedPath()]++
			mu.Unlock()
			return jsonResponse(http.StatusCreated, map[string]any{}), nil
		}),
	}

	broker, err := NewRabbitMQHTTPBroker(RabbitMQHTTPConfig{
		ManagementURL: "http://rabbitmq.local",
		VHost:         "/",
		Exchange:      "gh.backfill",
		HTTPClient:    client,
	})
	if err != nil {
		t.Fatalf("NewRabbitMQHTTPBroker() unexpected error: %v", err)
	}

	err = broker.EnsureTopology(context.Background(), TopologyConfig{
		MainQueue:       "gh.backfill.jobs",
		DeadLetterQueue: "gh.backfill.dlq",
		RetryQueues: []RetryQueueSpec{
			{Name: "gh.backfill.jobs.retry.1m0s", Delay: time.Minute},
			{Name: "gh.backfill.jobs.retry.5m0s", Delay: 5 * time.Minute},
		},
	})
	if err != nil {
		t.Fatalf("EnsureTopology() unexpected error: %v", err)
	}

	requiredCalls := []string{
		"PUT /api/exchanges/%2F/gh.backfill",
		"PUT /api/queues/%2F/gh.backfill.jobs",
		"POST /api/bindings/%2F/e/gh.backfill/q/gh.backfill.jobs",
		"PUT /api/queues/%2F/gh.backfill.dlq",
		"POST /api/bindings/%2F/e/gh.backfill/q/gh.backfill.dlq",
		"PUT /api/queues/%2F/gh.backfill.jobs.retry.1m0s",
		"POST /api/bindings/%2F/e/gh.backfill/q/gh.backfill.jobs.retry.1m0s",
		"PUT /api/queues/%2F/gh.backfill.jobs.retry.5m0s",
		"POST /api/bindings/%2F/e/gh.backfill/q/gh.backfill.jobs.retry.5m0s",
	}
	for _, call := range requiredCalls {
		mu.Lock()
		count := calls[call]
		mu.Unlock()
		if count != 1 {
			t.Fatalf("call %q count = %d, want 1", call, count)
		}
	}
}

func TestRabbitMQHTTPBrokerHealth(t *testing.T) {
	t.Parallel()

	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method != http.MethodGet || req.URL.EscapedPath() != "/api/queues/%2F/gh.backfill.jobs" {
				return jsonResponse(http.StatusNotFound, map[string]any{"error": "not found"}), nil
			}
			return jsonResponse(http.StatusOK, map[string]any{"messages": 0}), nil
		}),
	}

	broker, err := NewRabbitMQHTTPBroker(RabbitMQHTTPConfig{
		ManagementURL: "http://rabbitmq.local",
		VHost:         "/",
		Exchange:      "gh.backfill",
		HTTPClient:    client,
	})
	if err != nil {
		t.Fatalf("NewRabbitMQHTTPBroker() unexpected error: %v", err)
	}
	if err := broker.Health(context.Background(), "gh.backfill.jobs"); err != nil {
		t.Fatalf("Health() unexpected error: %v", err)
	}
	if err := broker.Health(context.Background(), ""); err == nil {
		t.Fatalf("Health(empty queue) expected error, got nil")
	}
}

func TestRabbitMQHTTPBrokerEmitsTracingSpans(t *testing.T) {
	previousProvider := otel.GetTracerProvider()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(recorder),
	)
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previousProvider)
		_ = provider.Shutdown(context.Background())
	})

	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method != http.MethodPost || req.URL.EscapedPath() != "/api/exchanges/%2F/gh.backfill/publish" {
				return jsonResponse(http.StatusNotFound, map[string]any{"error": "not found"}), nil
			}
			return jsonResponse(http.StatusOK, map[string]any{"routed": true}), nil
		}),
	}

	broker, err := NewRabbitMQHTTPBroker(RabbitMQHTTPConfig{
		ManagementURL: "http://rabbitmq.local",
		VHost:         "/",
		Exchange:      "gh.backfill",
		HTTPClient:    client,
	})
	if err != nil {
		t.Fatalf("NewRabbitMQHTTPBroker() unexpected error: %v", err)
	}

	if err := broker.Publish(context.Background(), "gh.backfill.jobs", Message{
		ID:   "job-1",
		Body: []byte(`{"org":"org-a"}`),
	}); err != nil {
		t.Fatalf("Publish() unexpected error: %v", err)
	}

	found := false
	for _, span := range recorder.Ended() {
		if span.Name() == "rabbitmq.http.request" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("missing rabbitmq.http.request span")
	}
}

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func jsonResponse(statusCode int, payload any) *http.Response {
	bodyBytes, _ := json.Marshal(payload)
	return &http.Response{
		StatusCode: statusCode,
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Body: io.NopCloser(strings.NewReader(string(bodyBytes))),
	}
}
