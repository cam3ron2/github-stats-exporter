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

func TestRabbitMQHTTPBrokerEnsureTopologyQueueArguments(t *testing.T) {
	t.Parallel()

	var (
		mu          sync.Mutex
		queueBodies = make(map[string]map[string]any)
	)
	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodPut && strings.HasPrefix(req.URL.EscapedPath(), "/api/queues/%2F/") {
				defer req.Body.Close()
				payload := make(map[string]any)
				if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
					return jsonResponse(http.StatusBadRequest, map[string]any{"error": err.Error()}), nil
				}
				mu.Lock()
				queueBodies[req.URL.EscapedPath()] = payload
				mu.Unlock()
			}
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
		},
	})
	if err != nil {
		t.Fatalf("EnsureTopology() unexpected error: %v", err)
	}

	testCases := []struct {
		path             string
		wantMessageTTL   float64
		wantDLX          string
		wantDLRoutingKey string
	}{
		{
			path: "/api/queues/%2F/gh.backfill.jobs",
		},
		{
			path: "/api/queues/%2F/gh.backfill.dlq",
		},
		{
			path:             "/api/queues/%2F/gh.backfill.jobs.retry.1m0s",
			wantMessageTTL:   60000,
			wantDLX:          "gh.backfill",
			wantDLRoutingKey: "gh.backfill.jobs",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.path, func(t *testing.T) {
			t.Parallel()

			mu.Lock()
			body, ok := queueBodies[tc.path]
			mu.Unlock()
			if !ok {
				t.Fatalf("missing queue declaration body for %s", tc.path)
			}

			arguments, ok := body["arguments"].(map[string]any)
			if !ok {
				t.Fatalf("arguments type = %T, want map[string]any", body["arguments"])
			}

			if tc.wantMessageTTL == 0 {
				if len(arguments) != 0 {
					t.Fatalf("arguments len = %d, want 0", len(arguments))
				}
				return
			}

			if got := arguments["x-message-ttl"]; got != tc.wantMessageTTL {
				t.Fatalf("x-message-ttl = %v, want %v", got, tc.wantMessageTTL)
			}
			if got := arguments["x-dead-letter-exchange"]; got != tc.wantDLX {
				t.Fatalf("x-dead-letter-exchange = %v, want %v", got, tc.wantDLX)
			}
			if got := arguments["x-dead-letter-routing-key"]; got != tc.wantDLRoutingKey {
				t.Fatalf("x-dead-letter-routing-key = %v, want %v", got, tc.wantDLRoutingKey)
			}
		})
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

func TestRabbitMQHTTPBrokerOldestAge(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.February, 19, 12, 0, 0, 0, time.UTC)

	testCases := []struct {
		name       string
		headValue  string
		want       time.Duration
		statusCode int
	}{
		{
			name:      "rfc3339_timestamp",
			headValue: "2026-02-19T11:59:00Z",
			want:      time.Minute,
		},
		{
			name:      "unix_timestamp",
			headValue: "1739966340",
			want:      now.Sub(time.Unix(1739966340, 0).UTC()),
		},
		{
			name:      "future_timestamp_returns_zero",
			headValue: "2099-01-01T00:00:00Z",
			want:      0,
		},
		{
			name:       "queue_info_failure_returns_zero",
			headValue:  "2026-02-19T11:59:00Z",
			want:       0,
			statusCode: http.StatusServiceUnavailable,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			statusCode := tc.statusCode
			if statusCode == 0 {
				statusCode = http.StatusOK
			}
			client := &http.Client{
				Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					if req.Method != http.MethodGet {
						return jsonResponse(http.StatusMethodNotAllowed, map[string]any{"error": "method"}), nil
					}
					if statusCode != http.StatusOK {
						return jsonResponse(statusCode, map[string]any{"error": "unavailable"}), nil
					}
					if tc.headValue == "1739966340" {
						return jsonResponse(http.StatusOK, map[string]any{"head_message_timestamp": float64(1739966340)}), nil
					}
					return jsonResponse(http.StatusOK, map[string]any{"head_message_timestamp": tc.headValue}), nil
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

			got := broker.OldestAge("gh.backfill.jobs", now)
			if got != tc.want {
				t.Fatalf("OldestAge() = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestParseHeadMessageTimestamp(t *testing.T) {
	t.Parallel()

	want := time.Date(2026, time.February, 19, 12, 0, 0, 0, time.UTC)
	testCases := []struct {
		name   string
		raw    any
		want   time.Time
		isZero bool
	}{
		{
			name: "rfc3339nano",
			raw:  "2026-02-19T12:00:00.000000000Z",
			want: want,
		},
		{
			name: "rfc3339",
			raw:  "2026-02-19T12:00:00Z",
			want: want,
		},
		{
			name: "float_seconds",
			raw:  float64(1771502400),
			want: time.Unix(1771502400, 0).UTC(),
		},
		{
			name:   "invalid_string",
			raw:    "not-a-time",
			isZero: true,
		},
		{
			name:   "zero_seconds",
			raw:    float64(0),
			isZero: true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := parseHeadMessageTimestamp(tc.raw)
			if tc.isZero {
				if !got.IsZero() {
					t.Fatalf("parseHeadMessageTimestamp(%v) = %s, want zero", tc.raw, got)
				}
				return
			}
			if !got.Equal(tc.want) {
				t.Fatalf("parseHeadMessageTimestamp(%v) = %s, want %s", tc.raw, got, tc.want)
			}
		})
	}
}

func TestRabbitMQHTTPBrokerDoJSONErrorPaths(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		endpoint     string
		body         any
		decodeTarget any
		client       *http.Client
		wantErr      bool
		errContains  string
	}{
		{
			name:         "marshal_error",
			endpoint:     "http://rabbitmq.local/api/path",
			body:         map[string]any{"bad": func() {}},
			decodeTarget: nil,
			client:       &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, nil })},
			wantErr:      true,
			errContains:  "marshal rabbitmq request body",
		},
		{
			name:         "invalid_endpoint",
			endpoint:     "://bad-endpoint",
			body:         nil,
			decodeTarget: nil,
			client:       &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, nil })},
			wantErr:      true,
			errContains:  "create rabbitmq request",
		},
		{
			name:         "execute_error",
			endpoint:     "http://rabbitmq.local/api/path",
			body:         nil,
			decodeTarget: nil,
			client: &http.Client{
				Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					return nil, io.ErrUnexpectedEOF
				}),
			},
			wantErr:     true,
			errContains: "execute rabbitmq request",
		},
		{
			name:         "non_2xx_body_read_error",
			endpoint:     "http://rabbitmq.local/api/path",
			body:         nil,
			decodeTarget: nil,
			client: &http.Client{
				Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusBadGateway,
						Body:       &failingReadCloser{},
						Header:     make(http.Header),
					}, nil
				}),
			},
			wantErr:     true,
			errContains: "body-read-error",
		},
		{
			name:         "non_2xx_with_body",
			endpoint:     "http://rabbitmq.local/api/path",
			body:         nil,
			decodeTarget: nil,
			client: &http.Client{
				Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusBadRequest,
						Body:       io.NopCloser(strings.NewReader("invalid request")),
						Header:     make(http.Header),
					}, nil
				}),
			},
			wantErr:     true,
			errContains: "status=400 body=invalid request",
		},
		{
			name:         "decode_error",
			endpoint:     "http://rabbitmq.local/api/path",
			body:         nil,
			decodeTarget: &map[string]any{},
			client: &http.Client{
				Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       io.NopCloser(strings.NewReader("{")),
						Header:     make(http.Header),
					}, nil
				}),
			},
			wantErr:     true,
			errContains: "decode rabbitmq response",
		},
		{
			name:         "success",
			endpoint:     "http://rabbitmq.local/api/path",
			body:         map[string]any{"queue": "gh.backfill.jobs"},
			decodeTarget: &map[string]any{},
			client: &http.Client{
				Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
						Header:     make(http.Header),
					}, nil
				}),
			},
			wantErr: false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			broker, err := NewRabbitMQHTTPBroker(RabbitMQHTTPConfig{
				ManagementURL: "http://rabbitmq.local",
				VHost:         "/",
				Exchange:      "gh.backfill",
				HTTPClient:    tc.client,
			})
			if err != nil {
				t.Fatalf("NewRabbitMQHTTPBroker() unexpected error: %v", err)
			}

			err = broker.doJSON(context.Background(), http.MethodPost, tc.endpoint, tc.body, tc.decodeTarget)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("doJSON() expected error, got nil")
				}
				if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
					t.Fatalf("doJSON() error = %q, missing %q", err.Error(), tc.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("doJSON() unexpected error: %v", err)
			}
		})
	}
}

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

type failingReadCloser struct{}

func (f *failingReadCloser) Read([]byte) (int, error) {
	return 0, io.ErrUnexpectedEOF
}

func (f *failingReadCloser) Close() error {
	return nil
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
