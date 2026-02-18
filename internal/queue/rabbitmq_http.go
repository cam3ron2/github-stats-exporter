package queue

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
)

// RabbitMQHTTPConfig configures the RabbitMQ management API broker.
type RabbitMQHTTPConfig struct {
	ManagementURL string
	VHost         string
	Exchange      string
	Username      string
	//nolint:gosec // Password is required for RabbitMQ basic auth.
	Password     string
	PollInterval time.Duration
	HTTPClient   *http.Client
	Now          func() time.Time
	Sleep        func(time.Duration)
}

// RabbitMQHTTPBroker uses RabbitMQ management endpoints for queue operations.
type RabbitMQHTTPBroker struct {
	baseURL      string
	vhostEncoded string
	exchange     string
	username     string
	password     string
	pollInterval time.Duration
	httpClient   *http.Client
	now          func() time.Time
	sleep        func(time.Duration)
}

// RabbitMQHTTPConfigFromAMQPURL derives management API config from an AMQP connection URL.
func RabbitMQHTTPConfigFromAMQPURL(amqpURL, exchange string) (RabbitMQHTTPConfig, error) {
	parsedURL, err := url.Parse(strings.TrimSpace(amqpURL))
	if err != nil {
		return RabbitMQHTTPConfig{}, fmt.Errorf("parse amqp url: %w", err)
	}
	if parsedURL.Host == "" {
		return RabbitMQHTTPConfig{}, fmt.Errorf("amqp url host is required")
	}

	scheme := "http"
	if strings.EqualFold(parsedURL.Scheme, "amqps") {
		scheme = "https"
	}
	hostName := parsedURL.Hostname()
	port := parsedURL.Port()
	if port == "" || port == "5672" || port == "5671" {
		port = "15672"
	}

	vhost := strings.Trim(path.Clean(parsedURL.Path), "/")
	if vhost == "." || vhost == "" {
		vhost = "/"
	}

	username := ""
	password := ""
	if parsedURL.User != nil {
		username = parsedURL.User.Username()
		password, _ = parsedURL.User.Password()
	}

	return RabbitMQHTTPConfig{
		ManagementURL: fmt.Sprintf("%s://%s:%s", scheme, hostName, port),
		VHost:         vhost,
		Exchange:      exchange,
		Username:      username,
		Password:      password,
	}, nil
}

// NewRabbitMQHTTPBroker builds a RabbitMQ management API broker.
func NewRabbitMQHTTPBroker(cfg RabbitMQHTTPConfig) (*RabbitMQHTTPBroker, error) {
	baseURL := strings.TrimSpace(cfg.ManagementURL)
	if baseURL == "" {
		return nil, fmt.Errorf("rabbitmq management url is required")
	}
	if _, err := url.Parse(baseURL); err != nil {
		return nil, fmt.Errorf("parse rabbitmq management url: %w", err)
	}

	exchange := strings.TrimSpace(cfg.Exchange)
	if exchange == "" {
		return nil, fmt.Errorf("rabbitmq exchange is required")
	}

	vhost := strings.TrimSpace(cfg.VHost)
	if vhost == "" {
		vhost = "/"
	}

	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}

	pollInterval := cfg.PollInterval
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}

	nowFn := cfg.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	sleepFn := cfg.Sleep
	if sleepFn == nil {
		sleepFn = time.Sleep
	}

	return &RabbitMQHTTPBroker{
		baseURL:      strings.TrimRight(baseURL, "/"),
		vhostEncoded: url.PathEscape(vhost),
		exchange:     exchange,
		username:     cfg.Username,
		password:     cfg.Password,
		pollInterval: pollInterval,
		httpClient:   client,
		now:          nowFn,
		sleep:        sleepFn,
	}, nil
}

// Publish writes one message to the named queue using the configured exchange.
func (b *RabbitMQHTTPBroker) Publish(ctx context.Context, queueName string, msg Message) error {
	if b == nil {
		return fmt.Errorf("rabbitmq broker is nil")
	}
	if strings.TrimSpace(queueName) == "" {
		return fmt.Errorf("queue name is required")
	}

	headers := maps.Clone(msg.Headers)
	if headers == nil {
		headers = make(map[string]string)
	}
	if msg.ID != "" {
		headers["id"] = msg.ID
	}
	if msg.Attempt > 0 {
		headers["attempt"] = strconv.Itoa(msg.Attempt)
	}
	if !msg.CreatedAt.IsZero() {
		headers["created_at"] = msg.CreatedAt.UTC().Format(time.RFC3339Nano)
	}

	body := map[string]any{
		"properties": map[string]any{
			"headers": headers,
		},
		"routing_key":      queueName,
		"payload":          base64.StdEncoding.EncodeToString(msg.Body),
		"payload_encoding": "base64",
	}

	endpoint := fmt.Sprintf("%s/api/exchanges/%s/%s/publish", b.baseURL, b.vhostEncoded, url.PathEscape(b.exchange))
	var response struct {
		Routed bool `json:"routed"`
	}
	if err := b.doJSON(ctx, http.MethodPost, endpoint, body, &response); err != nil {
		return err
	}
	if !response.Routed {
		return fmt.Errorf("rabbitmq publish was not routed to queue %q", queueName)
	}
	return nil
}

// Consume reads messages from the named queue until context cancellation.
func (b *RabbitMQHTTPBroker) Consume(ctx context.Context, queueName string, cfg ConsumerConfig, handler Handler) {
	if b == nil || handler == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	nowFn := cfg.Now
	if nowFn == nil {
		nowFn = b.now
	}
	sleepFn := cfg.Sleep
	if sleepFn == nil {
		sleepFn = b.sleep
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, ok := b.popMessage(ctx, queueName)
		if !ok {
			sleepFn(b.pollInterval)
			continue
		}

		if ShouldDropMessageByAge(msg, nowFn(), cfg.MaxMessageAge) {
			continue
		}
		if msg.Attempt <= 0 {
			msg.Attempt = 1
		}

		err := handler(ctx, cloneMessage(msg))
		if err == nil {
			continue
		}

		delay, retry := cfg.RetryPolicy.NextDelay(msg.Attempt)
		if retry {
			retryMessage := cloneMessage(msg)
			retryMessage.Attempt++
			sleepFn(delay)
			if publishErr := b.Publish(ctx, queueName, retryMessage); publishErr != nil {
				continue
			}
			continue
		}

		if cfg.DeadLetterQueue != "" {
			dlqMessage := cloneMessage(msg)
			if dlqMessage.Headers == nil {
				dlqMessage.Headers = make(map[string]string)
			}
			dlqMessage.Headers["last_error"] = err.Error()
			if publishErr := b.Publish(ctx, cfg.DeadLetterQueue, dlqMessage); publishErr != nil {
				continue
			}
		}
	}
}

// Depth returns queued item count for one queue.
func (b *RabbitMQHTTPBroker) Depth(queueName string) int {
	if b == nil || strings.TrimSpace(queueName) == "" {
		return 0
	}

	endpoint := fmt.Sprintf("%s/api/queues/%s/%s", b.baseURL, b.vhostEncoded, url.PathEscape(queueName))
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, endpoint, nil)
	if err != nil {
		return 0
	}
	if b.username != "" || b.password != "" {
		req.SetBasicAuth(b.username, b.password)
	}

	//nolint:gosec // Endpoint is validated during broker construction.
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return 0
	}

	var queueData struct {
		Messages int `json:"messages"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&queueData); err != nil {
		return 0
	}
	return queueData.Messages
}

func (b *RabbitMQHTTPBroker) popMessage(ctx context.Context, queueName string) (Message, bool) {
	endpoint := fmt.Sprintf("%s/api/queues/%s/%s/get", b.baseURL, b.vhostEncoded, url.PathEscape(queueName))
	body := map[string]any{
		"count":    1,
		"ackmode":  "ack_requeue_false",
		"encoding": "base64",
		"truncate": 500000,
	}

	var payload []rabbitGetMessage
	if err := b.doJSON(ctx, http.MethodPost, endpoint, body, &payload); err != nil {
		return Message{}, false
	}
	if len(payload) == 0 {
		return Message{}, false
	}

	return decodeRabbitMessage(payload[0]), true
}

func decodeRabbitMessage(payload rabbitGetMessage) Message {
	msg := Message{
		Headers: make(map[string]string),
	}

	if payload.PayloadEncoding == "base64" {
		decoded, err := base64.StdEncoding.DecodeString(payload.Payload)
		if err == nil {
			msg.Body = decoded
		}
	} else {
		msg.Body = []byte(payload.Payload)
	}

	for key, value := range payload.Properties.Headers {
		msg.Headers[key] = fmt.Sprint(value)
	}

	msg.ID = strings.TrimSpace(msg.Headers["id"])
	if parsedAttempt, err := strconv.Atoi(msg.Headers["attempt"]); err == nil {
		msg.Attempt = parsedAttempt
	}
	if createdAt := strings.TrimSpace(msg.Headers["created_at"]); createdAt != "" {
		if parsedCreatedAt, err := time.Parse(time.RFC3339Nano, createdAt); err == nil {
			msg.CreatedAt = parsedCreatedAt.UTC()
		}
	}
	if msg.CreatedAt.IsZero() && payload.Properties.Timestamp > 0 {
		msg.CreatedAt = time.Unix(payload.Properties.Timestamp, 0).UTC()
	}

	return msg
}

func (b *RabbitMQHTTPBroker) doJSON(ctx context.Context, method, endpoint string, body any, decodeTarget any) error {
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal rabbitmq request body: %w", err)
		}
		reader = bytes.NewReader(payload)
	}

	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return fmt.Errorf("create rabbitmq request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if b.username != "" || b.password != "" {
		req.SetBasicAuth(b.username, b.password)
	}

	//nolint:gosec // Endpoint is validated during broker construction.
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute rabbitmq request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		bodyBytes, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if readErr != nil {
			return fmt.Errorf("rabbitmq request failed: status=%d body-read-error=%v", resp.StatusCode, readErr)
		}
		return fmt.Errorf("rabbitmq request failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
	}

	if decodeTarget == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(decodeTarget); err != nil {
		return fmt.Errorf("decode rabbitmq response: %w", err)
	}
	return nil
}

type rabbitGetMessage struct {
	Payload         string `json:"payload"`
	PayloadEncoding string `json:"payload_encoding"`
	Properties      struct {
		Headers   map[string]any `json:"headers"`
		Timestamp int64          `json:"timestamp"`
	} `json:"properties"`
}
