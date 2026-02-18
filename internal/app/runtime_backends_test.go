package app

import (
	"context"
	"testing"
	"time"

	"github.com/cam3ron2/github-stats/internal/backfill"
	"github.com/cam3ron2/github-stats/internal/config"
	queuepkg "github.com/cam3ron2/github-stats/internal/queue"
	"github.com/cam3ron2/github-stats/internal/store"
	"go.uber.org/zap"
)

func TestNewRuntimeBackendsFallbacks(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		cfg  *config.Config
	}{
		{
			name: "nil_config_defaults",
			cfg:  nil,
		},
		{
			name: "invalid_redis_and_amqp_falls_back",
			cfg: &config.Config{
				Store: config.StoreConfig{
					Backend:   "redis",
					RedisMode: "standalone",
					RedisAddr: "127.0.0.1:1",
				},
				AMQP: config.AMQPConfig{
					URL:      "://invalid",
					Exchange: "gh.backfill",
					Queue:    "gh.backfill.jobs",
				},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			storeBackend, queueBackend := newRuntimeBackends(tc.cfg, zap.NewNop(), time.Hour, 1000)
			if storeBackend == nil {
				t.Fatalf("store backend is nil")
			}
			if queueBackend == nil {
				t.Fatalf("queue backend is nil")
			}
			if _, ok := storeBackend.(*store.MemoryStore); !ok {
				t.Fatalf("store backend type = %T, want *store.MemoryStore", storeBackend)
			}
			if queueBackend.Depth() < 0 {
				t.Fatalf("queue depth should be non-negative")
			}
		})
	}
}

func TestBrokerBackfillQueuePublishAndConsume(t *testing.T) {
	t.Parallel()

	broker := queuepkg.NewInMemoryBroker(8)
	queue := &brokerBackfillQueue{
		broker:    broker,
		queueName: "gh.backfill.jobs",
		retryPolicy: queuepkg.RetryPolicy{
			MaxAttempts: 3,
			Delays:      []time.Duration{0},
		},
	}

	now := time.Unix(1739836800, 0).UTC()
	message := backfill.Message{
		JobID:       "job-1",
		Org:         "org-a",
		Repo:        "repo-a",
		Reason:      "scrape_error",
		WindowStart: now.Add(-time.Hour),
		WindowEnd:   now,
		Attempt:     1,
		MaxAttempts: 7,
		CreatedAt:   now,
	}
	if err := queue.Publish(message); err != nil {
		t.Fatalf("Publish() unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumed := make(chan backfill.Message, 1)
	go queue.Consume(ctx, func(msg backfill.Message) error {
		consumed <- msg
		cancel()
		return nil
	}, 24*time.Hour, func() time.Time { return now })

	select {
	case got := <-consumed:
		if got.JobID != "job-1" {
			t.Fatalf("JobID = %q, want job-1", got.JobID)
		}
		if got.Org != "org-a" || got.Repo != "repo-a" {
			t.Fatalf("unexpected message identity: org=%q repo=%q", got.Org, got.Repo)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for consumed message")
	}
}

func TestBrokerBackfillQueueDecodeErrorMovesToDLQ(t *testing.T) {
	t.Parallel()

	broker := queuepkg.NewInMemoryBroker(8)
	queue := &brokerBackfillQueue{
		broker:    broker,
		queueName: "gh.backfill.jobs",
		dlqName:   "gh.backfill.dlq",
		retryPolicy: queuepkg.RetryPolicy{
			MaxAttempts: 1,
			Delays:      []time.Duration{0},
		},
	}

	now := time.Unix(1739836800, 0).UTC()
	err := broker.Publish(context.Background(), "gh.backfill.jobs", queuepkg.Message{
		ID:        "job-1",
		Body:      []byte("{"),
		CreatedAt: now,
		Attempt:   1,
	})
	if err != nil {
		t.Fatalf("preload publish unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go queue.Consume(ctx, func(backfill.Message) error {
		t.Fatalf("handler should not be called for decode errors")
		return nil
	}, 24*time.Hour, func() time.Time { return now })

	dlqMessages := make(chan queuepkg.Message, 1)
	go broker.Consume(ctx, "gh.backfill.dlq", queuepkg.ConsumerConfig{}, func(_ context.Context, msg queuepkg.Message) error {
		dlqMessages <- msg
		cancel()
		return nil
	})

	select {
	case msg := <-dlqMessages:
		if string(msg.Body) != "{" {
			t.Fatalf("dlq payload = %q, want \"{\"", string(msg.Body))
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for dlq payload")
	}
}

func TestNewRedisStoreFromConfig(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		cfg       *config.Config
		wantError bool
	}{
		{
			name:      "nil_config",
			cfg:       nil,
			wantError: true,
		},
		{
			name: "ping_failure",
			cfg: &config.Config{
				Store: config.StoreConfig{
					RedisMode: "standalone",
					RedisAddr: "127.0.0.1:1",
				},
			},
			wantError: true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := newRedisStoreFromConfig(tc.cfg, time.Hour, 1000)
			if tc.wantError {
				if err == nil {
					t.Fatalf("newRedisStoreFromConfig() expected error, got nil")
				}
				if got != nil {
					t.Fatalf("newRedisStoreFromConfig() expected nil store on error")
				}
				return
			}
			if err != nil {
				t.Fatalf("newRedisStoreFromConfig() unexpected error: %v", err)
			}
			if got == nil {
				t.Fatalf("newRedisStoreFromConfig() returned nil store")
			}
		})
	}
}

func TestBuildRetryQueueNamesAndSpecs(t *testing.T) {
	t.Parallel()

	delays := []time.Duration{time.Minute, 5 * time.Minute}
	names := buildRetryQueueNames("gh.backfill.jobs", delays)
	if len(names) != 2 {
		t.Fatalf("buildRetryQueueNames() len = %d, want 2", len(names))
	}
	if names[0] != "gh.backfill.jobs.retry.1m0s" {
		t.Fatalf("first retry queue = %q, want gh.backfill.jobs.retry.1m0s", names[0])
	}
	if names[1] != "gh.backfill.jobs.retry.5m0s" {
		t.Fatalf("second retry queue = %q, want gh.backfill.jobs.retry.5m0s", names[1])
	}

	specs := buildRetryQueueSpecs(names, delays)
	if len(specs) != 2 {
		t.Fatalf("buildRetryQueueSpecs() len = %d, want 2", len(specs))
	}
	if specs[0].Name != names[0] || specs[0].Delay != time.Minute {
		t.Fatalf("first retry queue spec = %#v, want name=%q delay=1m", specs[0], names[0])
	}
	if specs[1].Name != names[1] || specs[1].Delay != 5*time.Minute {
		t.Fatalf("second retry queue spec = %#v, want name=%q delay=5m", specs[1], names[1])
	}
}
