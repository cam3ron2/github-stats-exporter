package queue

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetryPolicyNextDelay(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		policy    RetryPolicy
		attempt   int
		wantDelay time.Duration
		wantOK    bool
	}{
		{
			name:      "no_delays_disables_retry",
			policy:    RetryPolicy{MaxAttempts: 3},
			attempt:   1,
			wantDelay: 0,
			wantOK:    false,
		},
		{
			name:      "first_attempt_uses_first_delay",
			policy:    RetryPolicy{MaxAttempts: 3, Delays: []time.Duration{time.Second, 2 * time.Second}},
			attempt:   1,
			wantDelay: time.Second,
			wantOK:    true,
		},
		{
			name:      "later_attempt_uses_last_delay_when_exhausted",
			policy:    RetryPolicy{MaxAttempts: 5, Delays: []time.Duration{time.Second, 2 * time.Second}},
			attempt:   4,
			wantDelay: 2 * time.Second,
			wantOK:    true,
		},
		{
			name:      "attempt_at_or_over_max_disables_retry",
			policy:    RetryPolicy{MaxAttempts: 2, Delays: []time.Duration{time.Second}},
			attempt:   2,
			wantDelay: 0,
			wantOK:    false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			delay, ok := tc.policy.NextDelay(tc.attempt)
			if ok != tc.wantOK {
				t.Fatalf("NextDelay().ok = %t, want %t", ok, tc.wantOK)
			}
			if delay != tc.wantDelay {
				t.Fatalf("NextDelay().delay = %s, want %s", delay, tc.wantDelay)
			}
		})
	}
}

func TestInMemoryBrokerPublishAndConsume(t *testing.T) {
	t.Parallel()

	broker := NewInMemoryBroker(8)
	now := time.Unix(1739836800, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := broker.Publish(ctx, "jobs", Message{ID: "m1", CreatedAt: now}); err != nil {
		t.Fatalf("Publish() unexpected error: %v", err)
	}

	processed := make(chan Message, 1)
	go broker.Consume(ctx, "jobs", ConsumerConfig{}, func(_ context.Context, msg Message) error {
		processed <- msg
		cancel()
		return nil
	})

	select {
	case got := <-processed:
		if got.ID != "m1" {
			t.Fatalf("message id = %q, want m1", got.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for message")
	}
}

func TestInMemoryBrokerDropsExpiredMessages(t *testing.T) {
	t.Parallel()

	broker := NewInMemoryBroker(8)
	now := time.Unix(1739836800, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := broker.Publish(ctx, "jobs", Message{ID: "m1", CreatedAt: now.Add(-2 * time.Hour)}); err != nil {
		t.Fatalf("Publish() unexpected error: %v", err)
	}

	var processed atomic.Int32
	go broker.Consume(ctx, "jobs", ConsumerConfig{
		MaxMessageAge: time.Hour,
		Now: func() time.Time {
			return now
		},
	}, func(_ context.Context, _ Message) error {
		processed.Add(1)
		return nil
	})

	time.Sleep(100 * time.Millisecond)
	cancel()
	if got := processed.Load(); got != 0 {
		t.Fatalf("processed = %d, want 0", got)
	}
}

func TestInMemoryBrokerRetriesThenSucceeds(t *testing.T) {
	t.Parallel()

	broker := NewInMemoryBroker(8)
	now := time.Unix(1739836800, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := broker.Publish(ctx, "jobs", Message{ID: "m1", CreatedAt: now, Attempt: 1}); err != nil {
		t.Fatalf("Publish() unexpected error: %v", err)
	}

	attempts := make([]int, 0)
	sleeps := make([]time.Duration, 0)
	var mu sync.Mutex

	go broker.Consume(ctx, "jobs", ConsumerConfig{
		RetryPolicy: RetryPolicy{MaxAttempts: 4, Delays: []time.Duration{time.Millisecond, 2 * time.Millisecond}},
		Sleep: func(delay time.Duration) {
			mu.Lock()
			sleeps = append(sleeps, delay)
			mu.Unlock()
		},
	}, func(_ context.Context, msg Message) error {
		mu.Lock()
		attempts = append(attempts, msg.Attempt)
		mu.Unlock()

		if msg.Attempt < 3 {
			return errors.New("try again")
		}
		cancel()
		return nil
	})

	<-ctx.Done()

	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(attempts, []int{1, 2, 3}) {
		t.Fatalf("attempts = %v, want [1 2 3]", attempts)
	}
	if !slices.Equal(sleeps, []time.Duration{time.Millisecond, 2 * time.Millisecond}) {
		t.Fatalf("sleeps = %v, want [1ms 2ms]", sleeps)
	}
}

func TestInMemoryBrokerMovesToDeadLetterWhenExhausted(t *testing.T) {
	t.Parallel()

	broker := NewInMemoryBroker(8)
	now := time.Unix(1739836800, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := broker.Publish(ctx, "jobs", Message{ID: "m1", CreatedAt: now, Attempt: 1}); err != nil {
		t.Fatalf("Publish() unexpected error: %v", err)
	}

	dlqMessages := make(chan Message, 1)
	go broker.Consume(ctx, "jobs-dlq", ConsumerConfig{}, func(_ context.Context, msg Message) error {
		dlqMessages <- msg
		cancel()
		return nil
	})

	go broker.Consume(ctx, "jobs", ConsumerConfig{
		RetryPolicy:     RetryPolicy{MaxAttempts: 2, Delays: []time.Duration{time.Millisecond}},
		DeadLetterQueue: "jobs-dlq",
		Sleep:           func(time.Duration) {},
	}, func(_ context.Context, _ Message) error {
		return errors.New("always fail")
	})

	select {
	case dlq := <-dlqMessages:
		if dlq.ID != "m1" {
			t.Fatalf("dlq id = %q, want m1", dlq.ID)
		}
		if dlq.Attempt != 2 {
			t.Fatalf("dlq attempt = %d, want 2", dlq.Attempt)
		}
		if dlq.Headers["last_error"] != "always fail" {
			t.Fatalf("last_error = %q, want %q", dlq.Headers["last_error"], "always fail")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for dlq message")
	}
}

func TestInMemoryBrokerPublishFailsWhenBufferFull(t *testing.T) {
	t.Parallel()

	broker := NewInMemoryBroker(1)
	ctx := context.Background()
	now := time.Unix(1739836800, 0)

	if err := broker.Publish(ctx, "jobs", Message{ID: "m1", CreatedAt: now}); err != nil {
		t.Fatalf("Publish(first) unexpected error: %v", err)
	}
	if err := broker.Publish(ctx, "jobs", Message{ID: "m2", CreatedAt: now}); err == nil {
		t.Fatalf("Publish(second) expected buffer full error, got nil")
	}
}
