package backfill

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestInMemoryQueuePublishConsume(t *testing.T) {
	t.Parallel()

	queue := NewInMemoryQueue(10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var processed int32
	done := make(chan struct{})

	go func() {
		queue.Consume(ctx, func(msg Message) error {
			if msg.Org == "org-a" {
				atomic.AddInt32(&processed, 1)
				close(done)
			}
			return nil
		}, 24*time.Hour, time.Now)
	}()

	err := queue.Publish(Message{
		Org:       "org-a",
		CreatedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("Publish() unexpected error: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for message processing")
	}

	if atomic.LoadInt32(&processed) != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}
}

func TestInMemoryQueueDropsExpiredMessages(t *testing.T) {
	t.Parallel()

	queue := NewInMemoryQueue(10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	now := time.Unix(1739836800, 0)
	err := queue.Publish(Message{
		Org:       "org-a",
		CreatedAt: now.Add(-25 * time.Hour),
	})
	if err != nil {
		t.Fatalf("Publish() unexpected error: %v", err)
	}

	var processed int32
	done := make(chan struct{})
	go func() {
		queue.Consume(ctx, func(_ Message) error {
			atomic.AddInt32(&processed, 1)
			return nil
		}, 24*time.Hour, func() time.Time { return now })
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	if atomic.LoadInt32(&processed) != 0 {
		t.Fatalf("processed = %d, want 0", processed)
	}
}
