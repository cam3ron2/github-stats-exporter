package backfill

import (
	"context"
	"fmt"
	"time"
)

// InMemoryQueue is an in-process queue for local development and tests.
type InMemoryQueue struct {
	ch chan Message
}

// NewInMemoryQueue creates an in-memory queue.
func NewInMemoryQueue(buffer int) *InMemoryQueue {
	if buffer <= 0 {
		buffer = 1
	}
	return &InMemoryQueue{
		ch: make(chan Message, buffer),
	}
}

// Publish enqueues a message.
func (q *InMemoryQueue) Publish(msg Message) error {
	select {
	case q.ch <- msg:
		return nil
	default:
		return fmt.Errorf("queue buffer full")
	}
}

// Consume consumes messages until context cancellation.
func (q *InMemoryQueue) Consume(
	ctx context.Context,
	handler func(Message) error,
	maxMessageAge time.Duration,
	nowFn func() time.Time,
) {
	if nowFn == nil {
		nowFn = time.Now
	}
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-q.ch:
			if ShouldDropMessageByAge(msg, nowFn(), maxMessageAge) {
				continue
			}
			_ = handler(msg)
		}
	}
}

// Depth returns the number of queued messages.
func (q *InMemoryQueue) Depth() int {
	return len(q.ch)
}
