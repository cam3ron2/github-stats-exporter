package queue

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"sync"
	"time"
)

// Message is a queue payload with retry metadata.
type Message struct {
	ID        string
	Body      []byte
	Headers   map[string]string
	CreatedAt time.Time
	Attempt   int
}

// RetryPolicy controls consumer retry behavior.
type RetryPolicy struct {
	MaxAttempts int
	Delays      []time.Duration
}

// NextDelay returns the retry delay for the current attempt.
func (p RetryPolicy) NextDelay(attempt int) (time.Duration, bool) {
	if len(p.Delays) == 0 {
		return 0, false
	}
	if attempt < 1 {
		attempt = 1
	}
	if p.MaxAttempts > 0 && attempt >= p.MaxAttempts {
		return 0, false
	}

	idx := attempt - 1
	if idx >= len(p.Delays) {
		idx = len(p.Delays) - 1
	}
	return p.Delays[idx], true
}

// ConsumerConfig controls in-memory consumer behavior.
type ConsumerConfig struct {
	MaxMessageAge   time.Duration
	RetryPolicy     RetryPolicy
	RetryQueues     []string
	DeadLetterQueue string
	Now             func() time.Time
	Sleep           func(time.Duration)
}

// Handler processes one queue message.
type Handler func(ctx context.Context, msg Message) error

// InMemoryBroker is a simple named-queue broker for tests and local development.
type InMemoryBroker struct {
	mu     sync.RWMutex
	buffer int
	queues map[string]chan Message
	times  map[string][]time.Time
}

// NewInMemoryBroker creates an in-memory broker.
func NewInMemoryBroker(buffer int) *InMemoryBroker {
	if buffer <= 0 {
		buffer = 1
	}
	return &InMemoryBroker{
		buffer: buffer,
		queues: make(map[string]chan Message),
		times:  make(map[string][]time.Time),
	}
}

// Publish writes one message to the named queue.
func (b *InMemoryBroker) Publish(_ context.Context, queue string, msg Message) error {
	if b == nil {
		return fmt.Errorf("queue broker is nil")
	}
	if queue == "" {
		return fmt.Errorf("queue name is required")
	}

	queueChan := b.ensureQueue(queue)
	message := cloneMessage(msg)
	b.enqueueTimestamp(queue, message.CreatedAt)
	select {
	case queueChan <- message:
		return nil
	default:
		return fmt.Errorf("queue buffer full")
	}
}

// Consume processes messages from the named queue until context cancellation.
func (b *InMemoryBroker) Consume(ctx context.Context, queue string, cfg ConsumerConfig, handler Handler) {
	if b == nil || handler == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	nowFn := cfg.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	sleepFn := cfg.Sleep
	if sleepFn == nil {
		sleepFn = time.Sleep
	}

	queueChan := b.ensureQueue(queue)
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-queueChan:
			b.dequeueTimestamp(queue)
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
				retryQueue := queue
				if configuredQueue, ok := retryQueueForAttempt(cfg.RetryQueues, msg.Attempt); ok {
					retryQueue = configuredQueue
				} else {
					sleepFn(delay)
				}
				if publishErr := b.Publish(ctx, retryQueue, retryMessage); publishErr != nil {
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
}

// OldestAge returns age of the oldest tracked message for the queue.
func (b *InMemoryBroker) OldestAge(queue string, now time.Time) time.Duration {
	if b == nil || queue == "" {
		return 0
	}

	b.mu.RLock()
	defer b.mu.RUnlock()
	timestamps := b.times[queue]
	if len(timestamps) == 0 {
		return 0
	}
	oldest := timestamps[0]
	if oldest.IsZero() || now.Before(oldest) {
		return 0
	}
	return now.Sub(oldest)
}

// Depth returns the queued item count for one queue.
func (b *InMemoryBroker) Depth(queue string) int {
	if b == nil || queue == "" {
		return 0
	}

	b.mu.RLock()
	defer b.mu.RUnlock()
	queueChan, ok := b.queues[queue]
	if !ok {
		return 0
	}
	return len(queueChan)
}

// Health reports broker readiness for the requested queue.
func (b *InMemoryBroker) Health(_ context.Context, queue string) error {
	if b == nil {
		return fmt.Errorf("queue broker is nil")
	}
	if strings.TrimSpace(queue) == "" {
		return fmt.Errorf("queue name is required")
	}
	_ = b.ensureQueue(queue)
	return nil
}

// ShouldDropMessageByAge returns true when message age exceeds max age.
func ShouldDropMessageByAge(msg Message, now time.Time, maxAge time.Duration) bool {
	if msg.CreatedAt.IsZero() || maxAge <= 0 {
		return false
	}
	return now.Sub(msg.CreatedAt) > maxAge
}

func (b *InMemoryBroker) ensureQueue(queue string) chan Message {
	b.mu.RLock()
	queueChan, ok := b.queues[queue]
	b.mu.RUnlock()
	if ok {
		return queueChan
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	queueChan, ok = b.queues[queue]
	if ok {
		return queueChan
	}
	queueChan = make(chan Message, b.buffer)
	b.queues[queue] = queueChan
	return queueChan
}

func cloneMessage(msg Message) Message {
	cloned := msg
	cloned.Headers = maps.Clone(msg.Headers)
	if msg.Body != nil {
		cloned.Body = append([]byte(nil), msg.Body...)
	}
	return cloned
}

func retryQueueForAttempt(queues []string, attempt int) (string, bool) {
	if len(queues) == 0 {
		return "", false
	}
	if attempt < 1 {
		attempt = 1
	}
	index := attempt - 1
	if index >= len(queues) {
		index = len(queues) - 1
	}
	queue := strings.TrimSpace(queues[index])
	if queue == "" {
		return "", false
	}
	return queue, true
}

func (b *InMemoryBroker) enqueueTimestamp(queue string, createdAt time.Time) {
	now := createdAt.UTC()
	if createdAt.IsZero() {
		now = time.Now().UTC()
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.times[queue] = append(b.times[queue], now)
}

func (b *InMemoryBroker) dequeueTimestamp(queue string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	timestamps := b.times[queue]
	if len(timestamps) == 0 {
		return
	}
	if len(timestamps) == 1 {
		delete(b.times, queue)
		return
	}
	b.times[queue] = append([]time.Time{}, timestamps[1:]...)
}
