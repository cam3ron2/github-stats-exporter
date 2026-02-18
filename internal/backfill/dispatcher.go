package backfill

import (
	"fmt"
	"sync"
	"time"
)

// QueuePublisher publishes backfill jobs.
type QueuePublisher interface {
	Publish(msg Message) error
}

// Deduper acquires dedup locks for messages.
type Deduper interface {
	Acquire(key string, ttl time.Duration, now time.Time) bool
}

// Config controls dispatcher behavior.
type Config struct {
	CoalesceWindow             time.Duration
	DedupTTL                   time.Duration
	MaxEnqueuesPerOrgPerMinute int
}

// Message is a backfill queue payload.
type Message struct {
	JobID       string    `json:"job_id"`
	DedupKey    string    `json:"dedup_key"`
	Org         string    `json:"org"`
	Repo        string    `json:"repo"`
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	Reason      string    `json:"reason"`
	Attempt     int       `json:"attempt"`
	MaxAttempts int       `json:"max_attempts"`
	CreatedAt   time.Time `json:"created_at"`
}

// MessageInput is the enqueue input for a missed window.
type MessageInput struct {
	Org         string
	Repo        string
	WindowStart time.Time
	WindowEnd   time.Time
	Reason      string
	Now         time.Time
}

// EnqueueResult contains enqueue outcomes for observability.
type EnqueueResult struct {
	Published          bool
	DedupSuppressed    bool
	DroppedByRateLimit bool
}

// Dispatcher coalesces, deduplicates, and rate-limits backfill enqueueing.
type Dispatcher struct {
	mu           sync.Mutex
	config       Config
	queue        QueuePublisher
	deduper      Deduper
	perOrgMinute map[string]int
}

// NewDispatcher creates a dispatcher.
func NewDispatcher(config Config, queue QueuePublisher, deduper Deduper) *Dispatcher {
	return &Dispatcher{
		config:       config,
		queue:        queue,
		deduper:      deduper,
		perOrgMinute: make(map[string]int),
	}
}

// EnqueueMissing enqueues a missed-window message if not deduped and not rate-limited.
func (d *Dispatcher) EnqueueMissing(input MessageInput) EnqueueResult {
	windowStart, windowEnd := coalesceWindow(input.WindowStart, input.Now, d.config.CoalesceWindow)
	if !input.WindowEnd.IsZero() {
		_, explicitEnd := coalesceWindow(input.WindowEnd, input.Now, d.config.CoalesceWindow)
		if explicitEnd.After(windowEnd) {
			windowEnd = explicitEnd
		}
	}

	dedupKey := fmt.Sprintf("%s:%s:%s:%s", input.Org, input.Repo, windowStart.UTC().Format(time.RFC3339), windowEnd.UTC().Format(time.RFC3339))

	if !d.deduper.Acquire(dedupKey, d.config.DedupTTL, input.Now) {
		return EnqueueResult{
			Published:       false,
			DedupSuppressed: true,
		}
	}

	minuteKey := fmt.Sprintf("%s:%d", input.Org, input.Now.Unix()/60)
	d.mu.Lock()
	count := d.perOrgMinute[minuteKey]
	if d.config.MaxEnqueuesPerOrgPerMinute > 0 && count >= d.config.MaxEnqueuesPerOrgPerMinute {
		d.mu.Unlock()
		return EnqueueResult{
			Published:          false,
			DroppedByRateLimit: true,
		}
	}
	d.perOrgMinute[minuteKey] = count + 1
	d.mu.Unlock()

	msg := Message{
		JobID:       dedupKey + ":" + strconvSafeUnixNano(input.Now),
		DedupKey:    dedupKey,
		Org:         input.Org,
		Repo:        input.Repo,
		WindowStart: windowStart,
		WindowEnd:   windowEnd,
		Reason:      input.Reason,
		Attempt:     1,
		MaxAttempts: 7,
		CreatedAt:   input.Now,
	}
	if err := d.queue.Publish(msg); err != nil {
		return EnqueueResult{Published: false}
	}
	return EnqueueResult{Published: true}
}

// ShouldDropMessageByAge returns true when a message exceeds max age.
func ShouldDropMessageByAge(msg Message, now time.Time, maxAge time.Duration) bool {
	if msg.CreatedAt.IsZero() || maxAge <= 0 {
		return false
	}
	return now.Sub(msg.CreatedAt) > maxAge
}

func coalesceWindow(windowStart, now time.Time, coalesceWindow time.Duration) (time.Time, time.Time) {
	if coalesceWindow <= 0 {
		if windowStart.IsZero() {
			return now, now
		}
		return windowStart, windowStart
	}
	if windowStart.IsZero() {
		windowStart = now
	}
	coalescedStart := windowStart.Truncate(coalesceWindow)
	return coalescedStart, coalescedStart.Add(coalesceWindow)
}

func strconvSafeUnixNano(now time.Time) string {
	return fmt.Sprintf("%d", now.UnixNano())
}
