# internal/backfill

## Logic overview

The `backfill` package provides queue primitives and enqueue policy for missed scrape windows.

- `Dispatcher` coalesces time windows, deduplicates jobs, and enforces per-org enqueue caps.
- `InMemoryQueue` is a local/test queue implementation with max-age dropping during consumption.
- Message age checks prevent stale retries from running indefinitely.

## API reference

### Types

- `QueuePublisher`: queue publish interface used by `Dispatcher`.
- `Deduper`: lock acquisition interface used for dedup suppression.
- `Config`: dispatcher settings (`CoalesceWindow`, `DedupTTL`, `MaxEnqueuesPerOrgPerMinute`).
- `Message`: queue payload schema for one backfill job.
- `MessageInput`: enqueue request payload for missed data windows.
- `EnqueueResult`: enqueue decision outcome (`Published`, `DedupSuppressed`, `DroppedByRateLimit`).
- `Dispatcher`: policy engine for backfill enqueue operations.
- `InMemoryQueue`: buffered channel queue for local development and tests.

### Functions

- `NewDispatcher(config Config, queue QueuePublisher, deduper Deduper) *Dispatcher`: constructs dispatcher.
- `ShouldDropMessageByAge(msg Message, now time.Time, maxAge time.Duration) bool`: returns `true` when a message has exceeded max age.
- `NewInMemoryQueue(buffer int) *InMemoryQueue`: constructs in-memory queue.

### Methods

- `(*Dispatcher) EnqueueMissing(input MessageInput) EnqueueResult`: coalesces/dedups/rate-limits and publishes backfill jobs.
- `(*InMemoryQueue) Publish(msg Message) error`: enqueues one message.
- `(*InMemoryQueue) Consume(ctx context.Context, handler func(Message) error, maxMessageAge time.Duration, nowFn func() time.Time)`: consumes until context cancellation and drops expired messages.
- `(*InMemoryQueue) Depth() int`: returns queued message count.
