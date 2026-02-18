# internal/queue

## Logic overview

The `queue` package provides queue abstractions with both in-memory and RabbitMQ-backed implementations.

- Messages carry retry metadata (`Attempt`, `CreatedAt`, headers).
- `RetryPolicy` maps current attempt to the next delay and enforces maximum attempts.
- `InMemoryBroker` supports named queues with bounded buffers.
- `RabbitMQHTTPBroker` uses RabbitMQ management API endpoints for publish/poll/depth operations and supports the same consumer retry contract.
- Consumer flow supports message-age dropping, retries with backoff, and dead-letter routing.

## API reference

### Types

- `Message`: queue payload with metadata and headers.
- `RetryPolicy`: retry configuration (`MaxAttempts`, `Delays`).
- `ConsumerConfig`: consumer controls (`MaxMessageAge`, retry policy, dead-letter queue, time/sleep hooks).
- `Handler`: consumer callback function.
- `InMemoryBroker`: named in-memory queue broker.
- `RabbitMQHTTPConfig`: configuration for RabbitMQ management API access.
- `RabbitMQHTTPBroker`: RabbitMQ-backed broker implementation.

### Functions

- `NewInMemoryBroker(buffer int) *InMemoryBroker`: constructs a broker with per-queue bounded buffers.
- `RabbitMQHTTPConfigFromAMQPURL(amqpURL, exchange string) (RabbitMQHTTPConfig, error)`: derives management endpoint/vhost/auth values from AMQP URL format.
- `NewRabbitMQHTTPBroker(cfg RabbitMQHTTPConfig) (*RabbitMQHTTPBroker, error)`: constructs a RabbitMQ management API broker.
- `ShouldDropMessageByAge(msg Message, now time.Time, maxAge time.Duration) bool`: message age policy helper.

### Methods

- `(RetryPolicy) NextDelay(attempt int) (time.Duration, bool)`: returns retry delay for current attempt and whether retry is allowed.
- `(*InMemoryBroker) Publish(ctx context.Context, queue string, msg Message) error`: publishes a message to a queue.
- `(*InMemoryBroker) Consume(ctx context.Context, queue string, cfg ConsumerConfig, handler Handler)`: consumes and processes messages until context cancellation.
- `(*InMemoryBroker) Depth(queue string) int`: returns pending message count for a queue.
- `(*RabbitMQHTTPBroker) Publish(ctx context.Context, queue string, msg Message) error`: publishes a message via RabbitMQ exchange routing key.
- `(*RabbitMQHTTPBroker) Consume(ctx context.Context, queue string, cfg ConsumerConfig, handler Handler)`: polls and processes queue messages until context cancellation.
- `(*RabbitMQHTTPBroker) Depth(queue string) int`: returns queue depth from RabbitMQ queue metadata.
