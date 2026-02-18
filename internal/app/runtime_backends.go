package app

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/cam3ron2/github-stats/internal/backfill"
	"github.com/cam3ron2/github-stats/internal/config"
	queuepkg "github.com/cam3ron2/github-stats/internal/queue"
	"github.com/cam3ron2/github-stats/internal/store"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type queueBroker interface {
	Publish(ctx context.Context, queue string, msg queuepkg.Message) error
	Consume(ctx context.Context, queue string, cfg queuepkg.ConsumerConfig, handler queuepkg.Handler)
	Depth(queue string) int
	OldestAge(queue string, now time.Time) time.Duration
	Health(ctx context.Context, queue string) error
}

type brokerBackfillQueue struct {
	broker      queueBroker
	queueName   string
	dlqName     string
	retryPolicy queuepkg.RetryPolicy
	retryQueues []string
}

func newRuntimeBackends(cfg *config.Config, logger *zap.Logger, retention time.Duration, maxSeries int) (runtimeStore, runtimeQueue) {
	storeBackend := runtimeStore(store.NewMemoryStore(retention, maxSeries))
	if cfg != nil && strings.EqualFold(strings.TrimSpace(cfg.Store.Backend), "redis") {
		redisStore, err := newRedisStoreFromConfig(cfg, retention, maxSeries)
		if err != nil {
			logger.Warn("failed to initialize redis store; falling back to in-memory store", zap.Error(err))
		} else {
			storeBackend = redisStore
		}
	}

	queueName := "gh.backfill.jobs"
	dlqName := ""
	retryPolicy := queuepkg.RetryPolicy{MaxAttempts: 7}
	retryQueues := []string{}

	if cfg != nil {
		if strings.TrimSpace(cfg.AMQP.Queue) != "" {
			queueName = strings.TrimSpace(cfg.AMQP.Queue)
		}
		dlqName = strings.TrimSpace(cfg.AMQP.DLQ)
		retryPolicy = queuepkg.RetryPolicy{
			MaxAttempts: cfg.Retry.MaxAttempts,
			Delays:      cfg.Backfill.RequeueDelays,
		}
		retryQueues = buildRetryQueueNames(queueName, retryPolicy.Delays)
		if retryPolicy.MaxAttempts <= 0 {
			retryPolicy.MaxAttempts = 7
		}
	}

	broker := queueBroker(queuepkg.NewInMemoryBroker(10000))
	if cfg != nil && strings.TrimSpace(cfg.AMQP.URL) != "" && strings.TrimSpace(cfg.AMQP.Exchange) != "" {
		rabbitConfig, err := queuepkg.RabbitMQHTTPConfigFromAMQPURL(cfg.AMQP.URL, cfg.AMQP.Exchange)
		if err != nil {
			logger.Warn("failed to derive rabbitmq management config; falling back to in-memory queue", zap.Error(err))
		} else {
			rabbitConfig.PollInterval = 500 * time.Millisecond
			rabbitBroker, brokerErr := queuepkg.NewRabbitMQHTTPBroker(rabbitConfig)
			if brokerErr != nil {
				logger.Warn("failed to initialize rabbitmq queue; falling back to in-memory queue", zap.Error(brokerErr))
			} else {
				topology := queuepkg.TopologyConfig{
					MainQueue:       queueName,
					DeadLetterQueue: dlqName,
					RetryQueues:     buildRetryQueueSpecs(retryQueues, retryPolicy.Delays),
				}
				if topologyErr := rabbitBroker.EnsureTopology(context.Background(), topology); topologyErr != nil {
					logger.Warn("failed to declare rabbitmq topology", zap.Error(topologyErr))
				}
				broker = rabbitBroker
			}
		}
	}

	return storeBackend, &brokerBackfillQueue{
		broker:      broker,
		queueName:   queueName,
		dlqName:     dlqName,
		retryPolicy: retryPolicy,
		retryQueues: retryQueues,
	}
}

func newRedisStoreFromConfig(cfg *config.Config, retention time.Duration, maxSeries int) (*store.RedisStore, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is required")
	}

	var redisClient redis.UniversalClient
	if strings.EqualFold(cfg.Store.RedisMode, "sentinel") {
		redisClient = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    cfg.Store.RedisMasterSet,
			SentinelAddrs: cfg.Store.RedisSentinelAddrs,
			Password:      cfg.Store.RedisPassword,
			DB:            cfg.Store.RedisDB,
		})
	} else {
		redisClient = redis.NewClient(&redis.Options{
			Addr:     cfg.Store.RedisAddr,
			Password: cfg.Store.RedisPassword,
			DB:       cfg.Store.RedisDB,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		_ = redisClient.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	return store.NewRedisStore(redisClient, store.RedisStoreConfig{
		Namespace:   "ghm",
		Retention:   retention,
		MaxSeries:   maxSeries,
		IndexShards: cfg.Store.IndexShards,
	}), nil
}

func (q *brokerBackfillQueue) Publish(msg backfill.Message) error {
	if q == nil || q.broker == nil {
		return fmt.Errorf("backfill queue is not initialized")
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal backfill message: %w", err)
	}

	return q.broker.Publish(context.Background(), q.queueName, queuepkg.Message{
		ID:        msg.JobID,
		Body:      body,
		CreatedAt: msg.CreatedAt,
		Attempt:   msg.Attempt,
	})
}

func (q *brokerBackfillQueue) Consume(
	ctx context.Context,
	handler func(backfill.Message) error,
	maxMessageAge time.Duration,
	nowFn func() time.Time,
) {
	if q == nil || q.broker == nil || handler == nil {
		return
	}

	q.broker.Consume(ctx, q.queueName, queuepkg.ConsumerConfig{
		MaxMessageAge:   maxMessageAge,
		RetryPolicy:     q.retryPolicy,
		RetryQueues:     q.retryQueues,
		DeadLetterQueue: q.dlqName,
		Now:             nowFn,
	}, func(ctx context.Context, msg queuepkg.Message) error {
		var payload backfill.Message
		if err := json.Unmarshal(msg.Body, &payload); err != nil {
			return fmt.Errorf("decode backfill message: %w", err)
		}
		if payload.JobID == "" {
			payload.JobID = msg.ID
		}
		if payload.Attempt <= 0 {
			payload.Attempt = msg.Attempt
		}
		if payload.CreatedAt.IsZero() {
			payload.CreatedAt = msg.CreatedAt
		}
		return handler(payload)
	})
}

func (q *brokerBackfillQueue) Depth() int {
	if q == nil || q.broker == nil {
		return 0
	}
	return q.broker.Depth(q.queueName)
}

func (q *brokerBackfillQueue) OldestMessageAge(now time.Time) time.Duration {
	if q == nil || q.broker == nil {
		return 0
	}
	return q.broker.OldestAge(q.queueName, now)
}

func (q *brokerBackfillQueue) Healthy(ctx context.Context) bool {
	if q == nil || q.broker == nil {
		return false
	}
	return q.broker.Health(ctx, q.queueName) == nil
}

func buildRetryQueueNames(baseQueue string, delays []time.Duration) []string {
	if strings.TrimSpace(baseQueue) == "" || len(delays) == 0 {
		return nil
	}
	names := make([]string, 0, len(delays))
	for _, delay := range delays {
		if delay <= 0 {
			continue
		}
		names = append(names, fmt.Sprintf("%s.retry.%s", baseQueue, delayToken(delay)))
	}
	return names
}

func buildRetryQueueSpecs(names []string, delays []time.Duration) []queuepkg.RetryQueueSpec {
	if len(names) == 0 || len(delays) == 0 {
		return nil
	}
	retryQueues := make([]queuepkg.RetryQueueSpec, 0, len(names))
	for i, name := range names {
		if i >= len(delays) {
			break
		}
		retryQueues = append(retryQueues, queuepkg.RetryQueueSpec{
			Name:  name,
			Delay: delays[i],
		})
	}
	return retryQueues
}

func delayToken(delay time.Duration) string {
	token := strings.ReplaceAll(delay.String(), " ", "")
	token = strings.ReplaceAll(token, ".", "_")
	return token
}
