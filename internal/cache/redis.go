package cache

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Cache interface {
	SetPresence(ctx context.Context, userID string, status string, ttl time.Duration) error
	GetPresence(ctx context.Context, userID string) (string, error)
	IncConnections(ctx context.Context, userID string) (int64, error)
	DecConnections(ctx context.Context, userID string) (int64, error)
	GetConnections(ctx context.Context, userID string) (int64, error)
	PublishEvent(ctx context.Context, channel string, payload []byte) error
	Subscribe(ctx context.Context, channel string) (<-chan []byte, func(), error)
	Ping(ctx context.Context) error
}

type RedisCache struct {
	client *redis.Client
	logger *slog.Logger
}

func NewRedisCache(redisURL string, logger *slog.Logger) (*RedisCache, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("invalid redis url: %w", err)
	}

	client := redis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	return &RedisCache{client: client, logger: logger}, nil
}

func (r *RedisCache) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

func (r *RedisCache) SetPresence(ctx context.Context, userID string, status string, ttl time.Duration) error {
	key := fmt.Sprintf("user:%s:presence", userID)
	return r.client.Set(ctx, key, status, ttl).Err()
}

func (r *RedisCache) GetPresence(ctx context.Context, userID string) (string, error) {
	key := fmt.Sprintf("user:%s:presence", userID)
	val, err := r.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return "offline", nil
	}
	return val, err
}

func (r *RedisCache) IncConnections(ctx context.Context, userID string) (int64, error) {
	key := fmt.Sprintf("user:%s:connections", userID)
	pipe := r.client.Pipeline()
	incr := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, 24*time.Hour)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return 0, err
	}
	return incr.Val(), nil
}

func (r *RedisCache) DecConnections(ctx context.Context, userID string) (int64, error) {
	key := fmt.Sprintf("user:%s:connections", userID)
	val, err := r.client.Decr(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	if val <= 0 {
		r.client.Del(ctx, key)
		return 0, nil
	}
	return val, nil
}

func (r *RedisCache) GetConnections(ctx context.Context, userID string) (int64, error) {
	key := fmt.Sprintf("user:%s:connections", userID)
	val, err := r.client.Get(ctx, key).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return val, err
}

func (r *RedisCache) PublishEvent(ctx context.Context, channel string, payload []byte) error {
	return r.client.Publish(ctx, channel, payload).Err()
}

func (r *RedisCache) Subscribe(ctx context.Context, channel string) (<-chan []byte, func(), error) {
	pubsub := r.client.Subscribe(ctx, channel)
	ch := make(chan []byte, 100)

	go func() {
		defer close(ch)
		for msg := range pubsub.Channel() {
			ch <- []byte(msg.Payload)
		}
	}()

	cancel := func() {
		_ = pubsub.Close()
	}

	return ch, cancel, nil
}

// MemoryCache is the fallback implementation for local dev and testing when Redis is disabled
type MemoryCache struct {
	mu          sync.RWMutex
	presence    map[string]string
	connections map[string]int64
	channels    map[string][]chan []byte
}

func NewMemoryCache() *MemoryCache {
	return &MemoryCache{
		presence:    make(map[string]string),
		connections: make(map[string]int64),
		channels:    make(map[string][]chan []byte),
	}
}

func (m *MemoryCache) Ping(_ context.Context) error {
	return nil
}

func (m *MemoryCache) SetPresence(_ context.Context, userID string, status string, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.presence[userID] = status
	return nil
}

func (m *MemoryCache) GetPresence(_ context.Context, userID string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.presence[userID]
	if !ok {
		return "offline", nil
	}
	return val, nil
}

func (m *MemoryCache) IncConnections(_ context.Context, userID string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connections[userID]++
	return m.connections[userID], nil
}

func (m *MemoryCache) DecConnections(_ context.Context, userID string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.connections[userID] > 0 {
		m.connections[userID]--
	}
	return m.connections[userID], nil
}

func (m *MemoryCache) GetConnections(_ context.Context, userID string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.connections[userID], nil
}

func (m *MemoryCache) PublishEvent(_ context.Context, channel string, payload []byte) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	subs := m.channels[channel]
	for _, sub := range subs {
		select {
		case sub <- payload:
		default:
		}
	}
	return nil
}

func (m *MemoryCache) Subscribe(_ context.Context, channel string) (<-chan []byte, func(), error) {
	m.mu.Lock()
	ch := make(chan []byte, 100)
	m.channels[channel] = append(m.channels[channel], ch)
	m.mu.Unlock()

	cancel := func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		subs := m.channels[channel]
		for i, sub := range subs {
			if sub == ch {
				m.channels[channel] = append(subs[:i], subs[i+1:]...)
				close(ch)
				break
			}
		}
	}

	return ch, cancel, nil
}
