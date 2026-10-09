package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/lukmi/messaging-service/internal/model"
	"github.com/segmentio/kafka-go"
)

type EventPublisher interface {
	Publish(ctx context.Context, event model.Event) error
	Close() error
}

type Producer struct {
	writer *kafka.Writer
	logger *slog.Logger
	topic  string
}

func NewProducer(brokers []string, topic string, logger *slog.Logger) (*Producer, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("no kafka/redpanda brokers provided")
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // Hash by Key (ConversationID) to preserve message ordering per conversation
		MaxAttempts:  5,
		WriteTimeout: 10 * time.Second,
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}

	return &Producer{
		writer: writer,
		logger: logger,
		topic:  topic,
	}, nil
}

func (p *Producer) Publish(ctx context.Context, event model.Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal kafka event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(event.ConversationID),
		Value: payload,
		Time:  event.OccurredAt,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(event.Type)},
			{Key: "event_id", Value: []byte(event.ID)},
		},
	}

	ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := p.writer.WriteMessages(ctxTimeout, msg); err != nil {
		p.logger.Error("failed to publish kafka event", "event_type", event.Type, "event_id", event.ID, "error", err)
		return fmt.Errorf("kafka write failure: %w", err)
	}

	p.logger.Debug("published kafka event", "event_type", event.Type, "event_id", event.ID, "topic", p.topic)
	return nil
}

func (p *Producer) Close() error {
	if p.writer != nil {
		return p.writer.Close()
	}
	return nil
}

type Consumer struct {
	reader          *kafka.Reader
	logger          *slog.Logger
	processedEvents map[string]time.Time
	mu              sync.RWMutex
}

func NewConsumer(brokers []string, topic, groupID string, logger *slog.Logger) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       10,
		MaxBytes:       10 * 1024 * 1024, // 10 MB
		CommitInterval: 1 * time.Second,
		StartOffset:    kafka.FirstOffset,
	})

	return &Consumer{
		reader:          reader,
		logger:          logger,
		processedEvents: make(map[string]time.Time),
	}
}

func (c *Consumer) Start(ctx context.Context, handler func(ctx context.Context, event model.Event) error) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				_ = c.reader.Close()
				return
			default:
				msg, err := c.reader.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					c.logger.Error("error fetching kafka message", "error", err)
					time.Sleep(1 * time.Second)
					continue
				}

				var event model.Event
				if err := json.Unmarshal(msg.Value, &event); err != nil {
					c.logger.Error("error unmarshaling kafka event", "error", err)
					_ = c.reader.CommitMessages(ctx, msg)
					continue
				}

				// Idempotency check for duplicate event delivery
				if c.isDuplicate(event.ID) {
					c.logger.Info("ignoring duplicate kafka event", "event_id", event.ID)
					_ = c.reader.CommitMessages(ctx, msg)
					continue
				}

				if err := handler(ctx, event); err != nil {
					c.logger.Error("error processing kafka event", "event_id", event.ID, "error", err)
					// Handle retries or error logging without committing if needed
				} else {
					c.markProcessed(event.ID)
					_ = c.reader.CommitMessages(ctx, msg)
				}
			}
		}
	}()
}

func (c *Consumer) isDuplicate(eventID string) bool {
	if eventID == "" {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, exists := c.processedEvents[eventID]
	return exists
}

func (c *Consumer) markProcessed(eventID string) {
	if eventID == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.processedEvents[eventID] = time.Now()

	// Clean up old tracked events if map gets large
	if len(c.processedEvents) > 10000 {
		cutoff := time.Now().Add(-24 * time.Hour)
		for id, t := range c.processedEvents {
			if t.Before(cutoff) {
				delete(c.processedEvents, id)
			}
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

// MemoryPublisher is an in-memory event publisher for unit testing and local development
type MemoryPublisher struct {
	mu     sync.RWMutex
	Events []model.Event
	logger *slog.Logger
}

func NewMemoryPublisher(logger *slog.Logger) *MemoryPublisher {
	return &MemoryPublisher{logger: logger}
}

func (m *MemoryPublisher) Publish(_ context.Context, event model.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Events = append(m.Events, event)
	if m.logger != nil {
		m.logger.Info("memory publisher recorded event", "event_type", event.Type, "event_id", event.ID)
	}
	return nil
}

func (m *MemoryPublisher) Close() error {
	return nil
}

func (m *MemoryPublisher) GetEvents() []model.Event {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cp := make([]model.Event, len(m.Events))
	copy(cp, m.Events)
	return cp
}
