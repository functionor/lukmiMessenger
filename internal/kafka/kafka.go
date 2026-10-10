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

type Deduplicator interface {
	IsEventProcessed(ctx context.Context, eventID string) (bool, error)
	MarkEventProcessed(ctx context.Context, eventID string, eventType string) error
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
		Balancer:     &kafka.Hash{},
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
	reader *kafka.Reader
	dedup  Deduplicator
	logger *slog.Logger
}

func NewConsumer(brokers []string, topic, groupID string, dedup Deduplicator, logger *slog.Logger) *Consumer {
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
		reader: reader,
		dedup:  dedup,
		logger: logger,
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
					c.logger.Error("error unmarshaling kafka event payload, dead-lettering message", "error", err)
					_ = c.reader.CommitMessages(ctx, msg)
					continue
				}

				// Durable idempotency check
				if c.dedup != nil {
					processed, checkErr := c.dedup.IsEventProcessed(ctx, event.ID)
					if checkErr != nil {
						c.logger.Error("failed deduplication check, offset NOT committed for retry", "event_id", event.ID, "error", checkErr)
						time.Sleep(1 * time.Second)
						continue
					}
					if processed {
						c.logger.Info("ignoring duplicate kafka event via durable deduplicator", "event_id", event.ID)
						_ = c.reader.CommitMessages(ctx, msg)
						continue
					}
				}

				if err := handler(ctx, event); err != nil {
					c.logger.Error("error processing kafka event, offset NOT committed for retry", "event_id", event.ID, "error", err)
					time.Sleep(1 * time.Second)
				} else {
					if c.dedup != nil {
						if markErr := c.dedup.MarkEventProcessed(ctx, event.ID, string(event.Type)); markErr != nil {
							c.logger.Error("failed to mark event processed, offset NOT committed for retry", "event_id", event.ID, "error", markErr)
							time.Sleep(1 * time.Second)
							continue
						}
					}
					if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
						c.logger.Error("failed to commit kafka message offset", "event_id", event.ID, "error", commitErr)
					}
				}
			}
		}
	}()
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
