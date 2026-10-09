package kafka

import (
	"context"
	"github.com/lukmi/messaging-service/internal/service"
	"log/slog"
)

// Publisher is the Kafka/Redpanda port. Replace this implementation with the team's producer adapter.
type Publisher struct{ logger *slog.Logger }

func NewPublisher(logger *slog.Logger) *Publisher { return &Publisher{logger: logger} }
func (p *Publisher) Publish(_ context.Context, event service.Event) error {
	p.logger.Info("event queued", "event_type", event.Type, "event_id", event.ID)
	return nil
}
