package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/lukmi/messaging-service/internal/model"
)

func TestMemoryPublisherRecordsEvents(t *testing.T) {
	pub := NewMemoryPublisher(nil)
	ctx := context.Background()

	event := model.Event{
		ID:             "event-1",
		Type:           model.EventMessageSent,
		OccurredAt:     time.Now().UTC(),
		ConversationID: "conv-1",
		SenderID:       "user-1",
	}

	if err := pub.Publish(ctx, event); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	events := pub.GetEvents()
	if len(events) != 1 {
		t.Fatalf("event count = %d, want 1", len(events))
	}

	if events[0].ID != "event-1" || events[0].Type != model.EventMessageSent {
		t.Fatalf("recorded event mismatch: %+v", events[0])
	}
}

func TestConsumerDuplicateHandling(t *testing.T) {
	consumer := &Consumer{
		processedEvents: make(map[string]time.Time),
	}

	if consumer.isDuplicate("evt-123") {
		t.Fatal("new event should not be marked duplicate")
	}

	consumer.markProcessed("evt-123")

	if !consumer.isDuplicate("evt-123") {
		t.Fatal("processed event should be marked duplicate")
	}
}
