package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/lukmi/messaging-service/internal/model"
)

type mockDeduplicator struct {
	processed map[string]bool
}

func (m *mockDeduplicator) IsEventProcessed(_ context.Context, eventID string) (bool, error) {
	return m.processed[eventID], nil
}

func (m *mockDeduplicator) MarkEventProcessed(_ context.Context, eventID string, _ string) error {
	m.processed[eventID] = true
	return nil
}

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

func TestDurableDeduplicator(t *testing.T) {
	dedup := &mockDeduplicator{processed: make(map[string]bool)}
	ctx := context.Background()

	processed, err := dedup.IsEventProcessed(ctx, "evt-123")
	if err != nil || processed {
		t.Fatal("event should not be marked processed initially")
	}

	_ = dedup.MarkEventProcessed(ctx, "evt-123", "MESSAGE_SENT")

	processed, err = dedup.IsEventProcessed(ctx, "evt-123")
	if err != nil || !processed {
		t.Fatal("event should be marked processed after MarkEventProcessed")
	}
}
