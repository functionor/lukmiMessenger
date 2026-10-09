package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/lukmi/messaging-service/internal/model"
)

type mockOutboxRepo struct {
	events    map[string]*model.Event
	published map[string]bool
	failures  map[string]int
}

func newMockOutboxRepo() *mockOutboxRepo {
	return &mockOutboxRepo{
		events:    make(map[string]*model.Event),
		published: make(map[string]bool),
		failures:  make(map[string]int),
	}
}

func (m *mockOutboxRepo) GetPendingOutboxEvents(_ context.Context, limit int) ([]*model.Event, error) {
	var res []*model.Event
	for id, evt := range m.events {
		if !m.published[id] {
			res = append(res, evt)
			if len(res) == limit {
				break
			}
		}
	}
	return res, nil
}

func (m *mockOutboxRepo) MarkOutboxEventPublished(_ context.Context, eventID string) error {
	m.published[eventID] = true
	return nil
}

func (m *mockOutboxRepo) RecordOutboxEventFailure(_ context.Context, eventID string, _ string) error {
	m.failures[eventID]++
	return nil
}

func TestOutboxProcessorPublishesPendingEvents(t *testing.T) {
	repo := newMockOutboxRepo()
	publisher := NewMemoryPublisher(nil)
	processor := NewOutboxProcessor(repo, publisher, nil, 10*time.Millisecond)

	evt := &model.Event{
		ID:             "outbox-evt-1",
		Type:           model.EventMessageSent,
		OccurredAt:     time.Now().UTC(),
		ConversationID: "conv-1",
	}
	repo.events[evt.ID] = evt

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	processor.Start(ctx)
	time.Sleep(100 * time.Millisecond)
	processor.Stop()

	if !repo.published[evt.ID] {
		t.Fatalf("expected event %s to be marked published", evt.ID)
	}

	publishedEvents := publisher.GetEvents()
	if len(publishedEvents) != 1 || publishedEvents[0].ID != evt.ID {
		t.Fatalf("unexpected published events: %+v", publishedEvents)
	}
}
