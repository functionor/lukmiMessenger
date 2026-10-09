package service

import (
	"context"
	"errors"
	"testing"

	"github.com/lukmi/messaging-service/internal/kafka"
	"github.com/lukmi/messaging-service/internal/model"
	"github.com/lukmi/messaging-service/internal/repository"
)

type testBroadcaster struct {
	userIDs   []string
	eventType string
	payload   any
}

func (b *testBroadcaster) Broadcast(userIDs []string, eventType string, payload any) {
	b.userIDs, b.eventType, b.payload = userIDs, eventType, payload
}

func testService() (*Service, *repository.MemoryRepository, *kafka.MemoryPublisher, *testBroadcaster) {
	repo := repository.NewMemoryRepository()
	publisher := kafka.NewMemoryPublisher(nil)
	broadcaster := &testBroadcaster{}
	svc := New(repo, publisher, broadcaster)
	now := int64(1)
	svc.id = func() string {
		now++
		return "id"
	}
	return svc, repo, publisher, broadcaster
}

func createConversation(t *testing.T, repo *repository.MemoryRepository) {
	t.Helper()
	if err := repo.CreateConversation(context.Background(), &model.Conversation{ID: "conversation-1"}, []string{"john", "sarah"}); err != nil {
		t.Fatal(err)
	}
}

func TestSendTextMessageUsesAuthenticatedSenderAndPublishes(t *testing.T) {
	svc, repo, publisher, broadcaster := testService()
	createConversation(t, repo)

	message, err := svc.SendMessage(context.Background(), "john", "conversation-1", model.Message{
		Type:        model.Text,
		TextContent: "hello",
		SenderID:    "attacker", // Should be overridden by authenticated sender "john"
		ClientID:    "client-1",
	})
	if err != nil {
		t.Fatal(err)
	}

	if message.SenderID != "john" {
		t.Fatalf("sender = %q, want john", message.SenderID)
	}

	events := publisher.GetEvents()
	if len(events) != 1 || events[0].Type != model.EventMessageSent {
		t.Fatalf("unexpected events: %+v", events)
	}

	if broadcaster.eventType != "message.new" {
		t.Fatalf("expected ws broadcast event message.new, got %s", broadcaster.eventType)
	}
}

func TestSendSharedPostStoresReferenceWithoutFetchingResource(t *testing.T) {
	svc, repo, _, _ := testService()
	createConversation(t, repo)

	message, err := svc.SendMessage(context.Background(), "john", "conversation-1", model.Message{
		Type:        model.PostShare,
		ReferenceID: "post-123",
	})
	if err != nil {
		t.Fatal(err)
	}

	if message.ReferenceID != "post-123" || message.TextContent != "" {
		t.Fatalf("unexpected shared message: %+v", message)
	}
}

func TestSendSharedProfileAndStoryStoresReference(t *testing.T) {
	svc, repo, _, _ := testService()
	createConversation(t, repo)

	// Profile share
	profMsg, err := svc.SendMessage(context.Background(), "john", "conversation-1", model.Message{
		Type:        model.ProfileShare,
		ReferenceID: "user-456",
	})
	if err != nil {
		t.Fatalf("profile share failed: %v", err)
	}
	if profMsg.ReferenceID != "user-456" {
		t.Fatalf("expected reference_id user-456, got %s", profMsg.ReferenceID)
	}

	// Story share
	storyMsg, err := svc.SendMessage(context.Background(), "john", "conversation-1", model.Message{
		Type:        model.StoryShare,
		ReferenceID: "story-789",
	})
	if err != nil {
		t.Fatalf("story share failed: %v", err)
	}
	if storyMsg.ReferenceID != "story-789" {
		t.Fatalf("expected reference_id story-789, got %s", storyMsg.ReferenceID)
	}
}

func TestSendE2EEMessageStoresCiphertext(t *testing.T) {
	svc, repo, _, _ := testService()
	createConversation(t, repo)

	message, err := svc.SendMessage(context.Background(), "john", "conversation-1", model.Message{
		Type:       model.Text,
		Ciphertext: "encrypted_payload_bytes_base64",
	})
	if err != nil {
		t.Fatal(err)
	}

	if message.Ciphertext != "encrypted_payload_bytes_base64" {
		t.Fatalf("expected ciphertext, got %s", message.Ciphertext)
	}
}

func TestSendRejectsNonMember(t *testing.T) {
	svc, repo, _, _ := testService()
	createConversation(t, repo)

	_, err := svc.SendMessage(context.Background(), "mallory", "conversation-1", model.Message{
		Type:        model.Text,
		TextContent: "nope",
	})
	if !errors.Is(err, repository.ErrNotMember) {
		t.Fatalf("error = %v, want non-member", err)
	}
}

func TestSendIsIdempotentByClientID(t *testing.T) {
	svc, repo, _, _ := testService()
	createConversation(t, repo)

	input := model.Message{
		Type:        model.Text,
		TextContent: "retry",
		ClientID:    "same-client-id",
	}

	first, err := svc.SendMessage(context.Background(), "john", "conversation-1", input)
	if err != nil {
		t.Fatal(err)
	}

	second, err := svc.SendMessage(context.Background(), "john", "conversation-1", input)
	if err != nil {
		t.Fatal(err)
	}

	if first.ID != second.ID {
		t.Fatalf("IDs differ for idempotent requests: %q and %q", first.ID, second.ID)
	}

	messages, _ := repo.ListMessages(context.Background(), "conversation-1", "", 10)
	if len(messages) != 1 {
		t.Fatalf("stored %d messages, want 1", len(messages))
	}
}

func TestMarkReadPublishesEventAndBroadcasts(t *testing.T) {
	svc, repo, publisher, broadcaster := testService()
	createConversation(t, repo)

	msg, _ := svc.SendMessage(context.Background(), "john", "conversation-1", model.Message{
		Type:        model.Text,
		TextContent: "read me",
	})

	read, err := svc.MarkRead(context.Background(), "sarah", "conversation-1", msg.ID)
	if err != nil {
		t.Fatal(err)
	}

	if read.UserID != "sarah" || read.MessageID != msg.ID {
		t.Fatalf("unexpected read receipt: %+v", read)
	}

	events := publisher.GetEvents()
	if len(events) != 2 || events[1].Type != model.EventMessageRead {
		t.Fatalf("expected MESSAGE_READ event, got %+v", events)
	}

	if broadcaster.eventType != "message.read" {
		t.Fatalf("expected ws broadcast event message.read, got %s", broadcaster.eventType)
	}
}

func TestDeleteMessageVerifiesSenderAndPublishesEvent(t *testing.T) {
	svc, repo, publisher, broadcaster := testService()
	createConversation(t, repo)

	msg, _ := svc.SendMessage(context.Background(), "john", "conversation-1", model.Message{
		Type:        model.Text,
		TextContent: "delete me",
	})

	// Sarah (non-sender) attempts to delete John's message -> should fail
	err := svc.DeleteMessage(context.Background(), "sarah", "conversation-1", msg.ID)
	if !errors.Is(err, repository.ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized, got %v", err)
	}

	// John (sender) deletes own message -> should succeed
	err = svc.DeleteMessage(context.Background(), "john", "conversation-1", msg.ID)
	if err != nil {
		t.Fatalf("delete message failed: %v", err)
	}

	events := publisher.GetEvents()
	if len(events) != 2 || events[1].Type != model.EventMessageDeleted {
		t.Fatalf("expected MESSAGE_DELETED event, got %+v", events)
	}

	if broadcaster.eventType != "message.deleted" {
		t.Fatalf("expected ws broadcast event message.deleted, got %s", broadcaster.eventType)
	}
}

func TestLeaveConversation(t *testing.T) {
	svc, repo, _, _ := testService()
	createConversation(t, repo)

	err := svc.LeaveConversation(context.Background(), "sarah", "conversation-1")
	if err != nil {
		t.Fatalf("leave conversation failed: %v", err)
	}

	// Sarah tries sending message after leaving -> should be denied
	_, err = svc.SendMessage(context.Background(), "sarah", "conversation-1", model.Message{
		Type:        model.Text,
		TextContent: "I left",
	})
	if !errors.Is(err, repository.ErrNotMember) {
		t.Fatalf("expected ErrNotMember after leaving, got %v", err)
	}
}
