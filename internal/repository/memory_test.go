package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lukmi/messaging-service/internal/model"
)

func TestMemoryRepositoryConversationsAndMembers(t *testing.T) {
	repo := NewMemoryRepository()
	ctx := context.Background()

	now := time.Now().UTC()
	conv := &model.Conversation{
		ID:        "c-1",
		Type:      "direct",
		CreatedBy: "user-1",
		CreatedAt: now,
		UpdatedAt: now,
	}

	err := repo.CreateConversation(ctx, conv, []string{"user-1", "user-2"})
	if err != nil {
		t.Fatalf("create conversation failed: %v", err)
	}

	// Duplicate create should fail
	if err := repo.CreateConversation(ctx, conv, []string{"user-1"}); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}

	// Get conversation
	fetched, err := repo.GetConversation(ctx, "c-1")
	if err != nil || fetched.ID != "c-1" {
		t.Fatalf("get conversation failed: %v", err)
	}

	// Members check
	members, err := repo.Members(ctx, "c-1")
	if err != nil || len(members) != 2 {
		t.Fatalf("expected 2 members, got %v", members)
	}

	isMember, _ := repo.IsMember(ctx, "c-1", "user-1")
	if !isMember {
		t.Fatal("user-1 should be a member")
	}

	// Leave conversation
	if err := repo.LeaveConversation(ctx, "c-1", "user-2"); err != nil {
		t.Fatalf("leave conversation failed: %v", err)
	}

	isMember2, _ := repo.IsMember(ctx, "c-1", "user-2")
	if isMember2 {
		t.Fatal("user-2 should no longer be an active member after leaving")
	}
}

func TestMemoryRepositoryMessagesAndIdempotency(t *testing.T) {
	repo := NewMemoryRepository()
	ctx := context.Background()

	msg := &model.Message{
		ID:             "m-1",
		ConversationID: "c-1",
		SenderID:       "user-1",
		Type:           model.Text,
		TextContent:    "Hello World",
		ClientID:       "client-key-100",
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}

	created, err := repo.CreateMessage(ctx, msg)
	if err != nil {
		t.Fatalf("create message failed: %v", err)
	}

	// Idempotent duplicate check
	duplicateMsg := &model.Message{
		ID:             "m-2", // different ID, same clientID
		ConversationID: "c-1",
		SenderID:       "user-1",
		Type:           model.Text,
		TextContent:    "Hello World",
		ClientID:       "client-key-100",
	}

	existing, err := repo.CreateMessage(ctx, duplicateMsg)
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists on duplicate client_id, got %v", err)
	}
	if existing.ID != created.ID {
		t.Fatalf("returned message ID = %s, want original ID %s", existing.ID, created.ID)
	}
}
