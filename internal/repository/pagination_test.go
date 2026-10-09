package repository

import (
	"context"
	"testing"
	"time"

	"github.com/lukmi/messaging-service/internal/model"
)

func TestCursorEncodeDecode(t *testing.T) {
	now := time.Now().UTC()
	id := "msg-uuid-999"

	encoded := EncodeCursor(now, id)
	if encoded == "" {
		t.Fatal("expected non-empty cursor string")
	}

	decodedTime, decodedID, err := DecodeCursor(encoded)
	if err != nil {
		t.Fatalf("failed to decode valid cursor: %v", err)
	}

	if decodedID != id {
		t.Fatalf("decoded ID = %s, want %s", decodedID, id)
	}

	if decodedTime.UnixNano() != now.UnixNano() {
		t.Fatalf("decoded time = %v, want %v", decodedTime, now)
	}
}

func TestInvalidCursorFormat(t *testing.T) {
	_, _, err := DecodeCursor("invalid-base64-not-a-cursor!!!")
	if err == nil {
		t.Fatal("expected error on malformed cursor")
	}
}

func TestMemoryRepositoryKeysetPagination(t *testing.T) {
	repo := NewMemoryRepository()
	ctx := context.Background()

	_ = repo.CreateConversation(ctx, &model.Conversation{ID: "c-1"}, []string{"user-1"})

	baseTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	for i := 1; i <= 5; i++ {
		msgTime := baseTime.Add(time.Duration(i) * time.Minute)
		_, _ = repo.CreateMessageWithOutbox(ctx, &model.Message{
			ID:             "m-" + time.Duration(i).String(),
			ConversationID: "c-1",
			SenderID:       "user-1",
			Type:           model.Text,
			TextContent:    "Message " + time.Duration(i).String(),
			CreatedAt:      msgTime,
		}, nil)
	}

	// Fetch Page 1 (limit 2)
	page1, nextCursor1, err := repo.ListMessages(ctx, "c-1", "", 2)
	if err != nil {
		t.Fatalf("page 1 failed: %v", err)
	}
	if len(page1) != 2 {
		t.Fatalf("page 1 len = %d, want 2", len(page1))
	}
	if nextCursor1 == "" {
		t.Fatal("expected nextCursor1 for page 1")
	}

	// Fetch Page 2 using nextCursor1 (limit 2)
	page2, nextCursor2, err := repo.ListMessages(ctx, "c-1", nextCursor1, 2)
	if err != nil {
		t.Fatalf("page 2 failed: %v", err)
	}
	if len(page2) != 2 {
		t.Fatalf("page 2 len = %d, want 2", len(page2))
	}

	// Ensure no duplicates between Page 1 and Page 2
	for _, m1 := range page1 {
		for _, m2 := range page2 {
			if m1.ID == m2.ID {
				t.Fatalf("duplicate message %s found across pages!", m1.ID)
			}
		}
	}

	// Fetch Page 3 (final page)
	page3, nextCursor3, err := repo.ListMessages(ctx, "c-1", nextCursor2, 2)
	if err != nil {
		t.Fatalf("page 3 failed: %v", err)
	}
	if len(page3) != 1 {
		t.Fatalf("page 3 len = %d, want 1", len(page3))
	}
	if nextCursor3 != "" {
		t.Fatalf("expected empty nextCursor on last page, got %s", nextCursor3)
	}
}
