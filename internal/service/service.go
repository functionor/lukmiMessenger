package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/lukmi/messaging-service/internal/kafka"
	"github.com/lukmi/messaging-service/internal/model"
	"github.com/lukmi/messaging-service/internal/repository"
)

var (
	ErrInvalidMessage   = errors.New("invalid message content or payload too large")
	ErrInvalidReference = errors.New("shared content requires a valid reference_id")
)

const MaxTextLength = 10000

type Broadcaster interface {
	Broadcast(userIDs []string, eventType string, data any)
}

type Service struct {
	repo        repository.Repository
	publisher   kafka.EventPublisher
	broadcaster Broadcaster
	now         func() time.Time
	id          func() string
}

func New(repo repository.Repository, publisher kafka.EventPublisher, broadcaster Broadcaster) *Service {
	return &Service{
		repo:        repo,
		publisher:   publisher,
		broadcaster: broadcaster,
		now:         func() time.Time { return time.Now().UTC() },
		id: func() string {
			b := make([]byte, 12)
			_, _ = rand.Read(b)
			return fmt.Sprintf("%d-%s", time.Now().UnixNano(), hex.EncodeToString(b))
		},
	}
}

func (s *Service) CreateConversation(ctx context.Context, creatorID string, convType string, memberIDs []string) (*model.Conversation, error) {
	if convType == "" {
		convType = "direct"
	}

	membersMap := make(map[string]bool)
	membersMap[creatorID] = true
	for _, id := range memberIDs {
		if id != "" {
			membersMap[id] = true
		}
	}

	uniqueMembers := make([]string, 0, len(membersMap))
	for id := range membersMap {
		uniqueMembers = append(uniqueMembers, id)
	}

	now := s.now()
	conv := &model.Conversation{
		ID:        s.id(),
		Type:      convType,
		CreatedBy: creatorID,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := s.repo.CreateConversation(ctx, conv, uniqueMembers); err != nil {
		return nil, err
	}

	return conv, nil
}

func (s *Service) GetConversation(ctx context.Context, userID, conversationID string) (*model.Conversation, error) {
	isMember, err := s.repo.IsMember(ctx, conversationID, userID)
	if err != nil {
		return nil, err
	}
	if !isMember {
		return nil, repository.ErrNotMember
	}

	return s.repo.GetConversation(ctx, conversationID)
}

func (s *Service) ListConversations(ctx context.Context, userID string, cursor string, limit int) ([]*model.Conversation, string, error) {
	return s.repo.ListConversations(ctx, userID, cursor, limit)
}

func (s *Service) GetMembers(ctx context.Context, userID, conversationID string) ([]string, error) {
	isMember, err := s.repo.IsMember(ctx, conversationID, userID)
	if err != nil {
		return nil, err
	}
	if !isMember {
		return nil, repository.ErrNotMember
	}

	return s.repo.Members(ctx, conversationID)
}

func (s *Service) LeaveConversation(ctx context.Context, userID, conversationID string) error {
	isMember, err := s.repo.IsMember(ctx, conversationID, userID)
	if err != nil {
		return err
	}
	if !isMember {
		return repository.ErrNotMember
	}

	return s.repo.LeaveConversation(ctx, conversationID, userID)
}

func (s *Service) SendMessage(ctx context.Context, senderID string, conversationID string, input model.Message) (*model.Message, error) {
	isMember, err := s.repo.IsMember(ctx, conversationID, senderID)
	if err != nil {
		return nil, err
	}
	if !isMember {
		return nil, repository.ErrNotMember
	}

	if !input.Type.Valid() {
		return nil, ErrInvalidMessage
	}

	if len(input.TextContent) > MaxTextLength {
		return nil, ErrInvalidMessage
	}

	// Payload validation based on MessageType
	switch input.Type {
	case model.Text:
		if input.TextContent == "" && input.Ciphertext == "" {
			return nil, ErrInvalidMessage
		}
	case model.Image, model.Video, model.Audio, model.File:
		if input.MediaID == "" && input.Ciphertext == "" {
			return nil, ErrInvalidMessage
		}
	case model.PostShare, model.ProfileShare, model.StoryShare:
		if input.ReferenceID == "" {
			return nil, ErrInvalidReference
		}
	}

	// Sanitize Preview snapshot if present
	if input.Preview != nil {
		input.Preview.Username = sanitizeString(input.Preview.Username, 64)
		input.Preview.Caption = sanitizeString(input.Preview.Caption, 256)
		input.Preview.Title = sanitizeString(input.Preview.Title, 128)
		input.Preview.ThumbnailURL = sanitizeString(input.Preview.ThumbnailURL, 512)
	}

	// Idempotency check on client_message_id
	if input.ClientID != "" {
		existing, err := s.repo.GetMessageByClientID(ctx, conversationID, input.ClientID)
		if err == nil && existing != nil {
			return existing, nil
		}
	}

	now := s.now()
	msg := &model.Message{
		ID:             s.id(),
		ConversationID: conversationID,
		SenderID:       senderID, // Force authenticated sender ID
		Type:           input.Type,
		TextContent:    input.TextContent,
		Ciphertext:     input.Ciphertext,
		MediaID:        input.MediaID,
		ReferenceID:    input.ReferenceID,
		ClientID:       input.ClientID,
		Preview:        input.Preview,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	recipients, _ := s.repo.Members(ctx, conversationID)

	event := &model.Event{
		ID:             s.id(),
		Type:           model.EventMessageSent,
		OccurredAt:     now,
		Message:        msg,
		ConversationID: conversationID,
		SenderID:       senderID,
		RecipientIDs:   recipients,
	}

	// Persist message AND outbox event atomically
	created, err := s.repo.CreateMessageWithOutbox(ctx, msg, event)
	if err != nil {
		if errors.Is(err, repository.ErrAlreadyExists) && input.ClientID != "" {
			existing, getErr := s.repo.GetMessageByClientID(ctx, conversationID, input.ClientID)
			if getErr == nil && existing != nil {
				return existing, nil
			}
		}
		return nil, err
	}

	// Broadcast WS event
	if s.broadcaster != nil {
		s.broadcaster.Broadcast(recipients, "message.new", created)
	}

	return created, nil
}

func (s *Service) SyncMessages(ctx context.Context, userID, conversationID string, cursor string, limit int) ([]*model.Message, string, error) {
	isMember, err := s.repo.IsMember(ctx, conversationID, userID)
	if err != nil {
		return nil, "", err
	}
	if !isMember {
		return nil, "", repository.ErrNotMember
	}

	return s.repo.ListMessages(ctx, conversationID, cursor, limit)
}

func (s *Service) MarkRead(ctx context.Context, userID, conversationID, messageID string) (*model.MessageRead, error) {
	isMember, err := s.repo.IsMember(ctx, conversationID, userID)
	if err != nil {
		return nil, err
	}
	if !isMember {
		return nil, repository.ErrNotMember
	}

	now := s.now()
	read := &model.MessageRead{
		MessageID:      messageID,
		ConversationID: conversationID,
		UserID:         userID,
		ReadAt:         now,
	}

	recipients, _ := s.repo.Members(ctx, conversationID)

	event := &model.Event{
		ID:             s.id(),
		Type:           model.EventMessageRead,
		OccurredAt:     now,
		MessageRead:    read,
		ConversationID: conversationID,
		UserID:         userID,
		RecipientIDs:   recipients,
	}

	res, err := s.repo.MarkReadWithOutbox(ctx, read, event)
	if err != nil {
		return nil, err
	}

	if s.broadcaster != nil {
		s.broadcaster.Broadcast(recipients, "message.read", res)
	}

	return res, nil
}

func (s *Service) DeleteMessage(ctx context.Context, userID, conversationID, messageID string) error {
	isMember, err := s.repo.IsMember(ctx, conversationID, userID)
	if err != nil {
		return err
	}
	if !isMember {
		return repository.ErrNotMember
	}

	recipients, _ := s.repo.Members(ctx, conversationID)
	now := s.now()

	event := &model.Event{
		ID:             s.id(),
		Type:           model.EventMessageDeleted,
		OccurredAt:     now,
		ConversationID: conversationID,
		SenderID:       userID,
		RecipientIDs:   recipients,
		UserID:         userID,
	}

	if err := s.repo.DeleteMessageWithOutbox(ctx, conversationID, messageID, userID, event); err != nil {
		return err
	}

	if s.broadcaster != nil {
		s.broadcaster.Broadcast(recipients, "message.deleted", map[string]string{
			"message_id":      messageID,
			"conversation_id": conversationID,
			"deleted_by":      userID,
		})
	}

	return nil
}

func sanitizeString(s string, maxLen int) string {
	if len(s) > maxLen {
		return s[:maxLen]
	}
	return s
}
