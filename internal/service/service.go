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
	ErrInvalidMessage   = errors.New("invalid message content or type")
	ErrInvalidReference = errors.New("shared content requires a valid reference_id")
)

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

	// Ensure creator is included in members
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

func (s *Service) ListConversations(ctx context.Context, userID string, cursor string, limit int) ([]*model.Conversation, error) {
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

	// Validate message payload based on MessageType
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

	created, err := s.repo.CreateMessage(ctx, msg)
	if err != nil {
		if errors.Is(err, repository.ErrAlreadyExists) && input.ClientID != "" {
			existing, getErr := s.repo.GetMessageByClientID(ctx, conversationID, input.ClientID)
			if getErr == nil && existing != nil {
				return existing, nil
			}
		}
		return nil, err
	}

	recipients, _ := s.repo.Members(ctx, conversationID)

	// Build Kafka event
	event := model.Event{
		ID:             s.id(),
		Type:           model.EventMessageSent,
		OccurredAt:     now,
		Message:        created,
		ConversationID: conversationID,
		SenderID:       senderID,
		RecipientIDs:   recipients,
	}

	if s.publisher != nil {
		_ = s.publisher.Publish(ctx, event)
	}

	if s.broadcaster != nil {
		s.broadcaster.Broadcast(recipients, "message.new", created)
	}

	return created, nil
}

func (s *Service) SyncMessages(ctx context.Context, userID, conversationID string, cursor string, limit int) ([]*model.Message, error) {
	isMember, err := s.repo.IsMember(ctx, conversationID, userID)
	if err != nil {
		return nil, err
	}
	if !isMember {
		return nil, repository.ErrNotMember
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

	res, err := s.repo.MarkRead(ctx, read)
	if err != nil {
		return nil, err
	}

	recipients, _ := s.repo.Members(ctx, conversationID)

	event := model.Event{
		ID:             s.id(),
		Type:           model.EventMessageRead,
		OccurredAt:     now,
		MessageRead:    res,
		ConversationID: conversationID,
		UserID:         userID,
		RecipientIDs:   recipients,
	}

	if s.publisher != nil {
		_ = s.publisher.Publish(ctx, event)
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

	if err := s.repo.DeleteMessage(ctx, conversationID, messageID, userID); err != nil {
		return err
	}

	recipients, _ := s.repo.Members(ctx, conversationID)
	now := s.now()

	event := model.Event{
		ID:             s.id(),
		Type:           model.EventMessageDeleted,
		OccurredAt:     now,
		ConversationID: conversationID,
		SenderID:       userID,
		RecipientIDs:   recipients,
		UserID:         userID,
	}

	if s.publisher != nil {
		_ = s.publisher.Publish(ctx, event)
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
