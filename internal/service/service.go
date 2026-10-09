package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lukmi/messaging-service/internal/model"
	"github.com/lukmi/messaging-service/internal/repository"
)

var ErrInvalidMessage = errors.New("invalid message")

type EventPublisher interface {
	Publish(context.Context, Event) error
}
type Event struct {
	ID             string         `json:"event_id"`
	Type           string         `json:"event_type"`
	OccurredAt     time.Time      `json:"occurred_at"`
	Message        *model.Message `json:"message,omitempty"`
	ConversationID string         `json:"conversation_id"`
	SenderID       string         `json:"sender_id"`
	RecipientIDs   []string       `json:"recipient_ids,omitempty"`
	UserID         string         `json:"user_id,omitempty"`
}
type Broadcaster interface{ Broadcast([]string, string, any) }
type Service struct {
	repo        repository.Repository
	publisher   EventPublisher
	broadcaster Broadcaster
	now         func() time.Time
	id          func() string
}

func New(repo repository.Repository, publisher EventPublisher, broadcaster Broadcaster) *Service {
	return &Service{repo: repo, publisher: publisher, broadcaster: broadcaster, now: func() time.Time { return time.Now().UTC() }, id: func() string { return fmt.Sprintf("%d", time.Now().UnixNano()) }}
}
func (s *Service) SendMessage(ctx context.Context, userID, conversationID string, input model.Message) (*model.Message, error) {
	member, err := s.repo.IsMember(ctx, conversationID, userID)
	if err != nil {
		return nil, err
	}
	if !member {
		return nil, repository.ErrNotMember
	}
	if !input.Type.Valid() {
		return nil, ErrInvalidMessage
	}
	if input.Type == model.Text && input.TextContent == "" && input.Ciphertext == "" {
		return nil, ErrInvalidMessage
	}
	if input.Type != model.Text && input.MediaID == "" && input.ReferenceID == "" && input.Ciphertext == "" {
		return nil, ErrInvalidMessage
	}
	if (input.Type == model.PostShare || input.Type == model.ProfileShare || input.Type == model.StoryShare) && input.ReferenceID == "" {
		return nil, ErrInvalidMessage
	}
	if input.ClientID != "" {
		if existing, lookupErr := s.repo.GetMessageByClientID(ctx, conversationID, input.ClientID); lookupErr == nil {
			return existing, nil
		}
	}
	now := s.now()
	input.ID = s.id()
	input.ConversationID = conversationID
	input.SenderID = userID
	input.CreatedAt = now
	input.UpdatedAt = now
	created, err := s.repo.CreateMessage(ctx, &input)
	if err != nil {
		return nil, err
	}
	recipients, _ := s.repo.Members(ctx, conversationID)
	event := Event{ID: s.id(), Type: "MESSAGE_SENT", OccurredAt: now, Message: created, ConversationID: conversationID, SenderID: userID, RecipientIDs: recipients}
	if s.publisher != nil {
		_ = s.publisher.Publish(ctx, event)
	}
	if s.broadcaster != nil {
		s.broadcaster.Broadcast(recipients, "message.new", created)
	}
	return created, nil
}
func (s *Service) MarkRead(ctx context.Context, userID, conversationID, messageID string) (*model.MessageRead, error) {
	member, err := s.repo.IsMember(ctx, conversationID, userID)
	if err != nil {
		return nil, err
	}
	if !member {
		return nil, repository.ErrNotMember
	}
	read, err := s.repo.MarkRead(ctx, &model.MessageRead{MessageID: messageID, ConversationID: conversationID, UserID: userID, ReadAt: s.now()})
	if err != nil {
		return nil, err
	}
	if s.publisher != nil {
		_ = s.publisher.Publish(ctx, Event{ID: s.id(), Type: "MESSAGE_READ", OccurredAt: read.ReadAt, ConversationID: conversationID, UserID: userID})
	}
	if s.broadcaster != nil {
		s.broadcaster.Broadcast([]string{userID}, "message.read", read)
	}
	return read, nil
}
