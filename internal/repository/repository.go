package repository

import (
	"context"
	"errors"
	"time"

	"github.com/lukmi/messaging-service/internal/model"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
	ErrNotMember     = errors.New("not a conversation member")
	ErrUnauthorized  = errors.New("unauthorized action on message")
	ErrInvalidCursor = errors.New("invalid pagination cursor")
)

type Repository interface {
	CreateConversation(ctx context.Context, c *model.Conversation, userIDs []string) error
	GetConversation(ctx context.Context, id string) (*model.Conversation, error)
	ListConversations(ctx context.Context, userID string, cursor string, limit int) ([]*model.Conversation, string, error)
	Members(ctx context.Context, conversationID string) ([]string, error)
	IsMember(ctx context.Context, conversationID, userID string) (bool, error)
	AddMember(ctx context.Context, conversationID, userID string) error
	LeaveConversation(ctx context.Context, conversationID, userID string) error
	CreateMessageWithOutbox(ctx context.Context, m *model.Message, event *model.Event) (*model.Message, error)
	GetMessageByID(ctx context.Context, conversationID, messageID string) (*model.Message, error)
	GetMessageByClientID(ctx context.Context, conversationID, clientID string) (*model.Message, error)
	ListMessages(ctx context.Context, conversationID string, cursor string, limit int) ([]*model.Message, string, error)
	MarkReadWithOutbox(ctx context.Context, read *model.MessageRead, event *model.Event) (*model.MessageRead, error)
	DeleteMessageWithOutbox(ctx context.Context, conversationID, messageID, userID string, event *model.Event) error
	ClaimPendingOutboxEvents(ctx context.Context, processorID string, leaseDuration time.Duration, limit int) ([]*model.Event, error)
	MarkOutboxEventPublished(ctx context.Context, eventID string, processorID string) error
	RecordOutboxEventFailure(ctx context.Context, eventID string, processorID string, errMsg string) error
	Ping(ctx context.Context) error
}
