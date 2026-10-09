package repository

import (
	"context"
	"errors"

	"github.com/lukmi/messaging-service/internal/model"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
	ErrNotMember     = errors.New("not a conversation member")
	ErrUnauthorized  = errors.New("unauthorized action on message")
)

type Repository interface {
	CreateConversation(ctx context.Context, c *model.Conversation, userIDs []string) error
	GetConversation(ctx context.Context, id string) (*model.Conversation, error)
	ListConversations(ctx context.Context, userID string, cursor string, limit int) ([]*model.Conversation, error)
	Members(ctx context.Context, conversationID string) ([]string, error)
	IsMember(ctx context.Context, conversationID, userID string) (bool, error)
	AddMember(ctx context.Context, conversationID, userID string) error
	LeaveConversation(ctx context.Context, conversationID, userID string) error
	CreateMessage(ctx context.Context, m *model.Message) (*model.Message, error)
	GetMessageByID(ctx context.Context, conversationID, messageID string) (*model.Message, error)
	GetMessageByClientID(ctx context.Context, conversationID, clientID string) (*model.Message, error)
	ListMessages(ctx context.Context, conversationID string, cursor string, limit int) ([]*model.Message, error)
	MarkRead(ctx context.Context, read *model.MessageRead) (*model.MessageRead, error)
	DeleteMessage(ctx context.Context, conversationID, messageID, userID string) error
	Ping(ctx context.Context) error
}
