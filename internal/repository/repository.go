package repository

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/lukmi/messaging-service/internal/model"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
	ErrNotMember     = errors.New("not a conversation member")
)

type Repository interface {
	CreateConversation(context.Context, *model.Conversation, []string) error
	GetConversation(context.Context, string) (*model.Conversation, error)
	ListConversations(context.Context, string, string, int) ([]*model.Conversation, error)
	Members(context.Context, string) ([]string, error)
	IsMember(context.Context, string, string) (bool, error)
	CreateMessage(context.Context, *model.Message) (*model.Message, error)
	GetMessageByClientID(context.Context, string, string) (*model.Message, error)
	ListMessages(context.Context, string, string, int) ([]*model.Message, error)
	MarkRead(context.Context, *model.MessageRead) (*model.MessageRead, error)
	DeleteMessage(context.Context, string, string) error
}

// MemoryRepository keeps local development and unit tests runnable. Production uses PostgreSQL.
type MemoryRepository struct {
	mu            sync.RWMutex
	conversations map[string]*model.Conversation
	members       map[string]map[string]bool
	messages      map[string][]*model.Message
	clientIDs     map[string]*model.Message
	reads         map[string]*model.MessageRead
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{conversations: map[string]*model.Conversation{}, members: map[string]map[string]bool{}, messages: map[string][]*model.Message{}, clientIDs: map[string]*model.Message{}, reads: map[string]*model.MessageRead{}}
}
func (r *MemoryRepository) CreateConversation(_ context.Context, c *model.Conversation, users []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.conversations[c.ID]; ok {
		return ErrAlreadyExists
	}
	r.conversations[c.ID] = c
	r.members[c.ID] = map[string]bool{}
	for _, user := range users {
		r.members[c.ID][user] = true
	}
	return nil
}
func (r *MemoryRepository) GetConversation(_ context.Context, id string) (*model.Conversation, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.conversations[id]
	if !ok {
		return nil, ErrNotFound
	}
	return c, nil
}
func (r *MemoryRepository) ListConversations(_ context.Context, userID, _ string, limit int) ([]*model.Conversation, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := []*model.Conversation{}
	for id, c := range r.conversations {
		if r.members[id][userID] {
			out = append(out, c)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}
func (r *MemoryRepository) Members(_ context.Context, id string) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	users, ok := r.members[id]
	if !ok {
		return nil, ErrNotFound
	}
	out := []string{}
	for user := range users {
		out = append(out, user)
	}
	return out, nil
}
func (r *MemoryRepository) IsMember(_ context.Context, id, user string) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	users, ok := r.members[id]
	if !ok {
		return false, ErrNotFound
	}
	return users[user], nil
}
func (r *MemoryRepository) CreateMessage(_ context.Context, m *model.Message) (*model.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := m.ConversationID + ":" + m.ClientID
	if m.ClientID != "" {
		if existing, ok := r.clientIDs[key]; ok {
			return existing, ErrAlreadyExists
		}
	}
	r.messages[m.ConversationID] = append(r.messages[m.ConversationID], m)
	if m.ClientID != "" {
		r.clientIDs[key] = m
	}
	return m, nil
}
func (r *MemoryRepository) GetMessageByClientID(_ context.Context, conversationID, clientID string) (*model.Message, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, ok := r.clientIDs[conversationID+":"+clientID]
	if !ok {
		return nil, ErrNotFound
	}
	return m, nil
}
func (r *MemoryRepository) ListMessages(_ context.Context, id, _ string, limit int) ([]*model.Message, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	all := r.messages[id]
	if len(all) > limit {
		all = all[len(all)-limit:]
	}
	return all, nil
}
func (r *MemoryRepository) MarkRead(_ context.Context, read *model.MessageRead) (*model.MessageRead, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := read.MessageID + ":" + read.UserID
	if existing, ok := r.reads[key]; ok {
		return existing, nil
	}
	r.reads[key] = read
	return read, nil
}
func (r *MemoryRepository) DeleteMessage(_ context.Context, id, userID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range r.messages[id] {
		if m.SenderID == userID && m.DeletedAt == nil {
			now := time.Now().UTC()
			m.DeletedAt = &now
			return nil
		}
	}
	return ErrNotFound
}
