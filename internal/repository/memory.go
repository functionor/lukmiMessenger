package repository

import (
	"context"
	"sync"
	"time"

	"github.com/lukmi/messaging-service/internal/model"
)

type MemoryRepository struct {
	mu            sync.RWMutex
	conversations map[string]*model.Conversation
	members       map[string]map[string]*model.ConversationMember
	messages      map[string][]*model.Message
	clientIDs     map[string]*model.Message
	reads         map[string]*model.MessageRead
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		conversations: map[string]*model.Conversation{},
		members:       map[string]map[string]*model.ConversationMember{},
		messages:      map[string][]*model.Message{},
		clientIDs:     map[string]*model.Message{},
		reads:         map[string]*model.MessageRead{},
	}
}

func (r *MemoryRepository) Ping(_ context.Context) error {
	return nil
}

func (r *MemoryRepository) CreateConversation(_ context.Context, c *model.Conversation, userIDs []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.conversations[c.ID]; ok {
		return ErrAlreadyExists
	}
	r.conversations[c.ID] = c
	r.members[c.ID] = map[string]*model.ConversationMember{}
	for _, uid := range userIDs {
		r.members[c.ID][uid] = &model.ConversationMember{
			ConversationID: c.ID,
			UserID:         uid,
			JoinedAt:       c.CreatedAt,
		}
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

func (r *MemoryRepository) ListConversations(_ context.Context, userID, cursor string, limit int) ([]*model.Conversation, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := []*model.Conversation{}
	for id, c := range r.conversations {
		if m, ok := r.members[id][userID]; ok && m.LeftAt == nil {
			out = append(out, c)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

func (r *MemoryRepository) Members(_ context.Context, conversationID string) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	users, ok := r.members[conversationID]
	if !ok {
		return nil, ErrNotFound
	}

	var out []string
	for uid, m := range users {
		if m.LeftAt == nil {
			out = append(out, uid)
		}
	}
	return out, nil
}

func (r *MemoryRepository) IsMember(_ context.Context, conversationID, userID string) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	users, ok := r.members[conversationID]
	if !ok {
		return false, ErrNotFound
	}
	m, ok := users[userID]
	return ok && m.LeftAt == nil, nil
}

func (r *MemoryRepository) AddMember(_ context.Context, conversationID, userID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	users, ok := r.members[conversationID]
	if !ok {
		return ErrNotFound
	}
	users[userID] = &model.ConversationMember{
		ConversationID: conversationID,
		UserID:         userID,
		JoinedAt:       time.Now().UTC(),
	}
	return nil
}

func (r *MemoryRepository) LeaveConversation(_ context.Context, conversationID, userID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	users, ok := r.members[conversationID]
	if !ok {
		return ErrNotFound
	}
	m, ok := users[userID]
	if !ok || m.LeftAt != nil {
		return ErrNotMember
	}
	now := time.Now().UTC()
	m.LeftAt = &now
	return nil
}

func (r *MemoryRepository) CreateMessage(_ context.Context, m *model.Message) (*model.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if m.ClientID != "" {
		key := m.ConversationID + ":" + m.ClientID
		if existing, ok := r.clientIDs[key]; ok {
			return existing, ErrAlreadyExists
		}
		r.clientIDs[key] = m
	}

	r.messages[m.ConversationID] = append(r.messages[m.ConversationID], m)
	if c, ok := r.conversations[m.ConversationID]; ok {
		c.UpdatedAt = m.CreatedAt
	}
	return m, nil
}

func (r *MemoryRepository) GetMessageByID(_ context.Context, conversationID, messageID string) (*model.Message, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, msg := range r.messages[conversationID] {
		if msg.ID == messageID {
			return msg, nil
		}
	}
	return nil, ErrNotFound
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

func (r *MemoryRepository) ListMessages(_ context.Context, conversationID, cursor string, limit int) ([]*model.Message, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	all := r.messages[conversationID]
	if len(all) == 0 {
		return []*model.Message{}, nil
	}

	startIndex := len(all)
	if cursor != "" {
		for i, msg := range all {
			if msg.ID == cursor {
				startIndex = i
				break
			}
		}
	}

	if limit <= 0 || limit > 100 {
		limit = 50
	}

	start := startIndex - limit
	if start < 0 {
		start = 0
	}

	result := make([]*model.Message, 0, startIndex-start)
	for i := startIndex - 1; i >= start; i-- {
		result = append(result, all[i])
	}

	return result, nil
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

func (r *MemoryRepository) DeleteMessage(_ context.Context, conversationID, messageID, userID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, m := range r.messages[conversationID] {
		if m.ID == messageID {
			if m.SenderID != userID {
				return ErrUnauthorized
			}
			if m.DeletedAt != nil {
				return nil
			}
			now := time.Now().UTC()
			m.DeletedAt = &now
			return nil
		}
	}
	return ErrNotFound
}
