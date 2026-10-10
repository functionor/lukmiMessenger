package repository

import (
	"context"
	"sync"
	"time"

	"github.com/lukmi/messaging-service/internal/model"
)

type MemoryRepository struct {
	mu              sync.RWMutex
	conversations   map[string]*model.Conversation
	members         map[string]map[string]*model.ConversationMember
	messages        map[string][]*model.Message
	clientIDs       map[string]*model.Message
	reads           map[string]*model.MessageRead
	outboxEvents    map[string]*model.Event
	outboxStatus    map[string]string
	outboxTokens    map[string]string
	processedEvents map[string]bool
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		conversations:   map[string]*model.Conversation{},
		members:         map[string]map[string]*model.ConversationMember{},
		messages:        map[string][]*model.Message{},
		clientIDs:       map[string]*model.Message{},
		reads:           map[string]*model.MessageRead{},
		outboxEvents:    map[string]*model.Event{},
		outboxStatus:    map[string]string{},
		outboxTokens:    map[string]string{},
		processedEvents: map[string]bool{},
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

func (r *MemoryRepository) ListConversations(_ context.Context, userID, cursor string, limit int) ([]*model.Conversation, string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if limit <= 0 || limit > 100 {
		limit = 50
	}

	var cursorTime time.Time
	var cursorID string
	var err error
	if cursor != "" {
		cursorTime, cursorID, err = DecodeCursor(cursor)
		if err != nil {
			return nil, "", ErrInvalidCursor
		}
	}

	var candidates []*model.Conversation
	for id, c := range r.conversations {
		if m, ok := r.members[id][userID]; ok && m.LeftAt == nil {
			if !cursorTime.IsZero() {
				if c.UpdatedAt.Before(cursorTime) || (c.UpdatedAt.Equal(cursorTime) && c.ID < cursorID) {
					candidates = append(candidates, c)
				}
			} else {
				candidates = append(candidates, c)
			}
		}
	}

	out := candidates
	if len(out) > limit {
		out = out[:limit]
	}

	var nextCursor string
	if len(out) == limit {
		last := out[len(out)-1]
		nextCursor = EncodeCursor(last.UpdatedAt, last.ID)
	}

	return out, nextCursor, nil
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

func (r *MemoryRepository) CreateMessageWithOutbox(_ context.Context, m *model.Message, event *model.Event) (*model.Message, error) {
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

	if event != nil {
		r.outboxEvents[event.ID] = event
		r.outboxStatus[event.ID] = "pending"
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

func (r *MemoryRepository) ListMessages(_ context.Context, conversationID, cursor string, limit int) ([]*model.Message, string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if limit <= 0 || limit > 100 {
		limit = 50
	}

	var cursorTime time.Time
	var cursorID string
	var err error
	if cursor != "" {
		cursorTime, cursorID, err = DecodeCursor(cursor)
		if err != nil {
			return nil, "", ErrInvalidCursor
		}
	}

	all := r.messages[conversationID]
	var candidates []*model.Message

	for i := len(all) - 1; i >= 0; i-- {
		msg := all[i]
		if !cursorTime.IsZero() {
			if msg.CreatedAt.Before(cursorTime) || (msg.CreatedAt.Equal(cursorTime) && msg.ID < cursorID) {
				candidates = append(candidates, msg)
			}
		} else {
			candidates = append(candidates, msg)
		}
	}

	out := candidates
	if len(out) > limit {
		out = out[:limit]
	}

	var nextCursor string
	if len(out) == limit {
		last := out[len(out)-1]
		nextCursor = EncodeCursor(last.CreatedAt, last.ID)
	}

	return out, nextCursor, nil
}

func (r *MemoryRepository) MarkReadWithOutbox(_ context.Context, read *model.MessageRead, event *model.Event) (*model.MessageRead, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	found := false
	for _, m := range r.messages[read.ConversationID] {
		if m.ID == read.MessageID {
			found = true
			break
		}
	}
	if !found {
		return nil, ErrNotFound
	}

	key := read.MessageID + ":" + read.UserID
	if existing, ok := r.reads[key]; ok {
		return existing, nil
	}
	r.reads[key] = read

	if event != nil {
		r.outboxEvents[event.ID] = event
		r.outboxStatus[event.ID] = "pending"
	}

	return read, nil
}

func (r *MemoryRepository) DeleteMessageWithOutbox(_ context.Context, conversationID, messageID, userID string, event *model.Event) error {
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

			if event != nil {
				r.outboxEvents[event.ID] = event
				r.outboxStatus[event.ID] = "pending"
			}
			return nil
		}
	}
	return ErrNotFound
}

func (r *MemoryRepository) ClaimPendingOutboxEvents(_ context.Context, processorID string, claimToken string, _ time.Duration, limit int) ([]*model.Event, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var pending []*model.Event
	for id, evt := range r.outboxEvents {
		if r.outboxStatus[id] == "pending" {
			pending = append(pending, evt)
			r.outboxStatus[id] = "processing"
			r.outboxTokens[id] = claimToken
			if len(pending) == limit {
				break
			}
		}
	}
	return pending, nil
}

func (r *MemoryRepository) MarkOutboxEventPublished(_ context.Context, eventID string, claimToken string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.outboxTokens[eventID] != claimToken {
		return ErrStaleClaim
	}
	r.outboxStatus[eventID] = "published"
	return nil
}

func (r *MemoryRepository) RecordOutboxEventFailure(_ context.Context, eventID string, claimToken string, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.outboxTokens[eventID] != claimToken {
		return ErrStaleClaim
	}
	r.outboxStatus[eventID] = "pending"
	return nil
}

func (r *MemoryRepository) IsEventProcessed(_ context.Context, eventID string) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.processedEvents[eventID], nil
}

func (r *MemoryRepository) MarkEventProcessed(_ context.Context, eventID string, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.processedEvents[eventID] = true
	return nil
}
