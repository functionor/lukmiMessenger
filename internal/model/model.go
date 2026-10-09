package model

import "time"

type MessageType string

const (
	Text         MessageType = "TEXT"
	Image        MessageType = "IMAGE"
	Video        MessageType = "VIDEO"
	Audio        MessageType = "AUDIO"
	File         MessageType = "FILE"
	PostShare    MessageType = "POST_SHARE"
	ProfileShare MessageType = "PROFILE_SHARE"
	StoryShare   MessageType = "STORY_SHARE"
)

func (t MessageType) Valid() bool {
	switch t {
	case Text, Image, Video, Audio, File, PostShare, ProfileShare, StoryShare:
		return true
	}
	return false
}

type Conversation struct {
	ID        string    `json:"conversation_id"`
	Type      string    `json:"conversation_type"`
	CreatedBy string    `json:"created_by"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}
type Message struct {
	ID             string      `json:"message_id"`
	ConversationID string      `json:"conversation_id"`
	SenderID       string      `json:"sender_id"`
	Type           MessageType `json:"message_type"`
	TextContent    string      `json:"text_content,omitempty"`
	Ciphertext     string      `json:"ciphertext,omitempty"`
	MediaID        string      `json:"media_id,omitempty"`
	ReferenceID    string      `json:"reference_id,omitempty"`
	ClientID       string      `json:"client_message_id,omitempty"`
	CreatedAt      time.Time   `json:"created_at"`
	UpdatedAt      time.Time   `json:"updated_at"`
	DeletedAt      *time.Time  `json:"deleted_at,omitempty"`
}
type MessageRead struct {
	MessageID      string    `json:"message_id"`
	ConversationID string    `json:"conversation_id"`
	UserID         string    `json:"user_id"`
	ReadAt         time.Time `json:"read_at"`
}
