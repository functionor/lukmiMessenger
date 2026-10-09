package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"github.com/lukmi/messaging-service/internal/config"
	"github.com/lukmi/messaging-service/internal/model"
)

type PostgresRepository struct {
	db *sql.DB
}

func NewPostgresRepository(cfg *config.Config) (*PostgresRepository, error) {
	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres connection: %w", err)
	}

	db.SetMaxOpenConns(cfg.DBMaxOpenConns)
	db.SetMaxIdleConns(cfg.DBMaxIdleConns)
	db.SetConnMaxLifetime(cfg.DBConnMaxLifetime)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	repo := &PostgresRepository{db: db}
	if err := repo.initTables(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize postgres tables: %w", err)
	}

	return repo, nil
}

func (p *PostgresRepository) initTables(ctx context.Context) error {
	query := `
	CREATE TABLE IF NOT EXISTS conversations (
		conversation_id VARCHAR(64) PRIMARY KEY,
		conversation_type VARCHAR(32) NOT NULL DEFAULT 'direct',
		created_by VARCHAR(64) NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS conversation_members (
		conversation_id VARCHAR(64) NOT NULL REFERENCES conversations(conversation_id) ON DELETE CASCADE,
		user_id VARCHAR(64) NOT NULL,
		joined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		left_at TIMESTAMPTZ,
		PRIMARY KEY (conversation_id, user_id)
	);

	CREATE TABLE IF NOT EXISTS messages (
		message_id VARCHAR(64) PRIMARY KEY,
		conversation_id VARCHAR(64) NOT NULL REFERENCES conversations(conversation_id) ON DELETE CASCADE,
		sender_id VARCHAR(64) NOT NULL,
		message_type VARCHAR(32) NOT NULL,
		text_content TEXT,
		media_id VARCHAR(128),
		reference_id VARCHAR(128),
		ciphertext TEXT,
		client_message_id VARCHAR(128),
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		deleted_at TIMESTAMPTZ
	);

	CREATE TABLE IF NOT EXISTS message_reads (
		message_id VARCHAR(64) NOT NULL REFERENCES messages(message_id) ON DELETE CASCADE,
		user_id VARCHAR(64) NOT NULL,
		read_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		conversation_id VARCHAR(64) NOT NULL REFERENCES conversations(conversation_id) ON DELETE CASCADE,
		PRIMARY KEY (message_id, user_id)
	);

	CREATE INDEX IF NOT EXISTS idx_messages_conversation_created ON messages (conversation_id, created_at DESC);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_conversation_client_id ON messages (conversation_id, client_message_id) WHERE client_message_id IS NOT NULL AND client_message_id != '';
	CREATE INDEX IF NOT EXISTS idx_conversation_members_user ON conversation_members (user_id) WHERE left_at IS NULL;
	CREATE INDEX IF NOT EXISTS idx_message_reads_conv_user ON message_reads (conversation_id, user_id);
	`
	_, err := p.db.ExecContext(ctx, query)
	return err
}

func (p *PostgresRepository) Ping(ctx context.Context) error {
	return p.db.PingContext(ctx)
}

func (p *PostgresRepository) CreateConversation(ctx context.Context, c *model.Conversation, userIDs []string) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	convQuery := `
		INSERT INTO conversations (conversation_id, conversation_type, created_by, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (conversation_id) DO NOTHING
	`
	res, err := tx.ExecContext(ctx, convQuery, c.ID, c.Type, c.CreatedBy, c.CreatedAt, c.UpdatedAt)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrAlreadyExists
	}

	memberQuery := `
		INSERT INTO conversation_members (conversation_id, user_id, joined_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (conversation_id, user_id) DO UPDATE SET left_at = NULL
	`
	for _, uid := range userIDs {
		if _, err := tx.ExecContext(ctx, memberQuery, c.ID, uid, c.CreatedAt); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (p *PostgresRepository) GetConversation(ctx context.Context, id string) (*model.Conversation, error) {
	query := `
		SELECT conversation_id, conversation_type, created_by, created_at, updated_at
		FROM conversations
		WHERE conversation_id = $1
	`
	row := p.db.QueryRowContext(ctx, query, id)
	var c model.Conversation
	if err := row.Scan(&c.ID, &c.Type, &c.CreatedBy, &c.CreatedAt, &c.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &c, nil
}

func (p *PostgresRepository) ListConversations(ctx context.Context, userID string, cursor string, limit int) ([]*model.Conversation, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}

	query := `
		SELECT c.conversation_id, c.conversation_type, c.created_by, c.created_at, c.updated_at
		FROM conversations c
		JOIN conversation_members cm ON c.conversation_id = cm.conversation_id
		WHERE cm.user_id = $1 AND cm.left_at IS NULL
		ORDER BY c.updated_at DESC
		LIMIT $2
	`
	rows, err := p.db.QueryContext(ctx, query, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var convs []*model.Conversation
	for rows.Next() {
		var c model.Conversation
		if err := rows.Scan(&c.ID, &c.Type, &c.CreatedBy, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, err
		}
		convs = append(convs, &c)
	}
	return convs, nil
}

func (p *PostgresRepository) Members(ctx context.Context, conversationID string) ([]string, error) {
	query := `
		SELECT user_id
		FROM conversation_members
		WHERE conversation_id = $1 AND left_at IS NULL
	`
	rows, err := p.db.QueryContext(ctx, query, conversationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []string
	for rows.Next() {
		var uid string
		if err := rows.Scan(&uid); err != nil {
			return nil, err
		}
		users = append(users, uid)
	}
	if len(users) == 0 {
		// Verify if conversation exists
		if _, err := p.GetConversation(ctx, conversationID); err != nil {
			return nil, err
		}
	}
	return users, nil
}

func (p *PostgresRepository) IsMember(ctx context.Context, conversationID, userID string) (bool, error) {
	query := `
		SELECT COUNT(1)
		FROM conversation_members
		WHERE conversation_id = $1 AND user_id = $2 AND left_at IS NULL
	`
	var count int
	if err := p.db.QueryRowContext(ctx, query, conversationID, userID).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func (p *PostgresRepository) AddMember(ctx context.Context, conversationID, userID string) error {
	query := `
		INSERT INTO conversation_members (conversation_id, user_id, joined_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (conversation_id, user_id) DO UPDATE SET left_at = NULL, joined_at = NOW()
	`
	_, err := p.db.ExecContext(ctx, query, conversationID, userID)
	return err
}

func (p *PostgresRepository) LeaveConversation(ctx context.Context, conversationID, userID string) error {
	query := `
		UPDATE conversation_members
		SET left_at = NOW()
		WHERE conversation_id = $1 AND user_id = $2 AND left_at IS NULL
	`
	res, err := p.db.ExecContext(ctx, query, conversationID, userID)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrNotMember
	}
	return nil
}

func (p *PostgresRepository) CreateMessage(ctx context.Context, m *model.Message) (*model.Message, error) {
	// If client_message_id is supplied, check for existing idempotency match
	if m.ClientID != "" {
		existing, err := p.GetMessageByClientID(ctx, m.ConversationID, m.ClientID)
		if err == nil && existing != nil {
			return existing, ErrAlreadyExists
		}
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	msgQuery := `
		INSERT INTO messages (
			message_id, conversation_id, sender_id, message_type,
			text_content, media_id, reference_id, ciphertext,
			client_message_id, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`
	var clientIDNull *string
	if m.ClientID != "" {
		clientIDNull = &m.ClientID
	}

	_, err = tx.ExecContext(ctx, msgQuery,
		m.ID, m.ConversationID, m.SenderID, string(m.Type),
		nilIfEmpty(m.TextContent), nilIfEmpty(m.MediaID), nilIfEmpty(m.ReferenceID), nilIfEmpty(m.Ciphertext),
		clientIDNull, m.CreatedAt, m.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	// Update conversation updated_at timestamp
	_, _ = tx.ExecContext(ctx, `UPDATE conversations SET updated_at = $1 WHERE conversation_id = $2`, m.CreatedAt, m.ConversationID)

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return m, nil
}

func (p *PostgresRepository) GetMessageByID(ctx context.Context, conversationID, messageID string) (*model.Message, error) {
	query := `
		SELECT message_id, conversation_id, sender_id, message_type,
		       COALESCE(text_content, ''), COALESCE(media_id, ''), COALESCE(reference_id, ''), COALESCE(ciphertext, ''),
		       COALESCE(client_message_id, ''), created_at, updated_at, deleted_at
		FROM messages
		WHERE conversation_id = $1 AND message_id = $2
	`
	row := p.db.QueryRowContext(ctx, query, conversationID, messageID)
	return scanMessage(row)
}

func (p *PostgresRepository) GetMessageByClientID(ctx context.Context, conversationID, clientID string) (*model.Message, error) {
	query := `
		SELECT message_id, conversation_id, sender_id, message_type,
		       COALESCE(text_content, ''), COALESCE(media_id, ''), COALESCE(reference_id, ''), COALESCE(ciphertext, ''),
		       COALESCE(client_message_id, ''), created_at, updated_at, deleted_at
		FROM messages
		WHERE conversation_id = $1 AND client_message_id = $2
	`
	row := p.db.QueryRowContext(ctx, query, conversationID, clientID)
	return scanMessage(row)
}

func (p *PostgresRepository) ListMessages(ctx context.Context, conversationID string, cursor string, limit int) ([]*model.Message, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}

	var rows *sql.Rows
	var err error

	if cursor != "" {
		query := `
			SELECT message_id, conversation_id, sender_id, message_type,
			       COALESCE(text_content, ''), COALESCE(media_id, ''), COALESCE(reference_id, ''), COALESCE(ciphertext, ''),
			       COALESCE(client_message_id, ''), created_at, updated_at, deleted_at
			FROM messages
			WHERE conversation_id = $1 AND created_at < (SELECT created_at FROM messages WHERE message_id = $2)
			ORDER BY created_at DESC
			LIMIT $3
		`
		rows, err = p.db.QueryContext(ctx, query, conversationID, cursor, limit)
	} else {
		query := `
			SELECT message_id, conversation_id, sender_id, message_type,
			       COALESCE(text_content, ''), COALESCE(media_id, ''), COALESCE(reference_id, ''), COALESCE(ciphertext, ''),
			       COALESCE(client_message_id, ''), created_at, updated_at, deleted_at
			FROM messages
			WHERE conversation_id = $1
			ORDER BY created_at DESC
			LIMIT $2
		`
		rows, err = p.db.QueryContext(ctx, query, conversationID, limit)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []*model.Message
	for rows.Next() {
		m, err := scanMessageFromRows(rows)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}
	return msgs, nil
}

func (p *PostgresRepository) MarkRead(ctx context.Context, read *model.MessageRead) (*model.MessageRead, error) {
	query := `
		INSERT INTO message_reads (message_id, user_id, read_at, conversation_id)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (message_id, user_id) DO UPDATE SET read_at = EXCLUDED.read_at
	`
	_, err := p.db.ExecContext(ctx, query, read.MessageID, read.UserID, read.ReadAt, read.ConversationID)
	if err != nil {
		return nil, err
	}
	return read, nil
}

func (p *PostgresRepository) DeleteMessage(ctx context.Context, conversationID, messageID, userID string) error {
	query := `
		UPDATE messages
		SET deleted_at = NOW()
		WHERE conversation_id = $1 AND message_id = $2 AND sender_id = $3 AND deleted_at IS NULL
	`
	res, err := p.db.ExecContext(ctx, query, conversationID, messageID, userID)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		// Check if message exists or user is not sender
		m, getErr := p.GetMessageByID(ctx, conversationID, messageID)
		if getErr != nil {
			return ErrNotFound
		}
		if m.SenderID != userID {
			return ErrUnauthorized
		}
		if m.DeletedAt != nil {
			return nil // Already deleted
		}
		return ErrNotFound
	}
	return nil
}

func scanMessage(row *sql.Row) (*model.Message, error) {
	var m model.Message
	var msgType string
	if err := row.Scan(&m.ID, &m.ConversationID, &m.SenderID, &msgType, &m.TextContent, &m.MediaID, &m.ReferenceID, &m.Ciphertext, &m.ClientID, &m.CreatedAt, &m.UpdatedAt, &m.DeletedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	m.Type = model.MessageType(msgType)
	return &m, nil
}

func scanMessageFromRows(rows *sql.Rows) (*model.Message, error) {
	var m model.Message
	var msgType string
	if err := rows.Scan(&m.ID, &m.ConversationID, &m.SenderID, &msgType, &m.TextContent, &m.MediaID, &m.ReferenceID, &m.Ciphertext, &m.ClientID, &m.CreatedAt, &m.UpdatedAt, &m.DeletedAt); err != nil {
		return nil, err
	}
	m.Type = model.MessageType(msgType)
	return &m, nil
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
