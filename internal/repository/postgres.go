package repository

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
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
		_ = db.Close()
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	return &PostgresRepository{db: db}, nil
}

func (p *PostgresRepository) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
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

func (p *PostgresRepository) ListConversations(ctx context.Context, userID string, cursor string, limit int) ([]*model.Conversation, string, error) {
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

	var rows *sql.Rows
	if !cursorTime.IsZero() {
		query := `
			SELECT c.conversation_id, c.conversation_type, c.created_by, c.created_at, c.updated_at
			FROM conversations c
			JOIN conversation_members cm ON c.conversation_id = cm.conversation_id
			WHERE cm.user_id = $1 AND cm.left_at IS NULL
			  AND (c.updated_at, c.conversation_id) < ($2, $3)
			ORDER BY c.updated_at DESC, c.conversation_id DESC
			LIMIT $4
		`
		rows, err = p.db.QueryContext(ctx, query, userID, cursorTime, cursorID, limit)
	} else {
		query := `
			SELECT c.conversation_id, c.conversation_type, c.created_by, c.created_at, c.updated_at
			FROM conversations c
			JOIN conversation_members cm ON c.conversation_id = cm.conversation_id
			WHERE cm.user_id = $1 AND cm.left_at IS NULL
			ORDER BY c.updated_at DESC, c.conversation_id DESC
			LIMIT $2
		`
		rows, err = p.db.QueryContext(ctx, query, userID, limit)
	}

	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var convs []*model.Conversation
	for rows.Next() {
		var c model.Conversation
		if err := rows.Scan(&c.ID, &c.Type, &c.CreatedBy, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, "", err
		}
		convs = append(convs, &c)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	var nextCursor string
	if len(convs) == limit {
		last := convs[len(convs)-1]
		nextCursor = EncodeCursor(last.UpdatedAt, last.ID)
	}

	return convs, nextCursor, nil
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
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(users) == 0 {
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

func (p *PostgresRepository) CreateMessageWithOutbox(ctx context.Context, m *model.Message, event *model.Event) (*model.Message, error) {
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

	_, _ = tx.ExecContext(ctx, `UPDATE conversations SET updated_at = $1 WHERE conversation_id = $2`, m.CreatedAt, m.ConversationID)

	if event != nil {
		payloadBytes, err := json.Marshal(event)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal outbox event payload: %w", err)
		}

		outboxQuery := `
			INSERT INTO outbox_events (event_id, event_type, aggregate_type, aggregate_id, payload, status, created_at)
			VALUES ($1, $2, $3, $4, $5, 'pending', $6)
		`
		_, err = tx.ExecContext(ctx, outboxQuery, event.ID, string(event.Type), "message", m.ID, payloadBytes, event.OccurredAt)
		if err != nil {
			return nil, fmt.Errorf("failed to insert outbox event: %w", err)
		}
	}

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

func (p *PostgresRepository) ListMessages(ctx context.Context, conversationID string, cursor string, limit int) ([]*model.Message, string, error) {
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

	var rows *sql.Rows

	if !cursorTime.IsZero() {
		query := `
			SELECT message_id, conversation_id, sender_id, message_type,
			       COALESCE(text_content, ''), COALESCE(media_id, ''), COALESCE(reference_id, ''), COALESCE(ciphertext, ''),
			       COALESCE(client_message_id, ''), created_at, updated_at, deleted_at
			FROM messages
			WHERE conversation_id = $1 AND (created_at, message_id) < ($2, $3)
			ORDER BY created_at DESC, message_id DESC
			LIMIT $4
		`
		rows, err = p.db.QueryContext(ctx, query, conversationID, cursorTime, cursorID, limit)
	} else {
		query := `
			SELECT message_id, conversation_id, sender_id, message_type,
			       COALESCE(text_content, ''), COALESCE(media_id, ''), COALESCE(reference_id, ''), COALESCE(ciphertext, ''),
			       COALESCE(client_message_id, ''), created_at, updated_at, deleted_at
			FROM messages
			WHERE conversation_id = $1
			ORDER BY created_at DESC, message_id DESC
			LIMIT $2
		`
		rows, err = p.db.QueryContext(ctx, query, conversationID, limit)
	}

	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var msgs []*model.Message
	for rows.Next() {
		m, err := scanMessageFromRows(rows)
		if err != nil {
			return nil, "", err
		}
		msgs = append(msgs, m)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	var nextCursor string
	if len(msgs) == limit {
		last := msgs[len(msgs)-1]
		nextCursor = EncodeCursor(last.CreatedAt, last.ID)
	}

	return msgs, nextCursor, nil
}

func (p *PostgresRepository) MarkReadWithOutbox(ctx context.Context, read *model.MessageRead, event *model.Event) (*model.MessageRead, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var exists int
	checkQuery := `SELECT 1 FROM messages WHERE message_id = $1 AND conversation_id = $2`
	if err := tx.QueryRowContext(ctx, checkQuery, read.MessageID, read.ConversationID).Scan(&exists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	query := `
		INSERT INTO message_reads (message_id, user_id, read_at, conversation_id)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (message_id, user_id) DO UPDATE SET read_at = EXCLUDED.read_at
	`
	if _, err := tx.ExecContext(ctx, query, read.MessageID, read.UserID, read.ReadAt, read.ConversationID); err != nil {
		return nil, err
	}

	if event != nil {
		payloadBytes, err := json.Marshal(event)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal outbox event: %w", err)
		}
		outboxQuery := `
			INSERT INTO outbox_events (event_id, event_type, aggregate_type, aggregate_id, payload, status, created_at)
			VALUES ($1, $2, 'message_read', $3, $4, 'pending', $5)
		`
		if _, err := tx.ExecContext(ctx, outboxQuery, event.ID, string(event.Type), read.MessageID, payloadBytes, event.OccurredAt); err != nil {
			return nil, fmt.Errorf("failed to insert read outbox event: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return read, nil
}

func (p *PostgresRepository) DeleteMessageWithOutbox(ctx context.Context, conversationID, messageID, userID string, event *model.Event) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	query := `
		UPDATE messages
		SET deleted_at = NOW()
		WHERE conversation_id = $1 AND message_id = $2 AND sender_id = $3 AND deleted_at IS NULL
	`
	res, err := tx.ExecContext(ctx, query, conversationID, messageID, userID)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		m, getErr := p.GetMessageByID(ctx, conversationID, messageID)
		if getErr != nil {
			return ErrNotFound
		}
		if m.SenderID != userID {
			return ErrUnauthorized
		}
		if m.DeletedAt != nil {
			return nil
		}
		return ErrNotFound
	}

	if event != nil {
		payloadBytes, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("failed to marshal delete outbox event: %w", err)
		}
		outboxQuery := `
			INSERT INTO outbox_events (event_id, event_type, aggregate_type, aggregate_id, payload, status, created_at)
			VALUES ($1, $2, 'message_deleted', $3, $4, 'pending', $5)
		`
		if _, err := tx.ExecContext(ctx, outboxQuery, event.ID, string(event.Type), messageID, payloadBytes, event.OccurredAt); err != nil {
			return fmt.Errorf("failed to insert delete outbox event: %w", err)
		}
	}

	return tx.Commit()
}

func (p *PostgresRepository) ClaimPendingOutboxEvents(ctx context.Context, processorID string, leaseDuration time.Duration, limit int) ([]*model.Event, error) {
	if limit <= 0 {
		limit = 50
	}
	leaseSeconds := fmt.Sprintf("%d seconds", int(leaseDuration.Seconds()))

	query := `
		UPDATE outbox_events
		SET status = 'processing',
		    processor_id = $1,
		    locked_until = NOW() + $2::interval
		WHERE event_id IN (
			SELECT event_id
			FROM outbox_events
			WHERE status = 'pending' OR (status = 'processing' AND (locked_until IS NULL OR locked_until < NOW()))
			ORDER BY created_at ASC
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		)
		RETURNING payload
	`
	rows, err := p.db.QueryContext(ctx, query, processorID, leaseSeconds, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*model.Event
	for rows.Next() {
		var payloadBytes []byte
		if err := rows.Scan(&payloadBytes); err != nil {
			return nil, err
		}
		var evt model.Event
		if err := json.Unmarshal(payloadBytes, &evt); err == nil {
			events = append(events, &evt)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

func (p *PostgresRepository) MarkOutboxEventPublished(ctx context.Context, eventID string, processorID string) error {
	query := `
		UPDATE outbox_events
		SET status = 'published', published_at = NOW(), locked_until = NULL
		WHERE event_id = $1 AND processor_id = $2
	`
	res, err := p.db.ExecContext(ctx, query, eventID, processorID)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("outbox event %s not found or processor mismatch", eventID)
	}
	return nil
}

func (p *PostgresRepository) RecordOutboxEventFailure(ctx context.Context, eventID string, processorID string, errMsg string) error {
	query := `
		UPDATE outbox_events
		SET status = 'pending', retry_count = retry_count + 1, last_error = $3, locked_until = NULL
		WHERE event_id = $1 AND processor_id = $2
	`
	res, err := p.db.ExecContext(ctx, query, eventID, processorID, errMsg)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("outbox event %s failure record failed", eventID)
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

func EncodeCursor(t time.Time, id string) string {
	if t.IsZero() || id == "" {
		return ""
	}
	raw := fmt.Sprintf("%d:%s", t.UnixNano(), id)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func DecodeCursor(cursor string) (time.Time, string, error) {
	if cursor == "" {
		return time.Time{}, "", nil
	}
	decodedBytes, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("invalid base64 cursor: %w", err)
	}
	parts := strings.SplitN(string(decodedBytes), ":", 2)
	if len(parts) != 2 {
		return time.Time{}, "", fmt.Errorf("invalid cursor format")
	}
	nanos, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("invalid cursor timestamp")
	}
	return time.Unix(0, nanos).UTC(), parts[1], nil
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
