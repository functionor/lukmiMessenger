-- Central Lukmi Database Schema Migration for Messaging Service
-- Owner Repository: https://github.com/functionor/lukmi_database
-- Destination Path: migrations/000001_init_messaging_schema.up.sql

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

-- Compound Keyset Pagination & Idempotency Indexes
CREATE INDEX IF NOT EXISTS idx_messages_conv_created_id ON messages (conversation_id, created_at DESC, message_id DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_conversation_client_id ON messages (conversation_id, client_message_id) WHERE client_message_id IS NOT NULL AND client_message_id != '';
CREATE INDEX IF NOT EXISTS idx_conversation_members_user ON conversation_members (user_id) WHERE left_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_message_reads_conv_user ON message_reads (conversation_id, user_id);
