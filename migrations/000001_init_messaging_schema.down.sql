DROP INDEX IF EXISTS idx_message_reads_conv_user;
DROP INDEX IF EXISTS idx_conversation_members_user;
DROP INDEX IF EXISTS idx_messages_conversation_client_id;
DROP INDEX IF EXISTS idx_messages_conversation_created;

DROP TABLE IF EXISTS message_reads;
DROP TABLE IF EXISTS messages;
DROP TABLE IF EXISTS conversation_members;
DROP TABLE IF EXISTS conversations;
