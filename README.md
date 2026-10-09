# Lukmi Messaging Service Backend

Production-oriented backend microservice for the **Lukmi Messaging Service** written in **Go**.

The Messaging Service is an internal microservice located behind the Public API Gateway. It owns conversations, members, messages, message delivery states, read receipts, WebSocket real-time communication, and message synchronization.

---

## 1. Architecture & Boundaries

```text
Flutter App
     │
     │ HTTPS / WebSocket
     ▼
Public API Gateway
     │
     ▼
Messaging Service
     │
     ├── PostgreSQL (Durable Store)
     ├── Redis (Transient State & WS Pub/Sub)
     └── Kafka/Redpanda (Async Events)
```

### Critical Boundaries
- **Content / User / Story isolation**: When users share a post (`POST_SHARE`), profile (`PROFILE_SHARE`), or story (`STORY_SHARE`), the Messaging Service stores only the `reference_id` (resource UUID). It **never queries** Content Service, User Service, or Story Service to fetch full resource details.
- **Media**: Messages with media (`IMAGE`, `VIDEO`, `AUDIO`, `FILE`) store `media_id`. Media binaries remain owned by Media Service / Cloudflare R2.
- **E2EE Support**: The service accepts client-encrypted `ciphertext` without needing plaintext message content or managing private keys.
- **Notifications**: FCM/APNs push notification delivery is owned by the Notification Service. Messaging Service publishes asynchronous Kafka events (`MESSAGE_SENT`, `MESSAGE_READ`, `MESSAGE_DELETED`) consumed by Notification Service.

---

## 2. Supported Message Types

- `TEXT`
- `IMAGE`
- `VIDEO`
- `AUDIO`
- `FILE`
- `POST_SHARE` (requires `reference_id = post_uuid`)
- `PROFILE_SHARE` (requires `reference_id = user_uuid`)
- `STORY_SHARE` (requires `reference_id = story_uuid`)

---

## 3. Database Schema & Migration

The service connects to PostgreSQL and uses the central database schema:

- `conversations`
- `conversation_members`
- `messages` (extended with `reference_id`, `ciphertext`, `client_message_id`)
- `message_reads`

Migration scripts are located in `migrations/000001_init_messaging_schema.up.sql`.

---

## 4. API Endpoints

### Health & Readiness
- `GET /healthz` - Basic liveness probe
- `GET /readyz` - Readiness probe (checks PostgreSQL & Redis connectivity)

### Real-Time WebSocket
- `GET /api/v1/ws` - Authenticated WebSocket endpoint supporting multi-device real-time event delivery (`message.new`, `message.read`, `message.deleted`)

### Conversations
- `POST /api/v1/conversations` - Create a conversation (`conversation_type`, `member_ids`)
- `GET /api/v1/conversations` - List active conversations for the authenticated user
- `GET /api/v1/conversations/{conversation_id}` - Get conversation details
- `GET /api/v1/conversations/{conversation_id}/members` - Get active conversation members
- `POST /api/v1/conversations/{conversation_id}/leave` - Leave conversation

### Messages
- `POST /api/v1/conversations/{conversation_id}/messages` - Send message (text, media, post/profile/story share, or E2EE ciphertext with client idempotency key)
- `GET /api/v1/conversations/{conversation_id}/messages` - Sync messages with cursor/limit pagination
- `POST /api/v1/conversations/{conversation_id}/messages/{message_id}/read` - Mark message as read
- `DELETE /api/v1/conversations/{conversation_id}/messages/{message_id}` - Soft-delete message (sender-authorized)

---

## 5. Development & Testing

### Run Tests
```sh
go test ./...
```

### Run Service
```sh
go run ./cmd/server
```

---

## 6. Infrastructure Integration

- **PostgreSQL**: Durable storage for conversations, members, messages, and read receipts.
- **Redis**: User presence (`user:{id}:presence`), device connection tracking (`user:{id}:connections`), and Redis Pub/Sub (`messaging:ws_events`) for horizontal scaling across multiple instances.
- **Kafka / Redpanda**: Asynchronous publishing of `MESSAGE_SENT`, `MESSAGE_READ`, `MESSAGE_DELETED` events to the `messaging.events` topic with retries and duplicate event consumer handling.
