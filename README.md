# Lukmi Messaging Service Backend (v1 Hardened)

Production-hardened Go backend microservice for the **Lukmi Messaging Service**.

The Messaging Service is an internal service located behind the Public API Gateway. It owns conversations, members, messages, message delivery states, read receipts, WebSocket real-time delivery across instances, and asynchronous event publishing.

---

## 1. Architecture & Service Boundaries

```text
Flutter App
     │
     │ HTTPS / WebSocket
     ▼
Public API Gateway
     │
     ├── X-Gateway-Secret / Bearer JWT
     ▼
Messaging Service
     │
     ├── PostgreSQL (APP_DATABASE_URL - lukmi_app runtime role)
     ├── Redis (Transient Presence & Cross-Node WS Events)
     └── Kafka/Redpanda (Messaging Domain Events)
```

### Critical Boundaries
- **Resource Ownership**: Shared content (`POST_SHARE`, `PROFILE_SHARE`, `STORY_SHARE`) stores only the resource `reference_id`. The Messaging Service never queries Content, User, or Story services. Clients fetch full resources directly via the API Gateway.
- **Media**: Messages with media reference `media_id`. Binary media storage is owned by Media Service / Cloudflare R2.
- **E2EE**: Accepts client-encrypted `ciphertext` without performing server-side decryption or key management.
- **Push Notifications**: Push notifications (FCM/APNs) are owned by the Notification Service, which consumes asynchronous events (`MESSAGE_SENT`, `MESSAGE_READ`, `MESSAGE_DELETED`) published to Kafka/Redpanda.

---

## 2. Security & Authentication Contract

### API Gateway Trust Contract
In production, requests forwarded by the API Gateway must supply:
- `X-User-ID`: The authenticated user ID.
- `X-Gateway-Secret`: Header matching `GATEWAY_SECRET` configured on the service.

Direct client connections supplying a JWT bearer token are authenticated using HMAC (`HS256` pinned) with required claim checks (`sub` / `user_id`, `exp`, `iss` matching `JWT_ISSUER`, `aud` matching `JWT_AUDIENCE`).

Unauthenticated requests, forged headers, or invalid tokens return `401 Unauthorized`.

---

## 3. Database Schema Ownership & Outbox Pattern

The central database schema is owned by:
`https://github.com/functionor/lukmi_database`

The service connects using `APP_DATABASE_URL` with runtime credentials (`lukmi_app`). **Runtime DDL auto-migrations are disabled.**

### Required Migrations for `lukmi-database`
- `migrations/000001_init_messaging_schema.up.sql`: Conversations, members, messages, and read receipt tables.
- `migrations/000002_add_outbox_events.up.sql`: Transactional outbox table (`outbox_events`) for reliable event delivery.

### Transactional Outbox Worker
When messages, read receipts, or soft-deletions are written, an `outbox_events` record is created inside the same PostgreSQL database transaction. An asynchronous background `OutboxProcessor` atomically claims pending events with a lease lock (`FOR UPDATE SKIP LOCKED`) and publishes them to Kafka/Redpanda.

---

## 4. Keyset Pagination

- **Messages Pagination**: Uses compound keyset cursor `(created_at, message_id)`.
- **Conversations Pagination**: Uses compound keyset cursor `(updated_at, conversation_id)`.
- API endpoints return `next_cursor` strings. Invalid or malformed cursor strings return `400 Bad Request`.

---

## 5. Configuration & Environment Variables

| Variable | Description | Production Requirement |
| :--- | :--- | :--- |
| `ENV` | Environment mode (`development`, `test`, `production`) | Required |
| `PORT` | Listening HTTP port | Default: `8080` |
| `APP_DATABASE_URL` | PostgreSQL runtime database URL (`lukmi_app` user) | Required in production |
| `REDIS_URL` | Redis URL for presence and cross-node WS fan-out | Required in production |
| `ALLOW_DEGRADED_REDIS` | Set `true` to allow single-instance running if Redis is down | Default: `false` |
| `REDPANDA_BROKERS` | Kafka/Redpanda broker addresses | Required in production |
| `KAFKA_TOPIC` | Event topic name | Default: `messaging.events` |
| `JWT_SECRET` | Secret key for JWT verification | Required (min 32 chars) |
| `JWT_ISSUER` | Expected JWT issuer claim | Default: `lukmi-auth` |
| `JWT_AUDIENCE` | Expected JWT audience claim | Default: `lukmi-messaging` |
| `GATEWAY_SECRET` | Secret key for API Gateway assertion | Required (min 16 chars) |
| `ALLOWED_ORIGINS` | Comma-separated allowed WebSocket origins | Required in production |

---

## 6. Build & Test Commands

```sh
# Run unit and integration tests
go test -count=1 ./...

# Run tests with race detector
go test -race ./...

# Static analysis
go vet ./...

# Format check
gofmt -s -w .

# Build server binary
go build -o bin/messaging-service ./cmd/server
```
