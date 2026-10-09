# Lukmi Messaging Service

Internal Go messaging service. The API Gateway is the public edge; this service owns conversations, members, messages, delivery/read events, synchronization, and WebSocket delivery.

## Boundaries

- PostgreSQL is the durable source of truth for the existing `conversations`, `conversation_members`, `messages`, and `message_reads` tables.
- Shared content is stored as `reference_id`; this service never fetches posts, profiles, or stories.
- Media messages store `media_id`; media remains owned by the Media Service/R2.
- Clients may send `ciphertext` instead of plaintext. No private keys or custom cryptography belong here.
- Kafka/Redpanda receives asynchronous message events. Notification delivery is outside this service.
- Redis is reserved for transient connection/presence state; it is not the message store.

## Current runnable baseline

`cmd/server` runs with an in-memory repository so the service can be exercised locally. The `repository.Repository`, `service.EventPublisher`, and `service.Broadcaster` interfaces are the replacement points for the existing PostgreSQL, Kafka/Redpanda, and operational Redis adapters.

The current endpoints include health/readiness, conversation creation and access, member listing, paginated message synchronization, message creation, and authenticated WebSocket delivery at `/api/v1/ws`.

Identity is currently taken from the gateway-provided `X-User-ID` header (or the development `Bearer` value). In production, replace `auth` with the internal signed-token/service-auth validator; never allow an untrusted public caller to set this header.

## Run

```sh
go mod tidy
go test ./...
go run ./cmd/server
```

The service listens on `PORT` (default `8080`). See `.env.example` for infrastructure configuration names.

## Required production work

1. Implement the repository adapter against the central Lukmi PostgreSQL schema and coordinate any needed `reference_id` migration in `lukmi-database`.
2. Replace the development identity middleware with the API Gateway's validated internal authentication context.
3. Replace the logging publisher with a durable Kafka/Redpanda producer using retries and an outbox or equivalent delivery guarantee.
4. Put connection/presence bookkeeping behind the Redis adapter when running multiple service instances; use a shared pub/sub or broker fan-out for WebSocket delivery.
5. Add metrics and tracing export, rate limits, and integration tests against PostgreSQL, Redpanda, Redis, and the gateway auth contract.
