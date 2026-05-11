# Hard Rules Engine Implementation Plan

## 1. Project Skeleton and Dependencies
- Initialize a Rust workspace for a single Axum-based microservice.
- Add dependencies for Axum, Tokio, Serde, tracing, dotenvy, aho-corasick, regex, and PostgreSQL client libraries.
- Define configuration loading for database, logging, and runtime settings.

## 2. Data Model and Persistence
- Design the `rules` table schema per the architecture (id, category, severity, keywords, regex_pattern, action, enabled, source, timestamps).
- Create database migration scripts and a minimal data access layer.
- Define Rust types for rules and rule metadata used by the runtime engine.

## 3. In-Memory Rule Store and Indexing
- Build a rule loader that fetches enabled rules at startup.
- Compile regex patterns at load time and construct Aho-Corasick indexes from keywords.
- Store compiled artifacts in a shared, thread-safe in-memory structure optimized for concurrent reads.

## 4. Request Processing Pipeline
- Implement the `/check` endpoint contract and request/response DTOs.
- Normalize input and tokenize content per the architecture.
- Run Aho-Corasick matching to gather candidate rules.
- Apply regex verification to candidates and return PASS/FAIL with matched rule metadata.

## 5. Dynamic Rule Reloading
- Implement a reload mechanism that refreshes rules and rebuilds indexes without restarting.
- Ensure reload swaps the runtime indexes atomically to keep fast-path requests lock-free.
- Add a strategy for triggering reloads (polling, notification, or admin endpoint).

## 6. AI Fallback Flow (Integration Points)
- Define the interface for forwarding unmatched requests to an AI rule generator.
- Add validation steps for AI-generated rules (regex safety, duplication, false-positive checks, performance validation).
- Persist only validated rules and trigger runtime reload after approval.

## 7. Observability and Logging
- Configure structured logging with tracing.
- Record request identifiers, match outcomes, and latency metrics for throughput analysis.
- Ensure logs are compatible with CloudWatch.

## 8. Operational Requirements
- Enforce stateless behavior and avoid disk IO or DB reads in the fast path.
- Ensure async-only request handling with Tokio.
- Add limits for payload size and schema validation errors.

## 9. Containerization and Deployment
- Create a Dockerfile optimized for a small Rust runtime image.
- Define ECS Fargate deployment settings (stateless tasks, autoscaling, ECR image).
- Document required environment variables for runtime configuration.

## 10. Validation and Testing
- Add unit tests for tokenizer, matching layers, and regex verification.
- Add integration tests for `/check` endpoint and rule reload behavior.
- Include performance tests to validate high-throughput targets.
