package kafka

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/lukmi/messaging-service/internal/model"
)

type OutboxRepository interface {
	ClaimPendingOutboxEvents(ctx context.Context, processorID string, leaseDuration time.Duration, limit int) ([]*model.Event, error)
	MarkOutboxEventPublished(ctx context.Context, eventID string, processorID string) error
	RecordOutboxEventFailure(ctx context.Context, eventID string, processorID string, errMsg string) error
}

type OutboxProcessor struct {
	processorID  string
	repo         OutboxRepository
	publisher    EventPublisher
	logger       *slog.Logger
	pollInterval time.Duration
	wg           sync.WaitGroup
	cancel       func()
}

func NewOutboxProcessor(processorID string, repo OutboxRepository, publisher EventPublisher, logger *slog.Logger, pollInterval time.Duration) *OutboxProcessor {
	if pollInterval <= 0 {
		pollInterval = 500 * time.Millisecond
	}
	if processorID == "" {
		processorID = "default-processor"
	}
	return &OutboxProcessor{
		processorID:  processorID,
		repo:         repo,
		publisher:    publisher,
		logger:       logger,
		pollInterval: pollInterval,
	}
}

func (op *OutboxProcessor) Start(ctx context.Context) {
	procCtx, cancel := context.WithCancel(ctx)
	op.cancel = cancel

	op.wg.Add(1)
	go func() {
		defer op.wg.Done()
		ticker := time.NewTicker(op.pollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-procCtx.Done():
				op.flushPending(context.Background())
				return
			case <-ticker.C:
				op.processBatch(procCtx)
			}
		}
	}()
}

func (op *OutboxProcessor) processBatch(ctx context.Context) {
	events, err := op.repo.ClaimPendingOutboxEvents(ctx, op.processorID, 30*time.Second, 50)
	if err != nil {
		if op.logger != nil {
			op.logger.Error("outbox processor failed to claim pending events", "processor_id", op.processorID, "error", err)
		}
		return
	}

	for _, event := range events {
		if ctx.Err() != nil {
			return
		}

		pubErr := op.publisher.Publish(ctx, *event)
		if pubErr == nil {
			if markErr := op.repo.MarkOutboxEventPublished(ctx, event.ID, op.processorID); markErr != nil {
				if op.logger != nil {
					op.logger.Error("outbox processor failed to mark event published", "event_id", event.ID, "error", markErr)
				}
			} else if op.logger != nil {
				op.logger.Debug("outbox event published successfully", "event_id", event.ID, "event_type", event.Type)
			}
		} else {
			if recErr := op.repo.RecordOutboxEventFailure(ctx, event.ID, op.processorID, pubErr.Error()); recErr != nil {
				if op.logger != nil {
					op.logger.Error("outbox processor failed to record event failure", "event_id", event.ID, "error", recErr)
				}
			}
			if op.logger != nil {
				op.logger.Error("outbox event publish failed", "event_id", event.ID, "error", pubErr)
			}
		}
	}
}

func (op *OutboxProcessor) flushPending(ctx context.Context) {
	flushCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	events, err := op.repo.ClaimPendingOutboxEvents(flushCtx, op.processorID, 10*time.Second, 100)
	if err != nil || len(events) == 0 {
		return
	}

	for _, event := range events {
		if op.publisher.Publish(flushCtx, *event) == nil {
			_ = op.repo.MarkOutboxEventPublished(flushCtx, event.ID, op.processorID)
		}
	}
}

func (op *OutboxProcessor) Stop() {
	if op.cancel != nil {
		op.cancel()
	}
	op.wg.Wait()
}
