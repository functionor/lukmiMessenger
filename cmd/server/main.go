package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lukmi/messaging-service/internal/cache"
	"github.com/lukmi/messaging-service/internal/config"
	"github.com/lukmi/messaging-service/internal/httpapi"
	"github.com/lukmi/messaging-service/internal/kafka"
	"github.com/lukmi/messaging-service/internal/middleware"
	"github.com/lukmi/messaging-service/internal/repository"
	"github.com/lukmi/messaging-service/internal/service"
	"github.com/lukmi/messaging-service/internal/websocket"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	logger.Info("starting lukmi messaging service", "env", cfg.Env, "port", cfg.Port, "config", cfg.Redact())

	// 1. Initialize Repository (PostgreSQL mandatory in production)
	var repo repository.Repository
	pgRepo, err := repository.NewPostgresRepository(cfg)
	if err != nil {
		if cfg.IsProduction() {
			logger.Error("production startup error: postgresql repository connection failed", "error", err)
			os.Exit(1)
		}
		logger.Warn("postgresql unavailable in development mode, falling back to memory repository", "error", err)
		repo = repository.NewMemoryRepository()
	} else {
		logger.Info("connected to postgresql database")
		repo = pgRepo
		defer pgRepo.Close()
	}

	// 2. Initialize Cache (Redis)
	var c cache.Cache
	redisCache, err := cache.NewRedisCache(cfg.RedisURL, logger)
	if err != nil {
		if cfg.IsProduction() {
			if !cfg.AllowDegradedRedis {
				logger.Error("production startup error: redis connection failed and ALLOW_DEGRADED_REDIS is false", "error", err)
				os.Exit(1)
			}
			logger.Warn("redis cache connection failed in production; running with explicitly allowed degraded cache", "error", err)
			c = cache.NewMemoryCache()
		} else {
			logger.Warn("redis unavailable in development mode, falling back to memory cache", "error", err)
			c = cache.NewMemoryCache()
		}
	} else {
		logger.Info("connected to redis cache")
		c = redisCache
	}

	// 3. Initialize Kafka Event Publisher (Redpanda/Kafka mandatory in production)
	var publisher kafka.EventPublisher
	producer, err := kafka.NewProducer(cfg.RedpandaBrokers, cfg.KafkaTopic, logger)
	if err != nil {
		if cfg.IsProduction() {
			logger.Error("production startup error: kafka/redpanda producer connection failed", "error", err)
			os.Exit(1)
		}
		logger.Warn("kafka producer unavailable in development mode, falling back to memory publisher", "error", err)
		publisher = kafka.NewMemoryPublisher(logger)
	} else {
		logger.Info("connected to kafka/redpanda producer", "brokers", cfg.RedpandaBrokers, "topic", cfg.KafkaTopic)
		publisher = producer
		defer producer.Close()
	}

	// 4. Initialize Outbox Event Processor for reliable event delivery
	processorID := "proc-" + cfg.Port
	outboxProcessor := kafka.NewOutboxProcessor(processorID, repo, publisher, logger, 500*time.Millisecond)
	outboxCtx, cancelOutbox := context.WithCancel(context.Background())
	outboxProcessor.Start(outboxCtx)
	defer func() {
		cancelOutbox()
		outboxProcessor.Stop()
	}()

	// 5. Initialize WebSocket Hub
	hub := websocket.NewHub(cfg, c, logger)
	defer hub.Close()

	// 6. Initialize Core Service
	svc := service.New(repo, publisher, hub)

	// 7. Construct HTTP Handler & Routes
	handler := httpapi.New(cfg, repo, svc, hub, c, publisher)
	handlerWithMiddleware := middleware.Logging(logger)(handler.Routes())

	server := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           handlerWithMiddleware,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
	}

	// 8. Start HTTP Server with Graceful Shutdown
	stopCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("messaging server listening", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("messaging server stopped unexpectedly", "error", err)
			os.Exit(1)
		}
	}()

	<-stopCtx.Done()
	logger.Info("shutting down messaging service gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("server forced to shutdown", "error", err)
	}

	logger.Info("messaging service shutdown complete")
}
