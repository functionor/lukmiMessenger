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
	cfg := config.Load()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	logger.Info("starting lukmi messaging service", "env", cfg.Env, "port", cfg.Port)

	// 1. Initialize Repository (PostgreSQL with Memory fallback)
	var repo repository.Repository
	pgRepo, err := repository.NewPostgresRepository(cfg)
	if err != nil {
		logger.Warn("postgres unavailable, using memory repository fallback", "error", err)
		repo = repository.NewMemoryRepository()
	} else {
		logger.Info("connected to postgresql database")
		repo = pgRepo
	}

	// 2. Initialize Cache (Redis with Memory fallback)
	var c cache.Cache
	redisCache, err := cache.NewRedisCache(cfg.RedisURL, logger)
	if err != nil {
		logger.Warn("redis unavailable, using memory cache fallback", "error", err)
		c = cache.NewMemoryCache()
	} else {
		logger.Info("connected to redis")
		c = redisCache
	}

	// 3. Initialize Kafka Event Publisher (Redpanda/Kafka with Memory fallback)
	var publisher kafka.EventPublisher
	producer, err := kafka.NewProducer(cfg.RedpandaBrokers, cfg.KafkaTopic, logger)
	if err != nil {
		logger.Warn("kafka producer unavailable, using memory publisher fallback", "error", err)
		publisher = kafka.NewMemoryPublisher(logger)
	} else {
		logger.Info("connected to kafka/redpanda producer", "brokers", cfg.RedpandaBrokers, "topic", cfg.KafkaTopic)
		publisher = producer
	}

	// 4. Initialize WebSocket Hub
	hub := websocket.NewHub(c, logger)

	// 5. Initialize Core Domain Service
	svc := service.New(repo, publisher, hub)

	// 6. Construct HTTP Handler & Routes
	handler := httpapi.New(repo, svc, hub, c, publisher, cfg.JWTSecret)
	handlerWithMiddleware := middleware.Logging(logger)(handler.Routes())

	server := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           handlerWithMiddleware,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
	}

	// 7. Start HTTP Server
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("server listening", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server stopped unexpectedly", "error", err)
			os.Exit(1)
		}
	}()

	// 8. Graceful Shutdown
	<-ctx.Done()
	logger.Info("shutting down messaging service gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("server forced to shutdown", "error", err)
	}

	hub.Close()
	if publisher != nil {
		_ = publisher.Close()
	}

	logger.Info("messaging service shutdown complete")
}
