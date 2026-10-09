package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Env               string
	Port              string
	DatabaseURL       string
	RedisURL          string
	RedpandaBrokers   []string
	KafkaTopic        string
	KafkaGroupID      string
	JWTSecret         string
	GatewaySecret     string
	AllowedOrigins    []string
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	ShutdownTimeout   time.Duration
	DBMaxOpenConns    int
	DBMaxIdleConns    int
	DBConnMaxLifetime time.Duration
}

func (c *Config) IsProduction() bool {
	env := strings.ToLower(strings.TrimSpace(c.Env))
	return env == "production" || env == "prod"
}

func (c *Config) IsTest() bool {
	env := strings.ToLower(strings.TrimSpace(c.Env))
	return env == "test" || env == "testing"
}

func (c *Config) IsDevelopment() bool {
	return !c.IsProduction() && !c.IsTest()
}

func Load() (*Config, error) {
	env := getEnv("ENV", "development")
	portStr := getEnv("PORT", "8080")
	dbURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/lukmi?sslmode=disable")
	redisURL := getEnv("REDIS_URL", "redis://localhost:6379")
	brokersStr := getEnv("REDPANDA_BROKERS", getEnv("KAFKA_BROKERS", "localhost:9092"))
	kafkaTopic := getEnv("KAFKA_TOPIC", "messaging.events")
	kafkaGroupID := getEnv("KAFKA_GROUP_ID", "messaging-service-group")
	jwtSecret := os.Getenv("JWT_SECRET")
	gatewaySecret := os.Getenv("GATEWAY_SECRET")
	originsStr := getEnv("ALLOWED_ORIGINS", "")

	brokers := parseCommaList(brokersStr)
	origins := parseCommaList(originsStr)

	readTimeoutSec, err := getIntEnv("READ_TIMEOUT_SECONDS", 15)
	if err != nil || readTimeoutSec <= 0 {
		return nil, fmt.Errorf("invalid READ_TIMEOUT_SECONDS: must be positive integer")
	}

	writeTimeoutSec, err := getIntEnv("WRITE_TIMEOUT_SECONDS", 15)
	if err != nil || writeTimeoutSec <= 0 {
		return nil, fmt.Errorf("invalid WRITE_TIMEOUT_SECONDS: must be positive integer")
	}

	idleTimeoutSec, err := getIntEnv("IDLE_TIMEOUT_SECONDS", 60)
	if err != nil || idleTimeoutSec <= 0 {
		return nil, fmt.Errorf("invalid IDLE_TIMEOUT_SECONDS: must be positive integer")
	}

	shutdownTimeoutSec, err := getIntEnv("SHUTDOWN_TIMEOUT_SECONDS", 10)
	if err != nil || shutdownTimeoutSec <= 0 {
		return nil, fmt.Errorf("invalid SHUTDOWN_TIMEOUT_SECONDS: must be positive integer")
	}

	dbMaxOpenConns, err := getIntEnv("DB_MAX_OPEN_CONNS", 25)
	if err != nil || dbMaxOpenConns <= 0 {
		return nil, fmt.Errorf("invalid DB_MAX_OPEN_CONNS: must be positive integer")
	}

	dbMaxIdleConns, err := getIntEnv("DB_MAX_IDLE_CONNS", 10)
	if err != nil || dbMaxIdleConns <= 0 {
		return nil, fmt.Errorf("invalid DB_MAX_IDLE_CONNS: must be positive integer")
	}

	if dbMaxIdleConns > dbMaxOpenConns {
		return nil, fmt.Errorf("invalid DB pool config: DB_MAX_IDLE_CONNS (%d) cannot exceed DB_MAX_OPEN_CONNS (%d)", dbMaxIdleConns, dbMaxOpenConns)
	}

	dbConnLifetimeMin, err := getIntEnv("DB_CONN_MAX_LIFETIME_MINUTES", 15)
	if err != nil || dbConnLifetimeMin <= 0 {
		return nil, fmt.Errorf("invalid DB_CONN_MAX_LIFETIME_MINUTES: must be positive integer")
	}

	portNum, err := strconv.Atoi(portStr)
	if err != nil || portNum < 1 || portNum > 65535 {
		return nil, fmt.Errorf("invalid PORT (%s): must be integer between 1 and 65535", portStr)
	}

	cfg := &Config{
		Env:               env,
		Port:              portStr,
		DatabaseURL:       dbURL,
		RedisURL:          redisURL,
		RedpandaBrokers:   brokers,
		KafkaTopic:        kafkaTopic,
		KafkaGroupID:      kafkaGroupID,
		JWTSecret:         jwtSecret,
		GatewaySecret:     gatewaySecret,
		AllowedOrigins:    origins,
		ReadTimeout:       time.Duration(readTimeoutSec) * time.Second,
		WriteTimeout:      time.Duration(writeTimeoutSec) * time.Second,
		IdleTimeout:       time.Duration(idleTimeoutSec) * time.Second,
		ShutdownTimeout:   time.Duration(shutdownTimeoutSec) * time.Second,
		DBMaxOpenConns:    dbMaxOpenConns,
		DBMaxIdleConns:    dbMaxIdleConns,
		DBConnMaxLifetime: time.Duration(dbConnLifetimeMin) * time.Minute,
	}

	// Default fallback values for development/testing mode ONLY
	if cfg.IsDevelopment() || cfg.IsTest() {
		if cfg.JWTSecret == "" {
			cfg.JWTSecret = "development-only-jwt-secret-key-32bytes-min"
		}
		if cfg.GatewaySecret == "" {
			cfg.GatewaySecret = "development-only-gateway-secret-16bytes"
		}
	}

	// Strict Production Validation Rules
	if cfg.IsProduction() {
		if cfg.JWTSecret == "" || len(cfg.JWTSecret) < 32 || cfg.JWTSecret == "lukmi-secret-key-change-in-production" || strings.Contains(cfg.JWTSecret, "change-in-production") {
			return nil, errors.New("production configuration error: JWT_SECRET must be explicitly configured with at least 32 characters in production")
		}

		if cfg.GatewaySecret == "" || len(cfg.GatewaySecret) < 16 {
			return nil, errors.New("production configuration error: GATEWAY_SECRET must be explicitly configured with at least 16 characters in production")
		}

		if cfg.DatabaseURL == "" || strings.Contains(cfg.DatabaseURL, "postgres:postgres@localhost") {
			return nil, errors.New("production configuration error: DATABASE_URL must be explicitly set with non-default production credentials")
		}
	}

	return cfg, nil
}

func (c *Config) Redact() map[string]any {
	return map[string]any{
		"env":               c.Env,
		"port":              c.Port,
		"database_url":      redactURL(c.DatabaseURL),
		"redis_url":         redactURL(c.RedisURL),
		"brokers":           c.RedpandaBrokers,
		"kafka_topic":       c.KafkaTopic,
		"kafka_group_id":    c.KafkaGroupID,
		"allowed_origins":   c.AllowedOrigins,
		"db_max_open_conns": c.DBMaxOpenConns,
		"db_max_idle_conns": c.DBMaxIdleConns,
	}
}

func redactURL(rawURL string) string {
	if rawURL == "" {
		return ""
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return "[redacted-invalid-url]"
	}
	if u.User != nil {
		u.User = url.UserPassword(u.User.Username(), "*****")
	}
	return u.String()
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getIntEnv(key string, fallback int) (int, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	i, err := strconv.Atoi(value)
	if err != nil {
		return 0, err
	}
	return i, nil
}

func parseCommaList(raw string) []string {
	if raw == "" {
		return []string{}
	}
	parts := strings.Split(raw, ",")
	res := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			res = append(res, trimmed)
		}
	}
	return res
}
