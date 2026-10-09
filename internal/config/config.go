package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Env              string
	Port             string
	DatabaseURL      string
	RedisURL         string
	RedpandaBrokers  []string
	KafkaTopic       string
	KafkaGroupID     string
	JWTSecret        string
	ReadTimeout      time.Duration
	WriteTimeout     time.Duration
	IdleTimeout      time.Duration
	ShutdownTimeout  time.Duration
	DBMaxOpenConns   int
	DBMaxIdleConns   int
	DBConnMaxLifetime time.Duration
}

func Load() *Config {
	brokersStr := getEnv("REDPANDA_BROKERS", getEnv("KAFKA_BROKERS", "localhost:9092"))
	brokers := strings.Split(brokersStr, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	return &Config{
		Env:               getEnv("ENV", "development"),
		Port:              getEnv("PORT", "8080"),
		DatabaseURL:       getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/lukmi?sslmode=disable"),
		RedisURL:          getEnv("REDIS_URL", "redis://localhost:6379"),
		RedpandaBrokers:   brokers,
		KafkaTopic:        getEnv("KAFKA_TOPIC", "messaging.events"),
		KafkaGroupID:      getEnv("KAFKA_GROUP_ID", "messaging-service-group"),
		JWTSecret:         getEnv("JWT_SECRET", "lukmi-secret-key-change-in-production"),
		ReadTimeout:       getDurationEnv("READ_TIMEOUT_SECONDS", 15) * time.Second,
		WriteTimeout:      getDurationEnv("WRITE_TIMEOUT_SECONDS", 15) * time.Second,
		IdleTimeout:       getDurationEnv("IDLE_TIMEOUT_SECONDS", 60) * time.Second,
		ShutdownTimeout:   getDurationEnv("SHUTDOWN_TIMEOUT_SECONDS", 10) * time.Second,
		DBMaxOpenConns:    getIntEnv("DB_MAX_OPEN_CONNS", 25),
		DBMaxIdleConns:    getIntEnv("DB_MAX_IDLE_CONNS", 10),
		DBConnMaxLifetime: getDurationEnv("DB_CONN_MAX_LIFETIME_MINUTES", 15) * time.Minute,
	}
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getIntEnv(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return fallback
}

func getDurationEnv(key string, fallback int) time.Duration {
	return time.Duration(getIntEnv(key, fallback))
}
