package config

import (
	"os"
	"testing"
)

func TestConfigLoadDevelopmentDefaults(t *testing.T) {
	os.Clearenv()
	_ = os.Setenv("ENV", "development")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected valid load in development, got error: %v", err)
	}

	if cfg.Port != "8080" || cfg.JWTSecret == "" || cfg.GatewaySecret == "" {
		t.Fatalf("unexpected dev config: %+v", cfg)
	}
}

func TestConfigLoadProductionFailsWithoutSecrets(t *testing.T) {
	os.Clearenv()
	_ = os.Setenv("ENV", "production")

	_, err := Load()
	if err == nil {
		t.Fatal("expected production Load to fail without explicit JWT_SECRET and GATEWAY_SECRET")
	}
}

func TestConfigLoadProductionValid(t *testing.T) {
	os.Clearenv()
	_ = os.Setenv("ENV", "production")
	_ = os.Setenv("JWT_SECRET", "super-secret-production-jwt-key-32bytes-minimum!!")
	_ = os.Setenv("GATEWAY_SECRET", "super-secret-gateway-key-16bytes!!")
	_ = os.Setenv("DATABASE_URL", "postgres://prod_user:secure_pass@db.prod.internal:5432/lukmi_prod?sslmode=verify-full")
	_ = os.Setenv("ALLOWED_ORIGINS", "https://app.lukmi.com")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected production Load to succeed, got error: %v", err)
	}

	if !cfg.IsProduction() {
		t.Fatal("expected IsProduction() to be true")
	}
}

func TestConfigLoadInvalidPoolSizes(t *testing.T) {
	os.Clearenv()
	_ = os.Setenv("ENV", "development")
	_ = os.Setenv("DB_MAX_OPEN_CONNS", "10")
	_ = os.Setenv("DB_MAX_IDLE_CONNS", "20") // Max idle > Max open -> invalid

	_, err := Load()
	if err == nil {
		t.Fatal("expected error when DB_MAX_IDLE_CONNS > DB_MAX_OPEN_CONNS")
	}
}

func TestConfigLoadInvalidPort(t *testing.T) {
	os.Clearenv()
	_ = os.Setenv("ENV", "development")
	_ = os.Setenv("PORT", "999999")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error when PORT is invalid")
	}
}

func TestConfigRedact(t *testing.T) {
	os.Clearenv()
	_ = os.Setenv("ENV", "development")
	_ = os.Setenv("DATABASE_URL", "postgres://user:secretpassword@localhost:5432/lukmi")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	redacted := cfg.Redact()
	dbURLStr := redacted["database_url"].(string)
	if dbURLStr == "" || dbURLStr == cfg.DatabaseURL {
		t.Fatalf("expected database_url password to be redacted, got: %s", dbURLStr)
	}
}
