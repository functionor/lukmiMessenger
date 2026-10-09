package websocket

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lukmi/messaging-service/internal/cache"
	"github.com/lukmi/messaging-service/internal/config"
)

func TestHubOriginValidation(t *testing.T) {
	cfg := &config.Config{
		Env:            "production",
		AllowedOrigins: []string{"https://app.lukmi.com"},
	}
	memCache := cache.NewMemoryCache()
	hub := NewHub(cfg, memCache, nil)
	defer hub.Close()

	// Allowed origin
	reqAllowed := httptest.NewRequest(http.MethodGet, "/api/v1/ws", nil)
	reqAllowed.Header.Set("Origin", "https://app.lukmi.com")
	if !hub.checkOrigin(reqAllowed) {
		t.Fatal("expected origin https://app.lukmi.com to be allowed")
	}

	// Rejected origin
	reqRejected := httptest.NewRequest(http.MethodGet, "/api/v1/ws", nil)
	reqRejected.Header.Set("Origin", "https://malicious-site.com")
	if hub.checkOrigin(reqRejected) {
		t.Fatal("expected origin https://malicious-site.com to be rejected")
	}

	// Native client without Origin header -> allowed
	reqNative := httptest.NewRequest(http.MethodGet, "/api/v1/ws", nil)
	if !hub.checkOrigin(reqNative) {
		t.Fatal("expected native request without Origin header to be permitted")
	}
}

func TestHubUnauthenticatedServeHTTP(t *testing.T) {
	cfg := &config.Config{Env: "development"}
	memCache := cache.NewMemoryCache()
	hub := NewHub(cfg, memCache, nil)
	defer hub.Close()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/ws", nil)

	// Missing user_id
	hub.ServeHTTP(rec, req, "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 Unauthorized for empty user_id", rec.Code)
	}
}
