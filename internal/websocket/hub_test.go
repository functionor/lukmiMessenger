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

	// 1. Allowed origin matching scheme, host, port
	reqAllowed := httptest.NewRequest(http.MethodGet, "/api/v1/ws", nil)
	reqAllowed.Header.Set("Origin", "https://app.lukmi.com")
	if !hub.checkOrigin(reqAllowed) {
		t.Fatal("expected origin https://app.lukmi.com to be allowed")
	}

	// 2. Rejected origin (different host)
	reqRejected := httptest.NewRequest(http.MethodGet, "/api/v1/ws", nil)
	reqRejected.Header.Set("Origin", "https://malicious-site.com")
	if hub.checkOrigin(reqRejected) {
		t.Fatal("expected origin https://malicious-site.com to be rejected")
	}

	// 3. Rejected scheme mismatch (http vs https)
	reqSchemeMismatch := httptest.NewRequest(http.MethodGet, "/api/v1/ws", nil)
	reqSchemeMismatch.Header.Set("Origin", "http://app.lukmi.com")
	if hub.checkOrigin(reqSchemeMismatch) {
		t.Fatal("expected http://app.lukmi.com to be rejected when https is expected")
	}

	// 4. Native client without Origin header -> allowed when pre-authenticated
	reqNative := httptest.NewRequest(http.MethodGet, "/api/v1/ws", nil)
	if !hub.checkOrigin(reqNative) {
		t.Fatal("expected native request without Origin header to be permitted")
	}
}

func TestHubProductionRejectsWildcardOrigin(t *testing.T) {
	cfg := &config.Config{
		Env:            "production",
		AllowedOrigins: []string{"*"},
	}
	memCache := cache.NewMemoryCache()
	hub := NewHub(cfg, memCache, nil)
	defer hub.Close()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/ws", nil)
	req.Header.Set("Origin", "https://arbitrary-domain.com")
	if hub.checkOrigin(req) {
		t.Fatal("expected wildcard origin * to be rejected in production mode")
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
