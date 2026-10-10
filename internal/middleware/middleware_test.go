package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/lukmi/messaging-service/internal/config"
)

func testConfig() *config.Config {
	return &config.Config{
		Env:           "development",
		JWTSecret:     "test-jwt-secret-32bytes-minimum-key!!",
		JWTIssuer:     "lukmi-auth",
		JWTAudience:   "lukmi-messaging",
		GatewaySecret: "test-gateway-secret-key-16bytes!",
	}
}

func TestAuthMiddlewareGatewaySecretSuccess(t *testing.T) {
	cfg := testConfig()
	authHandler := Auth(cfg, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uid := GetUserID(r.Context())
		if uid != "valid-user-123" {
			t.Fatalf("expected valid-user-123, got %s", uid)
		}
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/conversations", nil)
	req.Header.Set("X-User-ID", "valid-user-123")
	req.Header.Set("X-Gateway-Secret", cfg.GatewaySecret)
	rec := httptest.NewRecorder()

	authHandler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestAuthMiddlewareForgedHeaderRejected(t *testing.T) {
	cfg := testConfig()
	authHandler := Auth(cfg, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/conversations", nil)
	req.Header.Set("X-User-ID", "attacker")
	req.Header.Set("X-Gateway-Secret", "wrong-secret")
	rec := httptest.NewRecorder()

	authHandler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 Unauthorized for forged header", rec.Code)
	}
}

func TestAuthMiddlewareJWTValid(t *testing.T) {
	cfg := testConfig()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": "jwt-user-456",
		"iss": cfg.JWTIssuer,
		"aud": cfg.JWTAudience,
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	tokenStr, _ := token.SignedString([]byte(cfg.JWTSecret))

	authHandler := Auth(cfg, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uid := GetUserID(r.Context())
		if uid != "jwt-user-456" {
			t.Fatalf("expected jwt-user-456, got %s", uid)
		}
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/conversations", nil)
	req.Header.Set("Authorization", "Bearer "+tokenStr)
	rec := httptest.NewRecorder()

	authHandler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestAuthMiddlewareExpiredJWT(t *testing.T) {
	cfg := testConfig()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": "jwt-user-456",
		"iss": cfg.JWTIssuer,
		"aud": cfg.JWTAudience,
		"exp": time.Now().Add(-time.Hour).Unix(), // Expired
	})
	tokenStr, _ := token.SignedString([]byte(cfg.JWTSecret))

	authHandler := Auth(cfg, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/conversations", nil)
	req.Header.Set("Authorization", "Bearer "+tokenStr)
	rec := httptest.NewRecorder()

	authHandler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 for expired token", rec.Code)
	}
}

func TestAuthMiddlewareNoneAlgorithmRejected(t *testing.T) {
	cfg := testConfig()
	authHandler := Auth(cfg, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	token := jwt.NewWithClaims(jwt.SigningMethodNone, jwt.MapClaims{
		"sub": "attacker",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	tokenStr, _ := token.SignedString(jwt.UnsafeAllowNoneSignatureType)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/conversations", nil)
	req.Header.Set("Authorization", "Bearer "+tokenStr)
	rec := httptest.NewRecorder()

	authHandler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 for 'none' alg", rec.Code)
	}
}

func TestAuthMiddlewareMissingCredentials(t *testing.T) {
	cfg := testConfig()
	authHandler := Auth(cfg, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/conversations", nil)
	rec := httptest.NewRecorder()

	authHandler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 for missing credentials", rec.Code)
	}
}
