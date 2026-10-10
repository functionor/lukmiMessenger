package middleware

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/lukmi/messaging-service/internal/config"
	"github.com/lukmi/messaging-service/internal/response"
)

type contextKey string

const (
	UserIDKey    contextKey = "user_id"
	RequestIDKey contextKey = "request_id"
)

var validUserIDRegex = regexp.MustCompile(`^[a-zA-Z0-9_\-\.\@]{1,64}$`)

func Auth(cfg *config.Config, logger *slog.Logger) func(http.Handler) http.Handler {
	secretBytes := []byte(cfg.JWTSecret)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Skip auth for health probes
			path := r.URL.Path
			if path == "/healthz" || path == "/readyz" {
				next.ServeHTTP(w, r)
				return
			}

			var userID string

			// 1. Gateway Contract Authentication (X-User-ID + X-Gateway-Secret)
			headerUserID := r.Header.Get("X-User-ID")
			headerGatewaySecret := r.Header.Get("X-Gateway-Secret")

			if headerUserID != "" {
				if cfg.GatewaySecret != "" && subtle.ConstantTimeCompare([]byte(headerGatewaySecret), []byte(cfg.GatewaySecret)) == 1 {
					if validUserIDRegex.MatchString(headerUserID) {
						userID = headerUserID
					} else {
						response.Error(w, http.StatusBadRequest, "invalid user id format")
						return
					}
				} else if cfg.GatewaySecret != "" {
					response.Error(w, http.StatusUnauthorized, "unauthorized: invalid or missing gateway secret")
					return
				}
			}

			// 2. Direct Signed JWT Authentication (Bearer Header or WS Query Parameter)
			if userID == "" {
				authHeader := r.Header.Get("Authorization")
				tokenStr := ""

				if strings.HasPrefix(authHeader, "Bearer ") {
					tokenStr = strings.TrimPrefix(authHeader, "Bearer ")
				} else if queryToken := r.URL.Query().Get("token"); queryToken != "" {
					tokenStr = queryToken
				}

				if tokenStr != "" {
					parsedToken, err := jwt.Parse(tokenStr, func(token *jwt.Token) (interface{}, error) {
						// Strictly pin HS256 algorithm
						if token.Method.Alg() != "HS256" {
							return nil, fmt.Errorf("unsupported signing algorithm: %v", token.Header["alg"])
						}
						return secretBytes, nil
					}, jwt.WithExpirationRequired())

					if err == nil && parsedToken.Valid {
						if claims, ok := parsedToken.Claims.(jwt.MapClaims); ok {
							// Enforce issuer check if configured
							if cfg.JWTIssuer != "" {
								if iss, ok := claims["iss"].(string); !ok || iss != cfg.JWTIssuer {
									response.Error(w, http.StatusUnauthorized, "unauthorized: invalid token issuer")
									return
								}
							}

							// Enforce audience check if configured
							if cfg.JWTAudience != "" {
								if aud, ok := claims["aud"].(string); !ok || aud != cfg.JWTAudience {
									response.Error(w, http.StatusUnauthorized, "unauthorized: invalid token audience")
									return
								}
							}

							var candidateID string
							if sub, ok := claims["sub"].(string); ok && sub != "" {
								candidateID = sub
							} else if uid, ok := claims["user_id"].(string); ok && uid != "" {
								candidateID = uid
							}

							if candidateID != "" && validUserIDRegex.MatchString(candidateID) {
								userID = candidateID
							}
						}
					}
				}
			}

			if userID == "" {
				response.Error(w, http.StatusUnauthorized, "unauthorized: valid authentication credentials required")
				return
			}

			ctx := context.WithValue(r.Context(), UserIDKey, userID)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func GetUserID(ctx context.Context) string {
	if val, ok := ctx.Value(UserIDKey).(string); ok {
		return val
	}
	return ""
}

func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get("X-Request-ID")
		if reqID == "" {
			bytes := make([]byte, 8)
			_, _ = rand.Read(bytes)
			reqID = fmt.Sprintf("%d-%s", time.Now().UnixNano(), hex.EncodeToString(bytes))
		}

		w.Header().Set("X-Request-ID", reqID)
		ctx := context.WithValue(r.Context(), RequestIDKey, reqID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func Logging(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			wrapped := &responseWriterWrapper{ResponseWriter: w, statusCode: http.StatusOK}

			next.ServeHTTP(wrapped, r)

			duration := time.Since(start)
			reqID, _ := r.Context().Value(RequestIDKey).(string)
			userID, _ := r.Context().Value(UserIDKey).(string)

			sanitizedPath := sanitizeURLPath(r.URL)

			logger.Info("http request",
				"method", r.Method,
				"path", sanitizedPath,
				"status", wrapped.statusCode,
				"duration_ms", duration.Milliseconds(),
				"request_id", reqID,
				"user_id", userID,
			)
		})
	}
}

func sanitizeURLPath(u *url.URL) string {
	if u == nil {
		return ""
	}
	query := u.Query()
	if query.Has("token") {
		query.Set("token", "[REDACTED]")
		return u.Path + "?" + query.Encode()
	}
	return u.RequestURI()
}

type responseWriterWrapper struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriterWrapper) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}
