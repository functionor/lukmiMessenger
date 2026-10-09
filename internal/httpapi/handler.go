package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/lukmi/messaging-service/internal/model"
	"github.com/lukmi/messaging-service/internal/repository"
	"github.com/lukmi/messaging-service/internal/service"
	"github.com/lukmi/messaging-service/internal/websocket"
)

type Handler struct {
	repo    repository.Repository
	service *service.Service
	hub     *websocket.Hub
}

func New(repo repository.Repository, svc *service.Service, hub *websocket.Hub) *Handler {
	return &Handler{repo: repo, service: svc, hub: hub}
}
func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ready"}) })
	mux.HandleFunc("/api/v1/ws", h.ws)
	mux.HandleFunc("/api/v1/conversations", h.conversations)
	mux.HandleFunc("/api/v1/conversations/", h.conversation)
	return requestID(auth(mux))
}

type contextKey string

const userContextKey contextKey = "user_id"

func auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID := r.Header.Get("X-User-ID")
		if userID == "" && strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			userID = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		}
		if userID == "" {
			writeJSON(w, 401, map[string]string{"error": "authentication required"})
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userContextKey, userID)))
	})
}
func currentUser(r *http.Request) string {
	value, _ := r.Context().Value(userContextKey).(string)
	return value
}
func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = strconv.FormatInt(time.Now().UnixNano(), 10)
		}
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r)
	})
}
func (h *Handler) ws(w http.ResponseWriter, r *http.Request) { h.hub.ServeHTTP(w, r, currentUser(r)) }
func (h *Handler) conversations(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, 405, map[string]string{"error": "method not allowed"})
		return
	}
	var input struct {
		Type      string   `json:"conversation_type"`
		MemberIDs []string `json:"member_ids"`
	}
	if json.NewDecoder(r.Body).Decode(&input) != nil || len(input.MemberIDs) == 0 {
		writeJSON(w, 400, map[string]string{"error": "invalid conversation"})
		return
	}
	id := strconv.FormatInt(time.Now().UnixNano(), 10)
	now := time.Now().UTC()
	c := &model.Conversation{ID: id, Type: input.Type, CreatedBy: currentUser(r), CreatedAt: now, UpdatedAt: now}
	members := append(input.MemberIDs, currentUser(r))
	if err := h.repo.CreateConversation(r.Context(), c, members); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 201, c)
}
func (h *Handler) conversation(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 4 {
		writeJSON(w, 404, map[string]string{"error": "not found"})
		return
	}
	conversationID := parts[3]
	member, err := h.repo.IsMember(r.Context(), conversationID, currentUser(r))
	if err != nil || !member {
		writeJSON(w, 403, map[string]string{"error": "conversation access denied"})
		return
	}
	if len(parts) == 4 && r.Method == http.MethodGet {
		c, getErr := h.repo.GetConversation(r.Context(), conversationID)
		if getErr != nil {
			writeError(w, getErr)
			return
		}
		writeJSON(w, 200, c)
		return
	}
	if len(parts) < 5 {
		writeJSON(w, 405, map[string]string{"error": "method not allowed"})
		return
	}
	switch parts[4] {
	case "members":
		members, getErr := h.repo.Members(r.Context(), conversationID)
		if getErr != nil {
			writeError(w, getErr)
			return
		}
		writeJSON(w, 200, members)
	case "messages":
		h.messages(w, r, conversationID)
	default:
		writeJSON(w, 404, map[string]string{"error": "not found"})
	}
}
func (h *Handler) messages(w http.ResponseWriter, r *http.Request, conversationID string) {
	if r.Method == http.MethodGet {
		limit := 50
		if value, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && value > 0 && value <= 100 {
			limit = value
		}
		messages, err := h.repo.ListMessages(r.Context(), conversationID, r.URL.Query().Get("cursor"), limit)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, 200, map[string]any{"messages": messages})
		return
	}
	if r.Method == http.MethodPost {
		var input model.Message
		if json.NewDecoder(r.Body).Decode(&input) != nil {
			writeJSON(w, 400, map[string]string{"error": "invalid message"})
			return
		}
		created, err := h.service.SendMessage(r.Context(), currentUser(r), conversationID, input)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, 201, created)
		return
	}
	writeJSON(w, 405, map[string]string{"error": "method not allowed"})
}
func writeError(w http.ResponseWriter, err error) {
	status := 500
	if errors.Is(err, repository.ErrNotFound) {
		status = 404
	}
	if errors.Is(err, repository.ErrNotMember) {
		status = 403
	}
	if errors.Is(err, service.ErrInvalidMessage) {
		status = 400
	}
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
