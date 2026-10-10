package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/lukmi/messaging-service/internal/cache"
	"github.com/lukmi/messaging-service/internal/config"
	"github.com/lukmi/messaging-service/internal/kafka"
	"github.com/lukmi/messaging-service/internal/middleware"
	"github.com/lukmi/messaging-service/internal/model"
	"github.com/lukmi/messaging-service/internal/repository"
	"github.com/lukmi/messaging-service/internal/response"
	"github.com/lukmi/messaging-service/internal/service"
	"github.com/lukmi/messaging-service/internal/websocket"
)

const MaxBodyBytes = 1024 * 1024 // 1 MB payload limit

type Handler struct {
	cfg       *config.Config
	repo      repository.Repository
	service   *service.Service
	hub       *websocket.Hub
	cache     cache.Cache
	publisher kafka.EventPublisher
}

func New(cfg *config.Config, repo repository.Repository, svc *service.Service, hub *websocket.Hub, c cache.Cache, publisher kafka.EventPublisher) *Handler {
	return &Handler{
		cfg:       cfg,
		repo:      repo,
		service:   svc,
		hub:       hub,
		cache:     c,
		publisher: publisher,
	}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", h.healthz)
	mux.HandleFunc("/readyz", h.readyz)
	mux.HandleFunc("/api/v1/ws", h.ws)
	mux.HandleFunc("/api/v1/conversations", h.conversations)
	mux.HandleFunc("/api/v1/conversations/", h.conversation)

	authMiddleware := middleware.Auth(h.cfg, nil)
	return middleware.RequestID(authMiddleware(mux))
}

func (h *Handler) healthz(w http.ResponseWriter, _ *http.Request) {
	response.JSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) readyz(w http.ResponseWriter, r *http.Request) {
	if err := h.repo.Ping(r.Context()); err != nil {
		response.Error(w, http.StatusServiceUnavailable, "database unready: "+err.Error())
		return
	}
	if h.cache != nil {
		if err := h.cache.Ping(r.Context()); err != nil {
			response.Error(w, http.StatusServiceUnavailable, "cache unready: "+err.Error())
			return
		}
	}
	response.JSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (h *Handler) ws(w http.ResponseWriter, r *http.Request) {
	userID := middleware.GetUserID(r.Context())
	h.hub.ServeHTTP(w, r, userID)
}

func (h *Handler) conversations(w http.ResponseWriter, r *http.Request) {
	userID := middleware.GetUserID(r.Context())

	switch r.Method {
	case http.MethodPost:
		r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
		var input struct {
			Type      string   `json:"conversation_type"`
			MemberIDs []string `json:"member_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			response.Error(w, http.StatusBadRequest, "invalid conversation json payload")
			return
		}
		conv, err := h.service.CreateConversation(r.Context(), userID, input.Type, input.MemberIDs)
		if err != nil {
			h.writeError(w, err)
			return
		}
		response.JSON(w, http.StatusCreated, conv)

	case http.MethodGet:
		limit, err := parseLimit(r.URL.Query().Get("limit"), 50)
		if err != nil {
			response.Error(w, http.StatusBadRequest, err.Error())
			return
		}
		cursor := r.URL.Query().Get("cursor")

		convs, nextCursor, err := h.service.ListConversations(r.Context(), userID, cursor, limit)
		if err != nil {
			h.writeError(w, err)
			return
		}

		res := map[string]any{"conversations": convs}
		if nextCursor != "" {
			res["next_cursor"] = nextCursor
		}
		response.JSON(w, http.StatusOK, res)

	default:
		response.Error(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *Handler) conversation(w http.ResponseWriter, r *http.Request) {
	userID := middleware.GetUserID(r.Context())
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")

	if len(parts) < 4 {
		response.Error(w, http.StatusNotFound, "resource not found")
		return
	}

	conversationID := parts[3]

	if len(parts) == 4 {
		if r.Method == http.MethodGet {
			conv, err := h.service.GetConversation(r.Context(), userID, conversationID)
			if err != nil {
				h.writeError(w, err)
				return
			}
			response.JSON(w, http.StatusOK, conv)
			return
		}
		response.Error(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	switch parts[4] {
	case "members":
		if r.Method == http.MethodGet {
			members, err := h.service.GetMembers(r.Context(), userID, conversationID)
			if err != nil {
				h.writeError(w, err)
				return
			}
			response.JSON(w, http.StatusOK, map[string]any{"members": members})
			return
		}
		response.Error(w, http.StatusMethodNotAllowed, "method not allowed")

	case "leave":
		if r.Method == http.MethodPost {
			if err := h.service.LeaveConversation(r.Context(), userID, conversationID); err != nil {
				h.writeError(w, err)
				return
			}
			response.JSON(w, http.StatusOK, map[string]string{"status": "left_conversation"})
			return
		}
		response.Error(w, http.StatusMethodNotAllowed, "method not allowed")

	case "messages":
		h.handleMessages(w, r, userID, conversationID, parts[5:])

	case "read":
		if r.Method == http.MethodPost {
			r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
			var input struct {
				MessageID string `json:"message_id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&input)
			if input.MessageID == "" {
				response.Error(w, http.StatusBadRequest, "message_id required")
				return
			}
			read, err := h.service.MarkRead(r.Context(), userID, conversationID, input.MessageID)
			if err != nil {
				h.writeError(w, err)
				return
			}
			response.JSON(w, http.StatusOK, read)
			return
		}
		response.Error(w, http.StatusMethodNotAllowed, "method not allowed")

	default:
		response.Error(w, http.StatusNotFound, "resource not found")
	}
}

func (h *Handler) handleMessages(w http.ResponseWriter, r *http.Request, userID, conversationID string, subParts []string) {
	if len(subParts) == 0 {
		switch r.Method {
		case http.MethodGet:
			limit, err := parseLimit(r.URL.Query().Get("limit"), 50)
			if err != nil {
				response.Error(w, http.StatusBadRequest, err.Error())
				return
			}
			cursor := r.URL.Query().Get("cursor")

			messages, nextCursor, err := h.service.SyncMessages(r.Context(), userID, conversationID, cursor, limit)
			if err != nil {
				h.writeError(w, err)
				return
			}

			res := map[string]any{"messages": messages}
			if nextCursor != "" {
				res["next_cursor"] = nextCursor
			}
			response.JSON(w, http.StatusOK, res)

		case http.MethodPost:
			r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
			var input model.Message
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				response.Error(w, http.StatusBadRequest, "invalid message json body")
				return
			}
			created, err := h.service.SendMessage(r.Context(), userID, conversationID, input)
			if err != nil {
				h.writeError(w, err)
				return
			}
			response.JSON(w, http.StatusCreated, created)

		default:
			response.Error(w, http.StatusMethodNotAllowed, "method not allowed")
		}
		return
	}

	messageID := subParts[0]

	if len(subParts) == 1 {
		if r.Method == http.MethodDelete {
			if err := h.service.DeleteMessage(r.Context(), userID, conversationID, messageID); err != nil {
				h.writeError(w, err)
				return
			}
			response.JSON(w, http.StatusOK, map[string]string{"status": "deleted"})
			return
		}
		response.Error(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if len(subParts) == 2 && subParts[1] == "read" {
		if r.Method == http.MethodPost {
			read, err := h.service.MarkRead(r.Context(), userID, conversationID, messageID)
			if err != nil {
				h.writeError(w, err)
				return
			}
			response.JSON(w, http.StatusOK, read)
			return
		}
		response.Error(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	response.Error(w, http.StatusNotFound, "resource not found")
}

func (h *Handler) writeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, repository.ErrNotFound):
		response.Error(w, http.StatusNotFound, "resource not found")
	case errors.Is(err, repository.ErrNotMember):
		response.Error(w, http.StatusForbidden, "access denied: not a conversation member")
	case errors.Is(err, repository.ErrUnauthorized):
		response.Error(w, http.StatusForbidden, "unauthorized operation")
	case errors.Is(err, service.ErrInvalidMessage), errors.Is(err, service.ErrInvalidReference), errors.Is(err, repository.ErrInvalidCursor):
		response.Error(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, repository.ErrAlreadyExists):
		response.Error(w, http.StatusConflict, "resource already exists")
	default:
		response.Error(w, http.StatusInternalServerError, "internal server error")
	}
}

func parseLimit(raw string, fallback int) (int, error) {
	if raw == "" {
		return fallback, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit <= 0 || limit > 100 {
		return 0, fmt.Errorf("invalid limit parameter: must be integer between 1 and 100")
	}
	return limit, nil
}
