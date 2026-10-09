package websocket

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/lukmi/messaging-service/internal/cache"
	"github.com/lukmi/messaging-service/internal/config"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 512 * 1024 // 512 KB
)

type Event struct {
	Type string `json:"type"`
	Data any    `json:"data"`
}

type BroadcastPayload struct {
	UserIDs []string `json:"user_ids"`
	Event   Event    `json:"event"`
}

type client struct {
	conn      *websocket.Conn
	send      chan []byte
	userID    string
	closeOnce sync.Once
}

type Hub struct {
	mu             sync.RWMutex
	users          map[string]map[*client]struct{}
	upgrader       websocket.Upgrader
	cache          cache.Cache
	logger         *slog.Logger
	cancel         func()
	allowedOrigins []string
}

func NewHub(cfg *config.Config, c cache.Cache, logger *slog.Logger) *Hub {
	var origins []string
	if cfg != nil {
		origins = cfg.AllowedOrigins
	}

	h := &Hub{
		users:          make(map[string]map[*client]struct{}),
		cache:          c,
		logger:         logger,
		allowedOrigins: origins,
	}

	h.upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     h.checkOrigin,
	}

	if c != nil {
		h.subscribeRedisEvents()
	}

	return h
}

func (h *Hub) checkOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true // Permit native clients (Flutter Android/iOS) without Origin header
	}

	if len(h.allowedOrigins) == 0 {
		return true // Development mode default
	}

	u, err := url.Parse(origin)
	if err != nil {
		return false
	}

	for _, allowed := range h.allowedOrigins {
		if allowed == "*" || stringsEqualFold(allowed, u.Host) || stringsEqualFold(allowed, origin) {
			return true
		}
	}

	if h.logger != nil {
		h.logger.Warn("websocket origin rejected", "origin", origin)
	}
	return false
}

func (h *Hub) subscribeRedisEvents() {
	ctx := context.Background()
	ch, cancel, err := h.cache.Subscribe(ctx, "messaging:ws_events")
	if err != nil {
		if h.logger != nil {
			h.logger.Error("failed to subscribe to redis ws events", "error", err)
		}
		return
	}
	h.cancel = cancel

	go func() {
		for payload := range ch {
			var bp BroadcastPayload
			if err := json.Unmarshal(payload, &bp); err != nil {
				continue
			}
			h.broadcastLocal(bp.UserIDs, bp.Event)
		}
	}()
}

func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request, userID string) {
	if userID == "" {
		http.Error(w, "Unauthorized: user_id required", http.StatusUnauthorized)
		return
	}

	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		if h.logger != nil {
			h.logger.Error("websocket upgrade error", "user_id", userID, "error", err)
		}
		return
	}

	c := &client{
		conn:   conn,
		send:   make(chan []byte, 64),
		userID: userID,
	}

	h.register(userID, c)

	go h.writeLoop(c)
	go h.readLoop(c)
}

func (h *Hub) register(userID string, c *client) {
	h.mu.Lock()
	if h.users[userID] == nil {
		h.users[userID] = make(map[*client]struct{})
	}
	h.users[userID][c] = struct{}{}
	h.mu.Unlock()

	if h.cache != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = h.cache.IncConnections(ctx, userID)
		_ = h.cache.SetPresence(ctx, userID, "online", 2*time.Minute)
	}

	if h.logger != nil {
		h.logger.Info("websocket client connected", "user_id", userID, "local_devices", h.Connected(userID))
	}
}

func (h *Hub) remove(c *client) {
	c.closeOnce.Do(func() {
		userID := c.userID

		h.mu.Lock()
		if clients, ok := h.users[userID]; ok {
			delete(clients, c)
			if len(clients) == 0 {
				delete(h.users, userID)
			}
		}
		h.mu.Unlock()

		_ = c.conn.Close()

		if h.cache != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			remaining, _ := h.cache.DecConnections(ctx, userID)
			if remaining <= 0 {
				_ = h.cache.SetPresence(ctx, userID, "offline", 2*time.Minute)
			}
		}

		if h.logger != nil {
			h.logger.Info("websocket client disconnected", "user_id", userID)
		}
	})
}

func (h *Hub) readLoop(c *client) {
	defer h.remove(c)

	c.conn.SetReadLimit(maxMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
		if h.cache != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			_ = h.cache.SetPresence(ctx, c.userID, "online", 2*time.Minute)
		}
		return nil
	})

	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

func (h *Hub) writeLoop(c *client) {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		h.remove(c)
	}()

	for {
		select {
		case payload, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				return
			}

		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (h *Hub) Broadcast(userIDs []string, eventType string, data any) {
	event := Event{Type: eventType, Data: data}

	h.broadcastLocal(userIDs, event)

	if h.cache != nil {
		bp := BroadcastPayload{UserIDs: userIDs, Event: event}
		if payload, err := json.Marshal(bp); err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = h.cache.PublishEvent(ctx, "messaging:ws_events", payload)
		}
	}
}

func (h *Hub) broadcastLocal(userIDs []string, event Event) {
	payload, err := json.Marshal(event)
	if err != nil {
		return
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	seen := make(map[*client]bool)
	for _, userID := range userIDs {
		if clients, ok := h.users[userID]; ok {
			for c := range clients {
				if !seen[c] {
					select {
					case c.send <- payload:
					default:
						if h.logger != nil {
							h.logger.Warn("ws client buffer full, dropping message", "user_id", userID)
						}
					}
					seen[c] = true
				}
			}
		}
	}
}

func (h *Hub) Connected(userID string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.users[userID])
}

func (h *Hub) Close() {
	if h.cancel != nil {
		h.cancel()
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	for _, clients := range h.users {
		for c := range clients {
			_ = c.conn.Close()
		}
	}
	h.users = make(map[string]map[*client]struct{})
}

func stringsEqualFold(s1, s2 string) bool {
	return url.QueryEscape(s1) == url.QueryEscape(s2)
}
