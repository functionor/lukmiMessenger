package websocket

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

type Event struct {
	Type string `json:"type"`
	Data any    `json:"data"`
}
type client struct {
	conn *websocket.Conn
	send chan []byte
}
type Hub struct {
	mu       sync.RWMutex
	users    map[string]map[*client]struct{}
	upgrader websocket.Upgrader
}

func NewHub() *Hub {
	return &Hub{users: map[string]map[*client]struct{}{}, upgrader: websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}}
}
func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request, userID string) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	c := &client{conn: conn, send: make(chan []byte, 32)}
	h.mu.Lock()
	if h.users[userID] == nil {
		h.users[userID] = map[*client]struct{}{}
	}
	h.users[userID][c] = struct{}{}
	h.mu.Unlock()
	go h.writeLoop(userID, c)
	h.readLoop(userID, c)
}
func (h *Hub) readLoop(userID string, c *client) {
	defer h.remove(userID, c)
	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			return
		}
	}
}
func (h *Hub) writeLoop(userID string, c *client) {
	defer h.remove(userID, c)
	for payload := range c.send {
		if err := c.conn.WriteMessage(websocket.TextMessage, payload); err != nil {
			return
		}
	}
}
func (h *Hub) remove(userID string, c *client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if users := h.users[userID]; users != nil {
		delete(users, c)
		if len(users) == 0 {
			delete(h.users, userID)
		}
	}
	close(c.send)
	_ = c.conn.Close()
}
func (h *Hub) Broadcast(userIDs []string, eventType string, data any) {
	payload, err := json.Marshal(Event{Type: eventType, Data: data})
	if err != nil {
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	seen := map[*client]bool{}
	for _, userID := range userIDs {
		for c := range h.users[userID] {
			if !seen[c] {
				select {
				case c.send <- payload:
				default:
				}
				seen[c] = true
			}
		}
	}
}
func (h *Hub) Connected(userID string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.users[userID])
}
