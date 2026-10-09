package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	gorillaWS "github.com/gorilla/websocket"
	"github.com/lukmi/messaging-service/internal/cache"
	"github.com/lukmi/messaging-service/internal/config"
	"github.com/lukmi/messaging-service/internal/httpapi"
	"github.com/lukmi/messaging-service/internal/kafka"
	"github.com/lukmi/messaging-service/internal/model"
	"github.com/lukmi/messaging-service/internal/repository"
	"github.com/lukmi/messaging-service/internal/service"
	ws "github.com/lukmi/messaging-service/internal/websocket"
)

func testCfg() *config.Config {
	return &config.Config{
		Env:           "development",
		JWTSecret:     "test-jwt-secret-32bytes-minimum-key!!",
		GatewaySecret: "test-gateway-secret-key-16bytes!",
	}
}

func generateJWT(userID, secret string) string {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": userID,
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	tokenStr, _ := token.SignedString([]byte(secret))
	return tokenStr
}

func TestWebSocketRealtimeDeliveryAndMultiDevice(t *testing.T) {
	cfg := testCfg()
	repo := repository.NewMemoryRepository()
	memCache := cache.NewMemoryCache()
	publisher := kafka.NewMemoryPublisher(nil)
	hub := ws.NewHub(cfg, memCache, nil)
	svc := service.New(repo, publisher, hub)
	h := httpapi.New(cfg, repo, svc, hub, memCache, publisher)

	server := httptest.NewServer(h.Routes())
	defer server.Close()

	// 1. Create conversation between John and Sarah
	conv, err := svc.CreateConversation(context.Background(), "john", "direct", []string{"sarah"})
	if err != nil {
		t.Fatalf("create conversation failed: %v", err)
	}

	sarahToken := generateJWT("sarah", cfg.JWTSecret)
	wsURL := "ws" + stringsTrimPrefix(server.URL, "http") + "/api/v1/ws?token=" + sarahToken

	// Connect Sarah Device 1
	wsConn1, _, err := gorillaWS.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial Sarah device 1 failed: %v", err)
	}
	defer wsConn1.Close()

	// Connect Sarah Device 2 (Multi-device requirement)
	wsConn2, _, err := gorillaWS.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial Sarah device 2 failed: %v", err)
	}
	defer wsConn2.Close()

	// 2. John sends message via REST API
	msgBody, _ := json.Marshal(map[string]any{
		"message_type": "TEXT",
		"text_content": "Realtime notification test",
	})

	req, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/conversations/"+conv.ID+"/messages", bytes.NewBuffer(msgBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "john")
	req.Header.Set("X-Gateway-Secret", cfg.GatewaySecret)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("send message failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("send message status = %d, want 201", resp.StatusCode)
	}

	// 3. Both of Sarah's WebSocket connections should receive message.new event
	for _, conn := range []*gorillaWS.Conn{wsConn1, wsConn2} {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("failed reading websocket message: %v", err)
		}

		var event ws.Event
		if err := json.Unmarshal(payload, &event); err != nil {
			t.Fatalf("failed to unmarshal ws event: %v", err)
		}

		if event.Type != "message.new" {
			t.Fatalf("expected ws event message.new, got %s", event.Type)
		}
	}
}

func TestE2EEAndSharedContentEndToEnd(t *testing.T) {
	cfg := testCfg()
	repo := repository.NewMemoryRepository()
	memCache := cache.NewMemoryCache()
	publisher := kafka.NewMemoryPublisher(nil)
	hub := ws.NewHub(cfg, memCache, nil)
	svc := service.New(repo, publisher, hub)
	h := httpapi.New(cfg, repo, svc, hub, memCache, publisher)

	server := httptest.NewServer(h.Routes())
	defer server.Close()

	conv, _ := svc.CreateConversation(context.Background(), "john", "direct", []string{"sarah"})

	// 1. Post Share Message
	postShareBody, _ := json.Marshal(map[string]any{
		"message_type": "POST_SHARE",
		"reference_id": "post-uuid-1010",
		"preview": map[string]string{
			"username": "alex",
			"caption":  "Beautiful view",
		},
	})

	req, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/conversations/"+conv.ID+"/messages", bytes.NewBuffer(postShareBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "john")
	req.Header.Set("X-Gateway-Secret", cfg.GatewaySecret)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post share request failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}

	var msg model.Message
	_ = json.NewDecoder(resp.Body).Decode(&msg)
	if msg.Type != model.PostShare || msg.ReferenceID != "post-uuid-1010" {
		t.Fatalf("unexpected message: %+v", msg)
	}
	if msg.Preview == nil || msg.Preview.Username != "alex" {
		t.Fatalf("unexpected preview: %+v", msg.Preview)
	}

	// 2. Encrypted E2EE Message
	e2eeBody, _ := json.Marshal(map[string]any{
		"message_type": "TEXT",
		"ciphertext":   "GCM_AES256_CIPHERTEXT_SAMPLE",
	})

	e2eeReq, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/conversations/"+conv.ID+"/messages", bytes.NewBuffer(e2eeBody))
	e2eeReq.Header.Set("Content-Type", "application/json")
	e2eeReq.Header.Set("X-User-ID", "john")
	e2eeReq.Header.Set("X-Gateway-Secret", cfg.GatewaySecret)

	e2eeResp, err := http.DefaultClient.Do(e2eeReq)
	if err != nil {
		t.Fatalf("e2ee request failed: %v", err)
	}
	if e2eeResp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", e2eeResp.StatusCode)
	}

	var e2eeMsg model.Message
	_ = json.NewDecoder(e2eeResp.Body).Decode(&e2eeMsg)
	if e2eeMsg.Ciphertext != "GCM_AES256_CIPHERTEXT_SAMPLE" {
		t.Fatalf("unexpected ciphertext: %+v", e2eeMsg)
	}
}

func stringsTrimPrefix(s, prefix string) string {
	if len(s) >= len(prefix) && s[:len(prefix)] == prefix {
		return s[len(prefix):]
	}
	return s
}
