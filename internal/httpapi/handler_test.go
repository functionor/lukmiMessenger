package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lukmi/messaging-service/internal/cache"
	"github.com/lukmi/messaging-service/internal/config"
	"github.com/lukmi/messaging-service/internal/kafka"
	"github.com/lukmi/messaging-service/internal/model"
	"github.com/lukmi/messaging-service/internal/repository"
	"github.com/lukmi/messaging-service/internal/service"
	"github.com/lukmi/messaging-service/internal/websocket"
)

func testCfg() *config.Config {
	return &config.Config{
		Env:           "development",
		JWTSecret:     "test-jwt-secret-32bytes-minimum-key!!",
		GatewaySecret: "test-gateway-secret-key-16bytes!",
	}
}

func TestHealthAndReadyEndpoints(t *testing.T) {
	cfg := testCfg()
	repo := repository.NewMemoryRepository()
	memCache := cache.NewMemoryCache()
	publisher := kafka.NewMemoryPublisher(nil)
	hub := websocket.NewHub(cfg, memCache, nil)
	svc := service.New(repo, publisher, hub)
	h := New(cfg, repo, svc, hub, memCache, publisher)

	server := httptest.NewServer(h.Routes())
	defer server.Close()

	// Healthz
	resp, err := http.Get(server.URL + "/healthz")
	if err != nil {
		t.Fatalf("healthz request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz status = %d, want 200", resp.StatusCode)
	}

	// Readyz
	resp, err = http.Get(server.URL + "/readyz")
	if err != nil {
		t.Fatalf("readyz request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("readyz status = %d, want 200", resp.StatusCode)
	}
}

func TestCreateAndGetConversationFlow(t *testing.T) {
	cfg := testCfg()
	repo := repository.NewMemoryRepository()
	memCache := cache.NewMemoryCache()
	publisher := kafka.NewMemoryPublisher(nil)
	hub := websocket.NewHub(cfg, memCache, nil)
	svc := service.New(repo, publisher, hub)
	h := New(cfg, repo, svc, hub, memCache, publisher)

	server := httptest.NewServer(h.Routes())
	defer server.Close()

	// 1. Create conversation as "john" with member "sarah"
	reqBody, _ := json.Marshal(map[string]any{
		"conversation_type": "direct",
		"member_ids":        []string{"sarah"},
	})

	req, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/conversations", bytes.NewBuffer(reqBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "john")
	req.Header.Set("X-Gateway-Secret", cfg.GatewaySecret)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("create conversation request failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create conversation status = %d, want 201", resp.StatusCode)
	}

	var conv model.Conversation
	_ = json.NewDecoder(resp.Body).Decode(&conv)
	if conv.ID == "" || conv.CreatedBy != "john" {
		t.Fatalf("invalid created conversation: %+v", conv)
	}

	// 2. Fetch created conversation as "sarah" (member)
	getReq, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/conversations/"+conv.ID, nil)
	getReq.Header.Set("X-User-ID", "sarah")
	getReq.Header.Set("X-Gateway-Secret", cfg.GatewaySecret)

	getResp, err := client.Do(getReq)
	if err != nil {
		t.Fatalf("get conversation failed: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("get conversation status = %d, want 200", getResp.StatusCode)
	}

	// 3. Unauthorized access attempt by non-member "mallory"
	malloryReq, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/conversations/"+conv.ID, nil)
	malloryReq.Header.Set("X-User-ID", "mallory")
	malloryReq.Header.Set("X-Gateway-Secret", cfg.GatewaySecret)

	malloryResp, err := client.Do(malloryReq)
	if err != nil {
		t.Fatalf("mallory get conversation failed: %v", err)
	}
	if malloryResp.StatusCode != http.StatusForbidden {
		t.Fatalf("non-member status = %d, want 403 Forbidden", malloryResp.StatusCode)
	}
}

func TestSendMessageAndSyncFlow(t *testing.T) {
	cfg := testCfg()
	repo := repository.NewMemoryRepository()
	memCache := cache.NewMemoryCache()
	publisher := kafka.NewMemoryPublisher(nil)
	hub := websocket.NewHub(cfg, memCache, nil)
	svc := service.New(repo, publisher, hub)
	h := New(cfg, repo, svc, hub, memCache, publisher)

	server := httptest.NewServer(h.Routes())
	defer server.Close()

	client := &http.Client{}

	c, _ := svc.CreateConversation(context.Background(), "john", "direct", []string{"sarah"})

	// Send normal text message
	msgBody, _ := json.Marshal(map[string]any{
		"message_type": "TEXT",
		"text_content": "Hello Sarah!",
		"client_id":    "client-msg-1",
	})
	sendReq, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/conversations/"+c.ID+"/messages", bytes.NewBuffer(msgBody))
	sendReq.Header.Set("Content-Type", "application/json")
	sendReq.Header.Set("X-User-ID", "john")
	sendReq.Header.Set("X-Gateway-Secret", cfg.GatewaySecret)

	sendResp, err := client.Do(sendReq)
	if err != nil {
		t.Fatalf("send message failed: %v", err)
	}
	if sendResp.StatusCode != http.StatusCreated {
		t.Fatalf("send message status = %d, want 201", sendResp.StatusCode)
	}

	var sentMsg model.Message
	_ = json.NewDecoder(sendResp.Body).Decode(&sentMsg)
	if sentMsg.ID == "" || sentMsg.TextContent != "Hello Sarah!" {
		t.Fatalf("unexpected sent message: %+v", sentMsg)
	}

	// Send shared post reference message
	shareBody, _ := json.Marshal(map[string]any{
		"message_type": "POST_SHARE",
		"reference_id": "post-xyz-999",
	})
	shareReq, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/conversations/"+c.ID+"/messages", bytes.NewBuffer(shareBody))
	shareReq.Header.Set("Content-Type", "application/json")
	shareReq.Header.Set("X-User-ID", "john")
	shareReq.Header.Set("X-Gateway-Secret", cfg.GatewaySecret)

	shareResp, err := client.Do(shareReq)
	if err != nil {
		t.Fatalf("share message failed: %v", err)
	}
	if shareResp.StatusCode != http.StatusCreated {
		t.Fatalf("share message status = %d, want 201", shareResp.StatusCode)
	}

	var sharedMsg model.Message
	_ = json.NewDecoder(shareResp.Body).Decode(&sharedMsg)
	if sharedMsg.Type != model.PostShare || sharedMsg.ReferenceID != "post-xyz-999" {
		t.Fatalf("unexpected shared message response: %+v", sharedMsg)
	}

	// Sync messages as Sarah
	syncReq, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/conversations/"+c.ID+"/messages", nil)
	syncReq.Header.Set("X-User-ID", "sarah")
	syncReq.Header.Set("X-Gateway-Secret", cfg.GatewaySecret)

	syncResp, err := client.Do(syncReq)
	if err != nil {
		t.Fatalf("sync messages failed: %v", err)
	}
	if syncResp.StatusCode != http.StatusOK {
		t.Fatalf("sync messages status = %d, want 200", syncResp.StatusCode)
	}

	var syncData struct {
		Messages []*model.Message `json:"messages"`
	}
	_ = json.NewDecoder(syncResp.Body).Decode(&syncData)
	if len(syncData.Messages) != 2 {
		t.Fatalf("sync returned %d messages, want 2", len(syncData.Messages))
	}
}

func TestMarkReadAndDeleteEndpoints(t *testing.T) {
	cfg := testCfg()
	repo := repository.NewMemoryRepository()
	memCache := cache.NewMemoryCache()
	publisher := kafka.NewMemoryPublisher(nil)
	hub := websocket.NewHub(cfg, memCache, nil)
	svc := service.New(repo, publisher, hub)
	h := New(cfg, repo, svc, hub, memCache, publisher)

	server := httptest.NewServer(h.Routes())
	defer server.Close()

	client := &http.Client{}

	c, _ := svc.CreateConversation(context.Background(), "john", "direct", []string{"sarah"})
	msg, _ := svc.SendMessage(context.Background(), "john", c.ID, model.Message{
		Type:        model.Text,
		TextContent: "Read or Delete me",
	})

	// Mark read
	readReq, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/conversations/"+c.ID+"/messages/"+msg.ID+"/read", nil)
	readReq.Header.Set("X-User-ID", "sarah")
	readReq.Header.Set("X-Gateway-Secret", cfg.GatewaySecret)

	readResp, err := client.Do(readReq)
	if err != nil {
		t.Fatalf("mark read failed: %v", err)
	}
	if readResp.StatusCode != http.StatusOK {
		t.Fatalf("mark read status = %d, want 200", readResp.StatusCode)
	}

	// Delete message
	delReq, _ := http.NewRequest(http.MethodDelete, server.URL+"/api/v1/conversations/"+c.ID+"/messages/"+msg.ID, nil)
	delReq.Header.Set("X-User-ID", "john")
	delReq.Header.Set("X-Gateway-Secret", cfg.GatewaySecret)

	delResp, err := client.Do(delReq)
	if err != nil {
		t.Fatalf("delete message failed: %v", err)
	}
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("delete message status = %d, want 200", delResp.StatusCode)
	}
}

func TestUnauthenticatedRequest(t *testing.T) {
	cfg := testCfg()
	repo := repository.NewMemoryRepository()
	memCache := cache.NewMemoryCache()
	publisher := kafka.NewMemoryPublisher(nil)
	hub := websocket.NewHub(cfg, memCache, nil)
	svc := service.New(repo, publisher, hub)
	h := New(cfg, repo, svc, hub, memCache, publisher)

	server := httptest.NewServer(h.Routes())
	defer server.Close()

	req, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/conversations", nil)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 Unauthorized", resp.StatusCode)
	}
}
