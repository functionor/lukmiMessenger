package websocket

import (
	"testing"

	"github.com/lukmi/messaging-service/internal/cache"
)

func TestHubTracksMultipleDevicesPerUser(t *testing.T) {
	memCache := cache.NewMemoryCache()
	hub := NewHub(memCache, nil)
	defer hub.Close()

	if hub.Connected("sarah") != 0 {
		t.Fatal("new hub has an unexpected connection")
	}
}
