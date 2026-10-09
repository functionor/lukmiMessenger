package cache

import (
	"context"
	"testing"
	"time"
)

func TestMemoryCachePresenceAndConnections(t *testing.T) {
	c := NewMemoryCache()
	ctx := context.Background()

	// Presence
	if status, _ := c.GetPresence(ctx, "user-1"); status != "offline" {
		t.Fatalf("expected offline, got %s", status)
	}

	_ = c.SetPresence(ctx, "user-1", "online", time.Hour)
	if status, _ := c.GetPresence(ctx, "user-1"); status != "online" {
		t.Fatalf("expected online, got %s", status)
	}

	// Connection counting
	c1, _ := c.IncConnections(ctx, "user-1")
	if c1 != 1 {
		t.Fatalf("connections = %d, want 1", c1)
	}

	c2, _ := c.IncConnections(ctx, "user-1")
	if c2 != 2 {
		t.Fatalf("connections = %d, want 2", c2)
	}

	c3, _ := c.DecConnections(ctx, "user-1")
	if c3 != 1 {
		t.Fatalf("connections = %d, want 1", c3)
	}
}

func TestMemoryCachePubSub(t *testing.T) {
	c := NewMemoryCache()
	ctx := context.Background()

	ch, cancel, err := c.Subscribe(ctx, "test:channel")
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	defer cancel()

	_ = c.PublishEvent(ctx, "test:channel", []byte("hello world"))

	select {
	case msg := <-ch:
		if string(msg) != "hello world" {
			t.Fatalf("received %s, want hello world", string(msg))
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for pubsub event")
	}
}
