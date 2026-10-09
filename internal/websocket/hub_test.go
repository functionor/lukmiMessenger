package websocket

import "testing"

func TestHubTracksMultipleDevicesPerUser(t *testing.T) { hub := NewHub(); if hub.Connected("sarah") != 0 { t.Fatal("new hub has an unexpected connection") } }
