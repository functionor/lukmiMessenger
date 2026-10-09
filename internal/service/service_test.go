package service

import (
	"context"
	"errors"
	"testing"
	"github.com/lukmi/messaging-service/internal/model"
	"github.com/lukmi/messaging-service/internal/repository"
)

type testPublisher struct { events []Event }
func (p *testPublisher) Publish(_ context.Context, event Event) error { p.events = append(p.events, event); return nil }
type testBroadcaster struct { userIDs []string; eventType string; payload any }
func (b *testBroadcaster) Broadcast(userIDs []string, eventType string, payload any) { b.userIDs, b.eventType, b.payload = userIDs, eventType, payload }
func testService() (*Service, *repository.MemoryRepository, *testPublisher) { repo := repository.NewMemoryRepository(); publisher := &testPublisher{}; svc := New(repo, publisher, &testBroadcaster{}); now := int64(1); svc.id = func() string { now++; return "id" }; return svc, repo, publisher }
func createConversation(t *testing.T, repo *repository.MemoryRepository) { t.Helper(); if err := repo.CreateConversation(context.Background(), &model.Conversation{ID:"conversation-1"}, []string{"john", "sarah"}); err != nil { t.Fatal(err) } }
func TestSendTextMessageUsesAuthenticatedSenderAndPublishes(t *testing.T) { svc, repo, publisher := testService(); createConversation(t, repo); message, err := svc.SendMessage(context.Background(), "john", "conversation-1", model.Message{Type:model.Text, TextContent:"hello", SenderID:"attacker", ClientID:"client-1"}); if err != nil { t.Fatal(err) }; if message.SenderID != "john" { t.Fatalf("sender = %q, want john", message.SenderID) }; if len(publisher.events) != 1 || publisher.events[0].Type != "MESSAGE_SENT" { t.Fatalf("unexpected events: %+v", publisher.events) } }
func TestSendSharedPostStoresReferenceWithoutFetchingResource(t *testing.T) { svc, repo, _ := testService(); createConversation(t, repo); message, err := svc.SendMessage(context.Background(), "john", "conversation-1", model.Message{Type:model.PostShare, ReferenceID:"post-123"}); if err != nil { t.Fatal(err) }; if message.ReferenceID != "post-123" || message.TextContent != "" { t.Fatalf("unexpected shared message: %+v", message) } }
func TestSendRejectsNonMember(t *testing.T) { svc, repo, _ := testService(); createConversation(t, repo); _, err := svc.SendMessage(context.Background(), "mallory", "conversation-1", model.Message{Type:model.Text, TextContent:"nope"}); if !errors.Is(err, repository.ErrNotMember) { t.Fatalf("error = %v, want non-member", err) } }
func TestSendIsIdempotentByClientID(t *testing.T) { svc, repo, _ := testService(); createConversation(t, repo); input := model.Message{Type:model.Text, TextContent:"retry", ClientID:"same"}; first, err := svc.SendMessage(context.Background(), "john", "conversation-1", input); if err != nil { t.Fatal(err) }; second, err := svc.SendMessage(context.Background(), "john", "conversation-1", input); if err != nil { t.Fatal(err) }; if first.ID != second.ID { t.Fatalf("IDs differ: %q and %q", first.ID, second.ID) }; messages, _ := repo.ListMessages(context.Background(), "conversation-1", "", 10); if len(messages) != 1 { t.Fatalf("stored %d messages, want 1", len(messages)) } }
