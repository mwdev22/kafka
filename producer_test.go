package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	kgo "github.com/segmentio/kafka-go"
)

type fakeWriter struct {
	mu   sync.Mutex
	msgs []kgo.Message
	err  error
}

func (w *fakeWriter) WriteMessages(_ context.Context, msgs ...kgo.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	w.msgs = append(w.msgs, msgs...)
	return nil
}

func (w *fakeWriter) Close() error { return nil }

func (w *fakeWriter) written() []kgo.Message {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]kgo.Message(nil), w.msgs...)
}

func TestNewProducer(t *testing.T) {
	tests := []struct {
		name    string
		brokers []string
		wantErr error
	}{
		{name: "#1 - OK", brokers: []string{"localhost:9092"}},
		{name: "#2 - FAIL - missing brokers", wantErr: ErrMissingBrokers},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := NewProducer(tt.brokers, WithRequiredAcks(RequireOne))
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("NewProducer() err = %v, want %v", err, tt.wantErr)
			}
			if err == nil {
				if p.opts.RequiredAcks != RequireOne {
					t.Fatalf("option not applied: %v", p.opts.RequiredAcks)
				}
				_ = p.Close()
			}
		})
	}
}

func TestProducerPublish(t *testing.T) {
	writeErr := errors.New("broker down")

	tests := []struct {
		name     string
		topic    string
		key      string
		headers  map[string]string
		writeErr error
		wantErr  error
	}{
		{name: "#1 - OK", topic: "orders", key: "order-1", headers: map[string]string{"trace": "abc"}},
		{name: "#2 - OK - empty key", topic: "orders"},
		{name: "#3 - FAIL - missing topic", wantErr: ErrMissingTopic},
		{name: "#4 - FAIL - write error", topic: "orders", writeErr: writeErr, wantErr: writeErr},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &fakeWriter{err: tt.writeErr}
			p := newProducer(w, defaultProducerOptions())

			err := p.Publish(context.Background(), tt.topic, tt.key, []byte("body"), tt.headers)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Publish() err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}

			msgs := w.written()
			if len(msgs) != 1 {
				t.Fatalf("written %d messages, want 1", len(msgs))
			}
			m := msgs[0]
			if m.Topic != tt.topic || string(m.Value) != "body" {
				t.Fatalf("unexpected message: %+v", m)
			}
			if tt.key == "" && m.Key != nil {
				t.Fatalf("empty key should be nil so the balancer round-robins, got %q", m.Key)
			}
			if tt.key != "" && string(m.Key) != tt.key {
				t.Fatalf("key = %q, want %q", m.Key, tt.key)
			}
			if got := fromHeaders(m.Headers); got["trace"] != tt.headers["trace"] {
				t.Fatalf("headers = %v, want %v", got, tt.headers)
			}
		})
	}
}

func TestProducerPublishEvent(t *testing.T) {
	w := &fakeWriter{}
	p := newProducer(w, defaultProducerOptions())

	e, err := NewEvent("order.created", 1, map[string]string{"id": "order-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.PublishEvent(context.Background(), "orders", "order-1", e); err != nil {
		t.Fatalf("PublishEvent() err = %v", err)
	}

	msgs := w.written()
	if len(msgs) != 1 {
		t.Fatalf("written %d messages, want 1", len(msgs))
	}
	if got := MessageID(msgs[0]); got != e.ID {
		t.Fatalf("MessageID() = %q, want event id %q", got, e.ID)
	}
	var got Event
	if err := json.Unmarshal(msgs[0].Value, &got); err != nil || got.ID != e.ID || got.Type != e.Type {
		t.Fatalf("body = %s, err = %v", msgs[0].Value, err)
	}
}
