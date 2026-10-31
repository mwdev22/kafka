package kafka

import (
	"errors"
	"fmt"
	"testing"

	kgo "github.com/segmentio/kafka-go"
)

func TestPermanent(t *testing.T) {
	base := errors.New("bad payload")

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "#1 - OK - permanent", err: Permanent(base), want: true},
		{name: "#2 - OK - wrapped permanent", err: fmt.Errorf("handle: %w", Permanent(base)), want: true},
		{name: "#3 - FAIL - plain error", err: base, want: false},
		{name: "#4 - FAIL - nil", err: Permanent(nil), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPermanent(tt.err); got != tt.want {
				t.Fatalf("IsPermanent() = %v, want %v", got, tt.want)
			}
			if tt.want && !errors.Is(tt.err, base) {
				t.Fatalf("permanent error should unwrap to base")
			}
		})
	}
}

func TestMessageID(t *testing.T) {
	tests := []struct {
		name string
		msg  kgo.Message
		want string
	}{
		{
			name: "#1 - OK - id header",
			msg:  kgo.Message{Topic: "orders", Headers: []kgo.Header{{Key: HeaderID, Value: []byte("evt-1")}}},
			want: "evt-1",
		},
		{
			name: "#2 - OK - fallback to topic/partition/offset",
			msg:  kgo.Message{Topic: "orders", Partition: 2, Offset: 42},
			want: "orders/2/42",
		},
		{
			name: "#3 - OK - empty id header falls back",
			msg:  kgo.Message{Topic: "orders", Offset: 7, Headers: []kgo.Header{{Key: HeaderID}}},
			want: "orders/0/7",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MessageID(tt.msg); got != tt.want {
				t.Fatalf("MessageID() = %q, want %q", got, tt.want)
			}
		})
	}
}
