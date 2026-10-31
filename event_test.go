package kafka

import (
	"encoding/json"
	"regexp"
	"testing"
)

var uuidRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestNewEvent(t *testing.T) {
	tests := []struct {
		name    string
		payload any
		wantErr bool
	}{
		{name: "#1 - OK", payload: map[string]int{"amount": 10}},
		{name: "#2 - FAIL - unmarshalable payload", payload: make(chan int), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, err := NewEvent("order.created", 1, tt.payload)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !uuidRe.MatchString(e.ID) {
				t.Fatalf("id %q is not a uuid v4", e.ID)
			}
			if e.Type != "order.created" || e.Version != 1 || e.OccurredAt.IsZero() {
				t.Fatalf("unexpected event: %+v", e)
			}
			var got map[string]int
			if err := json.Unmarshal(e.Payload, &got); err != nil || got["amount"] != 10 {
				t.Fatalf("payload = %s, err = %v", e.Payload, err)
			}
		})
	}
}
