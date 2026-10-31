package kafka

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"time"
)

type Event struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	Version    int             `json:"version"`
	OccurredAt time.Time       `json:"occurred_at"`
	Payload    json.RawMessage `json:"payload"`
}

func NewEvent(typ string, version int, payload any) (Event, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return Event{}, fmt.Errorf("kafka: marshal event payload: %w", err)
	}
	return Event{
		ID:         newUUID(),
		Type:       typ,
		Version:    version,
		OccurredAt: time.Now().UTC(),
		Payload:    body,
	}, nil
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
