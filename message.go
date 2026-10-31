package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	kgo "github.com/segmentio/kafka-go"
)

// header keys set by this lib
const (
	HeaderID            = "id"
	HeaderError         = "x-error"
	HeaderRetryCount    = "x-retry-count"
	HeaderOriginalTopic = "x-original-topic"
)

type Message struct {
	// ID is stable across redeliveries: the id header if set, else topic/partition/offset
	ID        string
	Key       string
	Body      []byte
	Headers   map[string]string
	Topic     string
	Partition int
	Offset    int64
	Time      time.Time
}

// Handler must be idempotent: delivery is at-least-once
type Handler func(ctx context.Context, m Message) error

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent marks err as not retryable, the message goes straight to the dlq
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

func MessageID(m kgo.Message) string {
	for _, h := range m.Headers {
		if h.Key == HeaderID && len(h.Value) > 0 {
			return string(h.Value)
		}
	}
	return fmt.Sprintf("%s/%d/%d", m.Topic, m.Partition, m.Offset)
}

func toMessage(m kgo.Message) Message {
	return Message{
		ID:        MessageID(m),
		Key:       string(m.Key),
		Body:      m.Value,
		Headers:   fromHeaders(m.Headers),
		Topic:     m.Topic,
		Partition: m.Partition,
		Offset:    m.Offset,
		Time:      m.Time,
	}
}

func fromHeaders(hs []kgo.Header) map[string]string {
	if len(hs) == 0 {
		return nil
	}
	out := make(map[string]string, len(hs))
	for _, h := range hs {
		out[h.Key] = string(h.Value)
	}
	return out
}

func toHeaders(m map[string]string) []kgo.Header {
	if len(m) == 0 {
		return nil
	}
	out := make([]kgo.Header, 0, len(m))
	for k, v := range m {
		out = append(out, kgo.Header{Key: k, Value: []byte(v)})
	}
	return out
}
