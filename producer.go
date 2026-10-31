package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	kgo "github.com/segmentio/kafka-go"
)

type writer interface {
	WriteMessages(ctx context.Context, msgs ...kgo.Message) error
	Close() error
}

type Producer struct {
	w    writer
	opts ProducerOptions
}

// NewProducer writes synchronously with acks from all in-sync replicas by default.
// messages are hash-balanced by key so one entity always lands on one partition
func NewProducer(brokers []string, opts ...ProducerOption) (*Producer, error) {
	if len(brokers) == 0 {
		return nil, ErrMissingBrokers
	}
	o := defaultProducerOptions()
	for _, opt := range opts {
		opt(&o)
	}
	w := &kgo.Writer{
		Addr:         kgo.TCP(brokers...),
		Balancer:     &kgo.Hash{},
		RequiredAcks: o.RequiredAcks,
		WriteTimeout: o.WriteTimeout,
		// default 1s would delay every sync single-message write
		BatchTimeout: 10 * time.Millisecond,
		ErrorLogger:  kgo.LoggerFunc(o.Logger.Printf),
	}
	return newProducer(w, o), nil
}

func newProducer(w writer, o ProducerOptions) *Producer {
	return &Producer{w: w, opts: o}
}

// Publish blocks until the broker acks the message or WriteTimeout passes
func (p *Producer) Publish(ctx context.Context, topic, key string, body []byte, headers map[string]string) error {
	if topic == "" {
		return ErrMissingTopic
	}
	m := kgo.Message{Topic: topic, Value: body, Headers: toHeaders(headers)}
	if key != "" {
		m.Key = []byte(key)
	}
	if p.opts.WriteTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.opts.WriteTimeout)
		defer cancel()
	}
	if err := p.w.WriteMessages(ctx, m); err != nil {
		return fmt.Errorf("kafka: publish to %s: %w", topic, err)
	}
	return nil
}

// PublishEvent sends e as json with the id header set to e.ID, so consumers get it as Message.ID
func (p *Producer) PublishEvent(ctx context.Context, topic, key string, e Event) error {
	body, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("kafka: marshal event: %w", err)
	}
	return p.Publish(ctx, topic, key, body, map[string]string{HeaderID: e.ID})
}

func (p *Producer) Close() error {
	return p.w.Close()
}
