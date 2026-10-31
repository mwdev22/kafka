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

// hash balancer keeps one key on one partition, so per-entity order holds
func NewProducer(brokers []string, opts ...ProducerOption) (*Producer, error) {
	if len(brokers) == 0 {
		return nil, WrapErr(OpConfig, ErrMissingBrokers)
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

func (p *Producer) Publish(ctx context.Context, topic, key string, body []byte, headers map[string]string) error {
	return WrapErr(OpPublish, p.publish(ctx, topic, key, body, headers))
}

func (p *Producer) publish(ctx context.Context, topic, key string, body []byte, headers map[string]string) error {
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
		return fmt.Errorf("to %s: %w", topic, err)
	}
	return nil
}

func (p *Producer) PublishEvent(ctx context.Context, topic, key string, e Event) error {
	body, err := json.Marshal(e)
	if err != nil {
		return WrapErr(OpEvent, fmt.Errorf("marshal: %w", err))
	}
	return p.Publish(ctx, topic, key, body, map[string]string{HeaderID: e.ID})
}

func (p *Producer) Close() error {
	return WrapErr(OpClose, p.w.Close())
}
