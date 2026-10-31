//go:build integration

package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	kgo "github.com/segmentio/kafka-go"
)

// run with: docker compose up -d && go test -tags integration ./...

const topicWaitTimeout = 10 * time.Second

func brokers() []string {
	if b := os.Getenv("KAFKA_BROKERS"); b != "" {
		return []string{b}
	}
	return []string{"localhost:9092"}
}

func createTopics(t *testing.T, topics ...string) {
	t.Helper()
	conn, err := kgo.Dial("tcp", brokers()[0])
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	ctrl, err := conn.Controller()
	if err != nil {
		t.Fatalf("controller: %v", err)
	}
	cc, err := kgo.Dial("tcp", net.JoinHostPort(ctrl.Host, strconv.Itoa(ctrl.Port)))
	if err != nil {
		t.Fatalf("dial controller: %v", err)
	}
	defer cc.Close()

	cfgs := make([]kgo.TopicConfig, 0, len(topics))
	for _, tp := range topics {
		cfgs = append(cfgs, kgo.TopicConfig{
			Topic:             tp,
			NumPartitions:     1,
			ReplicationFactor: 1,
		})
	}
	if err := cc.CreateTopics(cfgs...); err != nil {
		t.Fatalf("create topics: %v", err)
	}

	deadline := time.Now().Add(topicWaitTimeout)
	for _, tp := range topics {
		for {
			if _, err := conn.ReadPartitions(tp); err == nil {
				break
			} else if time.Now().After(deadline) {
				t.Fatalf("topic %s not visible: %v", tp, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func consume(t *testing.T, topic string, wantCalls int, h Handler, opts ...ConsumerOption) Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	got := make(chan Message, 1)
	var calls int
	wrapped := func(ctx context.Context, m Message) error {
		err := h(ctx, m)
		if calls++; calls == wantCalls {
			got <- m
		}
		return err
	}
	opts = append([]ConsumerOption{WithTopic(topic), WithGroupID(fmt.Sprintf("%s-group", topic)), WithBackoff(10*time.Millisecond, 50*time.Millisecond)}, opts...)
	c, err := NewConsumer(brokers(), wrapped, opts...)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	var m Message
	select {
	case m = <-got:
	case err := <-done:
		t.Fatalf("Run returned early: %v", err)
	case <-ctx.Done():
		t.Fatal("timed out waiting for message")
	}
	// give Run time to commit/dlq before shutdown
	time.Sleep(500 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}
	return m
}

func TestIntegrationRoundTrip(t *testing.T) {
	topic := fmt.Sprintf("it-orders-%d", time.Now().UnixNano())
	createTopics(t, topic, topic+".dlq")

	p, err := NewProducer(brokers())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	e, err := NewEvent("order.created", 1, map[string]string{"order_id": "o-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.PublishEvent(context.Background(), topic, "o-1", e); err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}

	m := consume(t, topic, 1, func(context.Context, Message) error { return nil })
	if m.ID != e.ID || m.Key != "o-1" {
		t.Fatalf("got id=%q key=%q, want id=%q key=o-1", m.ID, m.Key, e.ID)
	}
}

func TestIntegrationDLQ(t *testing.T) {
	topic := fmt.Sprintf("it-payments-%d", time.Now().UnixNano())
	createTopics(t, topic, topic+".dlq")

	p, err := NewProducer(brokers())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	if err := p.Publish(context.Background(), topic, "p-1", []byte(`{"bad"`), map[string]string{HeaderID: "msg-1"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	var calls int
	consume(t, topic, 3, func(context.Context, Message) error {
		calls++
		return errors.New("db down")
	}, WithMaxRetries(2))
	if calls != 3 {
		t.Fatalf("handler calls = %d, want 3", calls)
	}

	dead := consume(t, topic+".dlq", 1, func(context.Context, Message) error { return nil })
	if dead.ID != "msg-1" || string(dead.Body) != `{"bad"` {
		t.Fatalf("unexpected dlq message: %+v", dead)
	}
	if dead.Headers[HeaderOriginalTopic] != topic || dead.Headers[HeaderRetryCount] != "2" || dead.Headers[HeaderError] != "db down" {
		t.Fatalf("unexpected dlq headers: %v", dead.Headers)
	}
}
