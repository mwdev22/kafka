package kafka

import (
	"context"
	"errors"
	"testing"

	kgo "github.com/segmentio/kafka-go"
)

func TestError(t *testing.T) {
	brokerDown := errors.New("broker down")

	runWith := func(f *fakeFetcher, w *fakeWriter, h Handler) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		f.onEmpty = cancel
		o := defaultConsumerOptions()
		o.Topic, o.GroupID, o.DLQTopic, o.Logger = "orders", "billing", "orders.dlq", nopLogger{}
		return newConsumer(f, newProducer(w, defaultProducerOptions()), h, o).Run(ctx)
	}
	permanent := func(context.Context, Message) error { return Permanent(errors.New("bad payload")) }
	ok := func(context.Context, Message) error { return nil }

	tests := []struct {
		name     string
		call     func() error
		wantOp   string
		wantIs   error
		wantText string
	}{
		{
			name:     "#1 - FAIL - producer config",
			call:     func() error { _, err := NewProducer(nil); return err },
			wantOp:   OpConfig,
			wantIs:   ErrMissingBrokers,
			wantText: "kafka: config: missing brokers",
		},
		{
			name: "#2 - FAIL - consumer config",
			call: func() error {
				_, err := NewConsumer([]string{"localhost:9092"}, ok, WithGroupID("billing"))
				return err
			},
			wantOp:   OpConfig,
			wantIs:   ErrMissingTopic,
			wantText: "kafka: config: missing topic",
		},
		{
			name: "#3 - FAIL - publish",
			call: func() error {
				return newProducer(&fakeWriter{err: brokerDown}, defaultProducerOptions()).Publish(context.Background(), "orders", "k", nil, nil)
			},
			wantOp:   OpPublish,
			wantIs:   brokerDown,
			wantText: "kafka: publish: to orders: broker down",
		},
		{
			name: "#4 - FAIL - dlq publish",
			call: func() error {
				return runWith(&fakeFetcher{queue: []kgo.Message{msg(7)}}, &fakeWriter{err: brokerDown}, permanent)
			},
			wantOp:   OpDLQ,
			wantIs:   ErrDLQ,
			wantText: "kafka: dlq: publish failed for orders/0/7: to orders.dlq: broker down",
		},
		{
			name: "#5 - FAIL - commit",
			call: func() error {
				return runWith(&fakeFetcher{queue: []kgo.Message{msg(7)}, commitErr: brokerDown}, &fakeWriter{}, ok)
			},
			wantOp:   OpCommit,
			wantIs:   brokerDown,
			wantText: "kafka: commit: orders/0/7: broker down",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call()

			var ke *Error
			if !errors.As(err, &ke) {
				t.Fatalf("err = %v (%T), want *Error", err, err)
			}
			if ke.Op != tt.wantOp {
				t.Fatalf("Op = %q, want %q", ke.Op, tt.wantOp)
			}
			if !errors.Is(err, tt.wantIs) {
				t.Fatalf("errors.Is(%v, %v) = false", err, tt.wantIs)
			}
			if err.Error() != tt.wantText {
				t.Fatalf("Error() = %q, want %q", err.Error(), tt.wantText)
			}
		})
	}
}

func TestWrapErrNil(t *testing.T) {
	if err := WrapErr(OpPublish, nil); err != nil {
		t.Fatalf("WrapErr(nil) = %v, want nil", err)
	}
}
