package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	kgo "github.com/segmentio/kafka-go"
)

type fakeFetcher struct {
	mu      sync.Mutex
	queue   []kgo.Message
	commits []kgo.Message
	// called when the queue is drained, so Run sees a cancelled ctx and returns
	onEmpty   func()
	commitErr error
	closed    bool
}

func (f *fakeFetcher) FetchMessage(ctx context.Context) (kgo.Message, error) {
	f.mu.Lock()
	if len(f.queue) > 0 {
		m := f.queue[0]
		f.queue = f.queue[1:]
		f.mu.Unlock()
		return m, nil
	}
	f.mu.Unlock()
	f.onEmpty()
	<-ctx.Done()
	return kgo.Message{}, ctx.Err()
}

func (f *fakeFetcher) CommitMessages(_ context.Context, msgs ...kgo.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.commitErr != nil {
		return f.commitErr
	}
	f.commits = append(f.commits, msgs...)
	return nil
}

func (f *fakeFetcher) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

type nopLogger struct{}

func (nopLogger) Printf(string, ...interface{}) {}
func (nopLogger) Println(...interface{})        {}

func msg(offset int64) kgo.Message {
	return kgo.Message{
		Topic:  "orders",
		Offset: offset,
		Key:    []byte("k"),
		Value:  []byte("v"),
	}
}

func TestConsumerRun(t *testing.T) {
	errTransient := errors.New("db down")
	errBroker := errors.New("broker down")

	// handler factories get the run ctx cancel func to simulate SIGTERM
	type handlerFn func(cancel context.CancelFunc, calls *int) Handler

	failN := func(n int, err error) handlerFn {
		return func(_ context.CancelFunc, calls *int) Handler {
			return func(context.Context, Message) error {
				*calls++
				if *calls <= n {
					return err
				}
				return nil
			}
		}
	}

	tests := []struct {
		name        string
		msgs        []kgo.Message
		handler     handlerFn
		opts        []ConsumerOption
		dlqErr      error
		commitErr   error
		wantErr     error
		wantCalls   int
		wantCommits int
		wantDLQ     int
	}{
		{
			name:        "#1 - OK",
			msgs:        []kgo.Message{msg(1), msg(2)},
			handler:     failN(0, nil),
			wantCalls:   2,
			wantCommits: 2,
		},
		{
			name:        "#2 - OK - transient then success",
			msgs:        []kgo.Message{msg(1)},
			handler:     failN(1, errTransient),
			wantCalls:   2,
			wantCommits: 1,
		},
		{
			name:        "#3 - FAIL - transient exhausted → dlq",
			msgs:        []kgo.Message{msg(1)},
			handler:     failN(100, errTransient),
			opts:        []ConsumerOption{WithMaxRetries(2)},
			wantCalls:   3,
			wantCommits: 1,
			wantDLQ:     1,
		},
		{
			name:        "#4 - FAIL - permanent → dlq",
			msgs:        []kgo.Message{msg(1)},
			handler:     failN(100, Permanent(errTransient)),
			wantCalls:   1,
			wantCommits: 1,
			wantDLQ:     1,
		},
		{
			name: "#5 - OK - ctx cancel mid-handler finishes and commits",
			msgs: []kgo.Message{msg(1)},
			handler: func(cancel context.CancelFunc, calls *int) Handler {
				return func(ctx context.Context, _ Message) error {
					*calls++
					cancel()
					time.Sleep(20 * time.Millisecond)
					return ctx.Err()
				}
			},
			wantCalls:   1,
			wantCommits: 1,
		},
		{
			name: "#6 - OK - ctx cancel during backoff → no commit, no dlq",
			msgs: []kgo.Message{msg(1), msg(2)},
			handler: func(cancel context.CancelFunc, calls *int) Handler {
				return func(context.Context, Message) error {
					*calls++
					go func() {
						time.Sleep(10 * time.Millisecond)
						cancel()
					}()
					return errTransient
				}
			},
			opts:      []ConsumerOption{WithBackoff(time.Hour, time.Hour)},
			wantCalls: 1,
		},
		{
			name:      "#7 - FAIL - dlq publish fails → no commit",
			msgs:      []kgo.Message{msg(1), msg(2)},
			handler:   failN(100, Permanent(errTransient)),
			dlqErr:    errBroker,
			wantErr:   ErrDLQ,
			wantCalls: 1,
		},
		{
			name:      "#8 - FAIL - commit fails",
			msgs:      []kgo.Message{msg(1), msg(2)},
			handler:   failN(0, nil),
			commitErr: errBroker,
			wantErr:   errBroker,
			wantCalls: 1,
		},
		{
			// dedupe on Message.ID is the caller's job
			name:        "#9 - OK - same message twice → handler called twice",
			msgs:        []kgo.Message{msg(1), msg(1)},
			handler:     failN(0, nil),
			wantCalls:   2,
			wantCommits: 2,
		},
		{
			name:        "#10 - FAIL - transient during shutdown is not dead-lettered",
			msgs:        []kgo.Message{msg(1)},
			opts:        []ConsumerOption{WithMaxRetries(0)},
			wantCalls:   1,
			wantCommits: 0,
			handler: func(cancel context.CancelFunc, calls *int) Handler {
				return func(context.Context, Message) error {
					*calls++
					cancel()
					return errTransient
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			r := &fakeFetcher{
				queue:     tt.msgs,
				onEmpty:   cancel,
				commitErr: tt.commitErr,
			}
			w := &fakeWriter{err: tt.dlqErr}

			o := defaultConsumerOptions()
			o.Topic, o.GroupID, o.Logger = "orders", "billing", nopLogger{}
			o.BackoffBase, o.BackoffMax = time.Millisecond, time.Millisecond
			for _, opt := range tt.opts {
				opt(&o)
			}
			if err := o.validate(); err != nil {
				t.Fatal(err)
			}

			var calls int
			c := newConsumer(r, newProducer(w, defaultProducerOptions()), tt.handler(cancel, &calls), o)

			done := make(chan error, 1)
			go func() { done <- c.Run(ctx) }()

			var err error
			select {
			case err = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("Run did not return")
			}

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Run() err = %v, want %v", err, tt.wantErr)
			}
			if calls != tt.wantCalls {
				t.Fatalf("handler calls = %d, want %d", calls, tt.wantCalls)
			}
			if len(r.commits) != tt.wantCommits {
				t.Fatalf("commits = %d, want %d", len(r.commits), tt.wantCommits)
			}
			if !r.closed {
				t.Fatal("reader not closed")
			}

			dlq := w.written()
			if len(dlq) != tt.wantDLQ {
				t.Fatalf("dlq messages = %d, want %d", len(dlq), tt.wantDLQ)
			}
			for _, m := range dlq {
				h := fromHeaders(m.Headers)
				if m.Topic != "orders.dlq" || h[HeaderOriginalTopic] != "orders" || h[HeaderError] == "" || h[HeaderID] != "orders/0/1" {
					t.Fatalf("unexpected dlq message: topic=%s headers=%v", m.Topic, h)
				}
			}
		})
	}
}

func TestShutdownCtx(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	ctx, stop := shutdownCtx(parent, 20*time.Millisecond)
	defer stop()

	cancel()
	select {
	case <-ctx.Done():
		t.Fatal("handler ctx cancelled immediately with parent")
	case <-time.After(5 * time.Millisecond):
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("handler ctx not cancelled after shutdown timeout")
	}
}

func TestNewConsumer(t *testing.T) {
	h := func(context.Context, Message) error { return nil }
	brokers := []string{"localhost:9092"}

	tests := []struct {
		name    string
		brokers []string
		h       Handler
		opts    []ConsumerOption
		wantErr error
		wantDLQ string
	}{
		{name: "#1 - OK - default dlq topic", brokers: brokers, h: h, opts: []ConsumerOption{WithTopic("orders"), WithGroupID("billing")}, wantDLQ: "orders.dlq"},
		{name: "#2 - OK - custom dlq topic", brokers: brokers, h: h, opts: []ConsumerOption{WithTopic("orders"), WithGroupID("billing"), WithDLQTopic("dead")}, wantDLQ: "dead"},
		{name: "#3 - FAIL - missing brokers", h: h, opts: []ConsumerOption{WithTopic("orders"), WithGroupID("billing")}, wantErr: ErrMissingBrokers},
		{name: "#4 - FAIL - missing handler", brokers: brokers, opts: []ConsumerOption{WithTopic("orders"), WithGroupID("billing")}, wantErr: ErrMissingHandler},
		{name: "#5 - FAIL - missing topic", brokers: brokers, h: h, opts: []ConsumerOption{WithGroupID("billing")}, wantErr: ErrMissingTopic},
		{name: "#6 - FAIL - missing group id", brokers: brokers, h: h, opts: []ConsumerOption{WithTopic("orders")}, wantErr: ErrMissingGroupID},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewConsumer(tt.brokers, tt.h, append(tt.opts, WithConsumerLogger(nopLogger{}))...)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("NewConsumer() err = %v, want %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			defer c.Close()
			if c.opts.DLQTopic != tt.wantDLQ {
				t.Fatalf("dlq topic = %q, want %q", c.opts.DLQTopic, tt.wantDLQ)
			}
		})
	}
}
