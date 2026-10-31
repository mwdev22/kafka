package kafka

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"sync"
	"time"

	kgo "github.com/segmentio/kafka-go"
)

// a dead reader must not hang shutdown on commit
const commitTimeout = 10 * time.Second

// shutdown interrupted retries, the message stays uncommitted
var errStopped = errors.New("stopped during retry")

type fetcher interface {
	FetchMessage(ctx context.Context) (kgo.Message, error)
	CommitMessages(ctx context.Context, msgs ...kgo.Message) error
	Close() error
}

// sequential on purpose to keep partition order, scale with more instances
type Consumer struct {
	r         fetcher
	dlq       *Producer
	h         Handler
	opts      ConsumerOptions
	closeOnce sync.Once
	closeErr  error
}

func NewConsumer(brokers []string, h Handler, opts ...ConsumerOption) (*Consumer, error) {
	if len(brokers) == 0 {
		return nil, WrapErr(OpConfig, ErrMissingBrokers)
	}
	if h == nil {
		return nil, WrapErr(OpConfig, ErrMissingHandler)
	}
	o := defaultConsumerOptions()
	for _, opt := range opts {
		opt(&o)
	}
	if err := o.validate(); err != nil {
		return nil, WrapErr(OpConfig, err)
	}

	cfg := kgo.ReaderConfig{
		Brokers:        brokers,
		GroupID:        o.GroupID,
		Topic:          o.Topic,
		CommitInterval: 0,
		ErrorLogger:    kgo.LoggerFunc(o.Logger.Printf),
	}
	// kafka-go NewReader panics on invalid config
	if err := cfg.Validate(); err != nil {
		return nil, WrapErr(OpConfig, fmt.Errorf("reader: %w", err))
	}
	dlq, err := NewProducer(brokers, WithProducerLogger(o.Logger))
	if err != nil {
		return nil, err
	}
	return newConsumer(kgo.NewReader(cfg), dlq, h, o), nil
}

func newConsumer(r fetcher, dlq *Producer, h Handler, o ConsumerOptions) *Consumer {
	return &Consumer{
		r:    r,
		dlq:  dlq,
		h:    h,
		opts: o,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	defer c.closeAndLog()

	for {
		km, err := c.r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return WrapErr(OpFetch, err)
		}

		m := toMessage(km)
		retries, herr := c.handle(ctx, m)
		if errors.Is(herr, errStopped) {
			c.opts.Logger.Printf("kafka: shutdown during retry of %s, leaving uncommitted", m.ID)
			return nil
		}
		if herr != nil {
			if err := c.toDLQ(ctx, km, m, retries, herr); err != nil {
				return WrapErr(OpDLQ, fmt.Errorf("%w for %s: %v", ErrDLQ, m.ID, err))
			}
		}

		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
		err = c.r.CommitMessages(cctx, km)
		cancel()
		if err != nil {
			return WrapErr(OpCommit, fmt.Errorf("%s: %w", m.ID, err))
		}
	}
}

func (c *Consumer) handle(ctx context.Context, m Message) (int, error) {
	hctx, cancel := shutdownCtx(ctx, c.opts.ShutdownTimeout)
	defer cancel()

	for attempt := 0; ; attempt++ {
		err := c.h(hctx, m)
		if err == nil || IsPermanent(err) {
			return attempt, err
		}
		// a deploy must not dead-letter a retryable message
		if ctx.Err() != nil {
			return attempt, errStopped
		}
		if attempt >= c.opts.MaxRetries {
			return attempt, err
		}
		d := backoff(attempt, c.opts.BackoffBase, c.opts.BackoffMax)
		c.opts.Logger.Printf("kafka: handle %s failed (attempt %d), retrying in %v: %v", m.ID, attempt+1, d, err)
		if sleep(ctx, d) != nil {
			return attempt, errStopped
		}
	}
}

func (c *Consumer) toDLQ(ctx context.Context, km kgo.Message, m Message, retries int, herr error) error {
	h := make(map[string]string, len(m.Headers)+4)
	maps.Copy(h, m.Headers)
	h[HeaderID] = m.ID
	h[HeaderError] = herr.Error()
	h[HeaderRetryCount] = strconv.Itoa(retries)
	h[HeaderOriginalTopic] = m.Topic

	c.opts.Logger.Printf("kafka: sending %s to %s after %d retries: %v", m.ID, c.opts.DLQTopic, retries, herr)
	return c.dlq.publish(context.WithoutCancel(ctx), c.opts.DLQTopic, string(km.Key), km.Value, h)
}

func (c *Consumer) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = errors.Join(c.r.Close(), c.dlq.Close())
	})
	return WrapErr(OpClose, c.closeErr)
}

func (c *Consumer) closeAndLog() {
	if err := c.Close(); err != nil {
		c.opts.Logger.Printf("%v", err)
	}
}

// ignores parent cancellation until d after it, so the in-flight handler can finish
func shutdownCtx(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	stop := context.AfterFunc(parent, func() {
		time.AfterFunc(d, cancel)
	})
	return ctx, func() {
		stop()
		cancel()
	}
}
