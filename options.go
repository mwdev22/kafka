package kafka

import (
	"errors"
	"time"

	"github.com/mwdev22/logging"
	kgo "github.com/segmentio/kafka-go"
)

var (
	ErrMissingBrokers = errors.New("kafka: missing brokers")
	ErrMissingTopic   = errors.New("kafka: missing topic")
	ErrMissingGroupID = errors.New("kafka: missing group id")
	ErrMissingHandler = errors.New("kafka: missing handler")
	ErrDLQ            = errors.New("kafka: dlq publish failed")
)

type RequiredAcks = kgo.RequiredAcks

const (
	RequireNone = kgo.RequireNone
	RequireOne  = kgo.RequireOne
	RequireAll  = kgo.RequireAll
)

type ProducerOption func(*ProducerOptions)

type ProducerOptions struct {
	Logger       logging.Logger
	RequiredAcks RequiredAcks
	WriteTimeout time.Duration
}

func defaultProducerOptions() ProducerOptions {
	return ProducerOptions{
		Logger:       logging.DefaultLogger(),
		RequiredAcks: RequireAll,
		WriteTimeout: 10 * time.Second,
	}
}

func WithProducerLogger(l logging.Logger) ProducerOption {
	return func(o *ProducerOptions) { o.Logger = l }
}

func WithRequiredAcks(acks RequiredAcks) ProducerOption {
	return func(o *ProducerOptions) { o.RequiredAcks = acks }
}

func WithWriteTimeout(d time.Duration) ProducerOption {
	return func(o *ProducerOptions) { o.WriteTimeout = d }
}

type ConsumerOption func(*ConsumerOptions)

type ConsumerOptions struct {
	Topic           string
	GroupID         string
	DLQTopic        string // defaults to <topic>.dlq
	MaxRetries      int
	BackoffBase     time.Duration
	BackoffMax      time.Duration
	ShutdownTimeout time.Duration
	Logger          logging.Logger
}

func defaultConsumerOptions() ConsumerOptions {
	return ConsumerOptions{
		MaxRetries:      3,
		BackoffBase:     200 * time.Millisecond,
		BackoffMax:      10 * time.Second,
		ShutdownTimeout: 10 * time.Second,
		Logger:          logging.DefaultLogger(),
	}
}

func WithTopic(topic string) ConsumerOption {
	return func(o *ConsumerOptions) { o.Topic = topic }
}

func WithGroupID(id string) ConsumerOption {
	return func(o *ConsumerOptions) { o.GroupID = id }
}

func WithDLQTopic(topic string) ConsumerOption {
	return func(o *ConsumerOptions) { o.DLQTopic = topic }
}

// WithMaxRetries sets retries after the first attempt, 0 disables retries
func WithMaxRetries(n int) ConsumerOption {
	return func(o *ConsumerOptions) { o.MaxRetries = max(n, 0) }
}

func WithBackoff(base, max time.Duration) ConsumerOption {
	return func(o *ConsumerOptions) { o.BackoffBase, o.BackoffMax = base, max }
}

// WithShutdownTimeout bounds the in-flight handler after ctx is cancelled
func WithShutdownTimeout(d time.Duration) ConsumerOption {
	return func(o *ConsumerOptions) { o.ShutdownTimeout = d }
}

func WithConsumerLogger(l logging.Logger) ConsumerOption {
	return func(o *ConsumerOptions) { o.Logger = l }
}

func (o *ConsumerOptions) validate() error {
	if o.Topic == "" {
		return ErrMissingTopic
	}
	if o.GroupID == "" {
		return ErrMissingGroupID
	}
	if o.DLQTopic == "" {
		o.DLQTopic = o.Topic + ".dlq"
	}
	return nil
}
