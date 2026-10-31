# kafka

Shared Kafka producer/consumer lib on top of [segmentio/kafka-go](https://github.com/segmentio/kafka-go).
You write a `Handler`; the lib gives you at-least-once delivery, bounded retries, a DLQ and graceful shutdown.

```
go get github.com/mwdev22/kafka
```

## usage

```go
// producer
p, err := kafka.NewProducer([]string{"localhost:9092"},
	kafka.WithProducerLogger(l),
	kafka.WithRequiredAcks(kafka.RequireAll), // default
	kafka.WithWriteTimeout(5*time.Second),
)
defer p.Close()

e, _ := kafka.NewEvent("order.created", 1, OrderCreated{ID: orderID})
err = p.PublishEvent(ctx, "orders", orderID, e) // key = entity id → same partition → ordered per entity

// consumer
h := func(ctx context.Context, m kafka.Message) error {
	var e kafka.Event
	if err := json.Unmarshal(m.Body, &e); err != nil {
		return kafka.Permanent(err) // no retry, straight to dlq
	}
	return billing.Handle(ctx, m.ID, e) // transient errors are retried
}

c, err := kafka.NewConsumer([]string{"localhost:9092"}, h,
	kafka.WithTopic("orders"),
	kafka.WithGroupID("billing"),
	kafka.WithMaxRetries(3),                                  // default
	kafka.WithBackoff(200*time.Millisecond, 10*time.Second), // default
	kafka.WithDLQTopic("orders.dlq"),                         // default <topic>.dlq
	kafka.WithShutdownTimeout(10*time.Second),                // default
	kafka.WithConsumerLogger(l),
)

ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
defer stop()
if err := c.Run(ctx); err != nil { // nil on shutdown
	log.Fatal(err) // e.g. ErrDLQ: exit non-zero, let the orchestrator restart
}
```

## semantics

**At-least-once.** Offsets are committed explicitly (`CommitInterval: 0`), and only after the handler returns nil or the message has been written to the DLQ. A crash between handling and committing means the message is delivered again.

| situation | what `Run` does |
|---|---|
| handler ok | commit, continue |
| transient error, retries left | retry in place with exp backoff + jitter |
| `Permanent(err)` or retries exhausted | publish to dlq, then commit |
| dlq publish fails | no commit, return error wrapping `ErrDLQ` |
| commit fails | return the error (message is redelivered after restart) |
| ctx cancelled | in-flight handler finishes (ctx detached, bounded by `WithShutdownTimeout`), commit, close, return nil |
| ctx cancelled during a retry backoff | stop, no commit, no dlq, return nil; the next partition owner gets the message again |

DLQ messages keep the original key, body and headers, plus `id`, `x-error`, `x-retry-count` and `x-original-topic`.

### idempotency is your job

Duplicates happen (redelivery after a crash, a rebalance, or a deploy during retries). `Message.ID` is stable across redeliveries: the `id` header (set by `PublishEvent` from `Event.ID`), else `topic/partition/offset`. Record it in the same transaction as the side effect:

```sql
CREATE TABLE processed_messages (message_id text PRIMARY KEY, processed_at timestamptz NOT NULL DEFAULT now());
```

```go
tx, _ := db.Begin(ctx)
defer tx.Rollback(ctx)
tag, err := tx.Exec(ctx, `INSERT INTO processed_messages (message_id) VALUES ($1) ON CONFLICT DO NOTHING`, m.ID)
if err != nil {
	return err
}
if tag.RowsAffected() == 0 {
	return nil // already processed
}
// ... side effect in the same tx ...
return tx.Commit(ctx)
```

### retries block the partition

Retries happen in place, so one failing message holds up its partition for at most about `maxRetries * maxBackoff` (~30s with defaults). That keeps ordering and avoids extra topics, and it's fine for v1. If a slow dependency stalls you too often, lower the retries/backoff or move to retry topics (below).

Messages are handled one at a time per consumer, to keep partition order. To scale, run more instances in the same group, up to the partition count.

## errors

Every error returned by this package is a `*kafka.Error{Op, Err}`, with a message like `kafka: commit: orders/0/7: broker down`. Use `errors.As` to tell the broker apart and `errors.Is` to find the kind:

```go
var ke *kafka.Error
switch {
case errors.Is(err, kafka.ErrDLQ): // alert, exit non-zero
case errors.As(err, &ke):         // 503 / codes.Unavailable, label op=ke.Op
}
```

Ops: `OpConfig`, `OpPublish`, `OpFetch`, `OpCommit`, `OpDLQ`, `OpClose`, `OpEvent`. Use `kafka.WrapErr(op, err)` to tag your own broker-side failures the same way.

## tests

```
go test ./...                                        # unit, fakes for reader/writer
docker compose up -d && go test -tags integration ./... && docker compose down
```

## later

- retry topics (`<topic>.retry.5s`, `.1m`, ...) so retries don't block the partition
- transactional outbox on the producer side, for events that must match db state
- metrics: handled/retried/dead-lettered counters, dlq depth, consumer lag
