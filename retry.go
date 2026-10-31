package kafka

import (
	"context"
	"math/rand/v2"
	"time"
)

// backoff returns base*2^attempt capped at max, with jitter in [d/2, d]
func backoff(attempt int, base, max time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	d := base
	for i := 0; i < attempt && d < max; i++ {
		d *= 2
	}
	if max > 0 && d > max {
		d = max
	}
	half := d / 2
	return half + rand.N(half+1)
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
