package kafka

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	tests := []struct {
		name     string
		attempt  int
		base     time.Duration
		max      time.Duration
		min, top time.Duration
	}{
		{name: "#1 - OK - first attempt", attempt: 0, base: 100 * time.Millisecond, max: time.Second, min: 50 * time.Millisecond, top: 100 * time.Millisecond},
		{name: "#2 - OK - grows exponentially", attempt: 2, base: 100 * time.Millisecond, max: time.Second, min: 200 * time.Millisecond, top: 400 * time.Millisecond},
		{name: "#3 - OK - capped at max", attempt: 30, base: 100 * time.Millisecond, max: time.Second, min: 500 * time.Millisecond, top: time.Second},
		{name: "#4 - OK - zero base", attempt: 3, base: 0, max: time.Second, min: 0, top: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for range 100 {
				d := backoff(tt.attempt, tt.base, tt.max)
				if d < tt.min || d > tt.top {
					t.Fatalf("backoff() = %v, want in [%v, %v]", d, tt.min, tt.top)
				}
			}
		})
	}
}

func TestSleep(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name    string
		ctx     context.Context
		d       time.Duration
		wantErr error
	}{
		{name: "#1 - OK", ctx: context.Background(), d: time.Millisecond},
		{name: "#2 - FAIL - ctx cancelled", ctx: cancelled, d: time.Hour, wantErr: context.Canceled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := sleep(tt.ctx, tt.d); !errors.Is(err, tt.wantErr) {
				t.Fatalf("sleep() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}
