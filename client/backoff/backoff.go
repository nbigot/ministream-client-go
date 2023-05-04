package backoff

import (
	"time"
)

type ExpBackoff struct {
	duration time.Duration
	cancel   <-chan struct{}

	init   time.Duration
	max    time.Duration
	factor int64
	last   time.Time
}

// Reset resets the duration of the backoff.
func (b *ExpBackoff) Reset() {
	b.duration = b.init
}

func (b *ExpBackoff) SetDuration(d time.Duration) {
	b.duration = d
}

// Wait block until either the timer is completed or canceled.
func (b *ExpBackoff) Wait() bool {
	backoff := b.duration
	b.duration *= time.Duration(b.factor)
	if b.duration > b.max {
		b.duration = b.max
	}

	select {
	case <-b.cancel:
		return false
	case <-time.After(backoff):
		b.last = time.Now()
		return true
	}
}

func (b *ExpBackoff) WaitAndNotify(done chan<- bool) {
	backoff := b.duration
	b.duration *= time.Duration(b.factor)
	if b.duration > b.max {
		b.duration = b.max
	}

	select {
	case <-b.cancel:
		done <- false
	case <-time.After(backoff):
		b.last = time.Now()
		done <- true
	}
}

// NewExpBackoff returns a new exponential backoff.
func NewExpBackoff(cancel <-chan struct{}, init, max time.Duration) *ExpBackoff {
	return &ExpBackoff{
		duration: init,
		cancel:   cancel,
		init:     init,
		max:      max,
		factor:   2,
	}
}
