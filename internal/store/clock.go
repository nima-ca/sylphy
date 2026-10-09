package store

import (
	"sync/atomic"
	"time"
)

// Clock supplies the current wall-clock time as Unix milliseconds. The store
// reads time only through a Clock so that expiry can be tested without
// sleeping. Wall time (not a monotonic reading) is used on purpose: deadlines
// are absolute Unix timestamps, because EXPIREAT/PEXPIREAT accept them and
// persistence will later write them to disk.
type Clock interface {
	// NowMs returns the current time in milliseconds since the Unix epoch.
	NowMs() int64
}

// SystemClock is the Clock backed by the operating system's wall clock.
type SystemClock struct{}

// NowMs implements Clock.
func (SystemClock) NowMs() int64 { return time.Now().UnixMilli() }

// FakeClock is a manually advanced Clock for tests. It is safe for concurrent
// use, so it can be shared with a running sweeper and client goroutines.
type FakeClock struct{ ms atomic.Int64 }

// NewFakeClock returns a FakeClock that reads startMs until advanced.
func NewFakeClock(startMs int64) *FakeClock {
	c := &FakeClock{}
	c.ms.Store(startMs)
	return c
}

// NowMs implements Clock.
func (c *FakeClock) NowMs() int64 { return c.ms.Load() }

// Advance moves the clock forward by d, truncated to whole milliseconds.
func (c *FakeClock) Advance(d time.Duration) { c.ms.Add(d.Milliseconds()) }

// Set moves the clock to an absolute Unix-millisecond reading.
func (c *FakeClock) Set(ms int64) { c.ms.Store(ms) }

// NowMs returns the store's current time in Unix milliseconds, from the same
// Clock its expiry checks use. Command handlers use it to turn deadlines into
// remaining TTLs.
func (s *Store) NowMs() int64 { return s.clock.NowMs() }
