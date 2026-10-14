package store

import (
	"errors"
	"sync"
	"sync/atomic"
)

// replayClock wraps the store's real clock so that recovery can freeze time.
// Every time read in the store goes through it, so freezing needs no change at
// the call sites.
type replayClock struct {
	inner  Clock
	frozen atomic.Int32
}

// NowMs implements Clock: 0 while frozen, the wrapped clock otherwise.
func (c *replayClock) NowMs() int64 {
	if c.frozen.Load() > 0 {
		return 0
	}
	return c.inner.NowMs()
}

// BeginReplay freezes the store's clock at 0 until the returned function is
// called (calls nest; the function is safe to call more than once). While time
// is frozen nothing expires and no deadline deletes a key, so replaying the
// log reproduces the original sequence of commands exactly. Without it, a key
// whose deadline passed while the server was down would be deleted by an early
// record and recreated, without its TTL, by a later one.
//
// Use it only while no clients are being served.
func (s *Store) BeginReplay() (end func()) {
	s.replay.frozen.Add(1)
	var once sync.Once
	return func() { once.Do(func() { s.replay.frozen.Add(-1) }) }
}

// SetExpireHook installs fn, which is called with the key of every key removed
// because it expired (lazily, by the sweeper, or by PurgeExpired). It runs
// while the key's shard lock is still held, so a record it appends to the log
// lands at the right place relative to every other write to that key. It must
// follow the closure rules on Store: fast, non-blocking, no calls back into the
// Store. Passing nil removes the hook. It is safe to call at any time, but is
// meant to be installed once, before the server accepts clients.
func (s *Store) SetExpireHook(fn func(key string)) {
	if fn == nil {
		s.onExpire.Store(nil)
		return
	}
	s.onExpire.Store(&fn)
}

func (s *Store) noteExpired(key string) {
	if h := s.onExpire.Load(); h != nil {
		(*h)(key)
	}
}

// ShardCount returns the number of shards.
func (s *Store) ShardCount() int { return len(s.shards) }

// Load inserts key with value v and an optional absolute deadline in Unix
// milliseconds (0 means none), replacing whatever was there. It is the bulk
// load used by recovery: it does not consult the clock, so a deadline in the
// past is stored as it is (the key is then invisible and goes away on its
// first access or at PurgeExpired), and it triggers neither the expiry hook
// nor any logging. The store takes ownership of v.
func (s *Store) Load(key string, v Value, deadlineMs int64) error {
	if v == nil {
		return errors.New("store: Load of a nil value")
	}
	if c, ok := v.(Collection); ok && c.Len() == 0 {
		return errors.New("store: Load of an empty collection")
	}
	sh := s.shardFor(key)
	sh.lockW()
	defer sh.mu.Unlock()
	sh.put(key, v)
	if deadlineMs != 0 {
		sh.exp[key] = deadlineMs
	} else {
		delete(sh.exp, key)
	}
	return nil
}

// PurgeExpired removes every key that has expired, one shard at a time, and
// returns how many it removed. Each removal goes through the expiry hook like
// the sweeper's, so with a hook installed a DEL is logged for every purged key.
// Recovery calls it after replay: expired keys loaded from the snapshot stay
// stored (invisible to readers) so that replay sees the same state the original
// run did, and only now are they dropped, in the log as well as in memory.
func (s *Store) PurgeExpired() int {
	n := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.lockW()
		now := s.clock.NowMs()
		for k, at := range sh.exp {
			if now > at {
				sh.remove(k)
				s.expiredActive.Add(1)
				s.noteExpired(k)
				n++
			}
		}
		sh.mu.Unlock()
	}
	return n
}
