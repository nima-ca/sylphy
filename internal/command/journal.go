package command

import (
	"context"
	"strconv"

	"github.com/nima-ca/sylphy/internal/store"
)

// Propagator receives the deterministic form of every write command for
// persistence (and, in Phase 5, replication). *persist.AOF implements it.
type Propagator interface {
	// Admit blocks, outside any lock, while the log is backed up, and returns
	// an error if writes must be refused. It is called once before each write
	// command.
	Admit(ctx context.Context) error
	// Append copies complete records (see EncodeRecord) into the log, in order
	// and contiguously, and returns the sequence number of the last one. It
	// never blocks and does no I/O: it is called while shard locks are held.
	Append(recs ...[]byte) (seq uint64, err error)
	// WaitDurable blocks until the record with sequence seq is on stable
	// storage.
	WaitDurable(ctx context.Context, seq uint64) error
	// Err returns the error that put persistence into a failed state, or nil.
	Err() error
}

// ErrMisconf is the reply for a write command that persistence cannot accept.
var ErrMisconf = &ReplyError{Msg: "MISCONF Sylphy is unable to persist writes to disk, so commands that may modify the data set are disabled. Check the server logs for details."}

// EncodeRecord appends one log record, a RESP array of bulk strings, to dst.
// The bytes are identical to persist.AppendRecord's, so existing RESP tools can
// read the log.
func EncodeRecord(dst []byte, argv [][]byte) []byte {
	dst = append(dst, '*')
	dst = strconv.AppendInt(dst, int64(len(argv)), 10)
	dst = append(dst, '\r', '\n')
	for _, a := range argv {
		dst = append(dst, '$')
		dst = strconv.AppendInt(dst, int64(len(a)), 10)
		dst = append(dst, '\r', '\n')
		dst = append(dst, a...)
		dst = append(dst, '\r', '\n')
	}
	return dst
}

// journal is a Store that logs every write inside the critical section that
// applies it. That is what makes the log order equal the apply order for every
// key: a record is appended while the shard lock(s) of the touched keys are
// still held, so no other write to those keys can slip in between "applied" and
// "logged".
//
// Two kinds of method:
//
//   - Closure methods (Mutate, MutateEntry, Atomic, Update) are one command's
//     critical section. After the handler's closure succeeds, the command's
//     rewrite (Spec.Rewrite, or the request verbatim) is appended, once. This
//     relies on handlers recording what they did in ctx.fx inside the closure.
//   - Primitives (Set, Delete, Flush) are one key or the whole keyspace per
//     call, so they log their own fixed forms: SET key value, DEL key (only
//     when the key existed) and FLUSHALL. MSET therefore logs one SET per pair
//     and DEL one DEL per key that existed; both were atomic per key only.
//
// When the Propagator refuses a record after the command was already applied
// (the log latched between Admit and Append), the change cannot be undone. A
// closure method then returns ErrMisconf, so that client gets an error instead
// of the command's reply; an Update leaves the key untouched, but Mutate and
// MutateEntry changes that were made in place stay in memory (memory and log
// differ until a checkpoint taken after the failure). The primitives cannot
// return an error to their handler, so the reply goes out as usual. Either way
// the failure is kept for JournalErr, and Dispatch refuses further writes
// because the Propagator is in its error state.
type journal struct {
	Store // the unjournaled store; reads and unlogged operations go straight to it

	ctx     *Context
	p       Propagator
	base    context.Context
	spec    *Spec    // the write command being dispatched, nil outside Dispatch
	argv    [][]byte // its request
	scratch []byte   // reused record buffer
	recs    [][]byte // reused record list
	failed  error
}

var _ Store = (*journal)(nil)

// EnableJournal makes every write through c.Store appear in p, inside the
// critical section that applies it. base bounds how long Admit may block (use
// the connection's context). Calling it again, or with a nil p, does nothing.
// Context.LastSeq tracks the newest record this connection has appended.
func (c *Context) EnableJournal(base context.Context, p Propagator) {
	if c.journal != nil || p == nil {
		return
	}
	if base == nil {
		base = context.Background()
	}
	j := &journal{Store: c.Store, ctx: c, p: p, base: base}
	c.journal = j
	c.Store = j
}

// JournalErr returns, and clears, the error recorded when a record could not
// be appended after its command had already been applied. A server that
// acknowledges writes only once they are durable must treat a non-nil result as
// "this connection's replies are not safe to send".
func (c *Context) JournalErr() error {
	if c.journal == nil {
		return nil
	}
	err := c.journal.failed
	c.journal.failed = nil
	return err
}

// begin runs before a write command's handler: it refuses the command if
// persistence is failed, then waits (outside any lock) for room in the log.
func (j *journal) begin(spec *Spec, argv [][]byte) *ReplyError {
	if err := j.p.Err(); err != nil {
		return ErrMisconf
	}
	if err := j.p.Admit(j.base); err != nil {
		return ErrMisconf
	}
	j.spec, j.argv = spec, argv
	return nil
}

func (j *journal) end() {
	j.spec, j.argv = nil, nil
}

// emit appends the given commands as one contiguous group.
func (j *journal) emit(cmds ...[][]byte) error {
	if len(cmds) == 0 {
		return nil
	}
	j.scratch, j.recs = j.scratch[:0], j.recs[:0]
	for _, c := range cmds {
		start := len(j.scratch)
		j.scratch = EncodeRecord(j.scratch, c)
		j.recs = append(j.recs, j.scratch[start:])
	}
	seq, err := j.p.Append(j.recs...)
	if cap(j.scratch) > 1<<20 {
		j.scratch = nil // do not pin a huge value's buffer for the life of the connection
	}
	if err != nil {
		j.failed = err
		return ErrMisconf
	}
	if seq > j.ctx.LastSeq {
		j.ctx.LastSeq = seq
	}
	return nil
}

// emitCommand logs the command being dispatched in its deterministic form. It
// runs inside the handler's closure, after the handler recorded its effects.
func (j *journal) emitCommand() error {
	if j.spec == nil {
		return nil
	}
	return j.emit(j.spec.rewritten(j.argv, &j.ctx.fx)...)
}

func (j *journal) Set(key string, value []byte) {
	nv := store.NewString(value) // copy outside the lock
	_ = j.Store.MutateEntry(key, func(e *store.Entry) error {
		e.Put(nv)
		e.Persist()
		return j.emit([][]byte{[]byte("SET"), []byte(key), value})
	})
}

func (j *journal) Delete(keys ...string) int {
	n := 0
	for _, k := range keys {
		_ = j.Store.MutateEntry(k, func(e *store.Entry) error {
			if !e.Delete() {
				return nil
			}
			n++
			return j.emit([][]byte{[]byte("DEL"), []byte(k)})
		})
	}
	return n
}

func (j *journal) Flush() { j.FlushWith(nil) }

func (j *journal) FlushWith(underLock func()) {
	j.Store.FlushWith(func() {
		_ = j.emit([][]byte{[]byte("FLUSHALL")})
		if underLock != nil {
			underLock()
		}
	})
}

func (j *journal) Update(key string, fn func(old []byte, exists bool) ([]byte, error)) error {
	return j.Store.Update(key, func(old []byte, exists bool) ([]byte, error) {
		nv, err := fn(old, exists)
		if err != nil {
			return nil, err
		}
		if err := j.emitCommand(); err != nil {
			return nil, err
		}
		return nv, nil
	})
}

func (j *journal) Mutate(key string, fn func(cur store.Value) (store.Value, error)) error {
	return j.Store.Mutate(key, func(cur store.Value) (store.Value, error) {
		nv, err := fn(cur)
		if err != nil {
			return nil, err
		}
		if err := j.emitCommand(); err != nil {
			return nil, err
		}
		return nv, nil
	})
}

func (j *journal) MutateEntry(key string, fn func(e *store.Entry) error) error {
	return j.Store.MutateEntry(key, func(e *store.Entry) error {
		if err := fn(e); err != nil {
			return err
		}
		return j.emitCommand()
	})
}

func (j *journal) Atomic(keys []string, fn func(entries []*store.Entry) error) error {
	return j.Store.Atomic(keys, func(es []*store.Entry) error {
		if err := fn(es); err != nil {
			return err
		}
		return j.emitCommand()
	})
}
