package command

import (
	"errors"
	"testing"

	"github.com/nima-ca/sylphy/internal/store"
)

func TestWrongTypeReplies(t *testing.T) {
	h := newHarness(t)
	h.putList(t, "l")
	h.run(t, []step{
		do(rWrongType, "GET", "l"),
		do(rWrongType, "STRLEN", "l"),
		do(rWrongType, "APPEND", "l", "x"),
		do(rWrongType, "INCR", "l"),
		do(rWrongType, "INCRBY", "l", "5"),
		// SET ... GET fails before changing anything, with or without NX/XX.
		do(rWrongType, "SET", "l", "v", "GET"),
		do(rWrongType, "SET", "l", "v", "NX", "GET"),
		do(rWrongType, "GETDEL", "l"),
		do(rWrongType, "GETEX", "l"),
		do(rWrongType, "GETEX", "l", "EX", "10"),
		do(rWrongType, "GETEX", "l", "PERSIST"),
		do(rInt(-1), "TTL", "l"), // GETEX did not touch the TTL
		do(rInt(1), "EXISTS", "l"),

		// Commands that do not care about the type work on it.
		do("*1\r\n$-1\r\n", "MGET", "l"), // MGET reports non-strings as nil
		do(rNil, "SET", "l", "v", "NX"),  // exists, so NX declines; no GET, so no error
		do(rInt(0), "SETNX", "l", "v"),
		do(rInt(1), "EXPIRE", "l", "10"),
		do(rInt(10), "TTL", "l"),
		do(rInt(1), "PERSIST", "l"),
		do(rInt(1), "DEL", "l"),
	})

	// SET and SETEX overwrite any type and discard its TTL.
	h.putList(t, "l")
	h.run(t, []step{
		do(rInt(1), "EXPIRE", "l", "10"),
		do(rOK, "SET", "l", "v"),
		do(rBulk("v"), "GET", "l"),
		do(rInt(-1), "TTL", "l"),
	})
	h.putList(t, "l")
	h.run(t, []step{
		do(rOK, "SET", "l", "v", "EX", "10"),
		do(rBulk("v"), "GET", "l"),
	})
	h.putList(t, "l")
	h.run(t, []step{
		do(rOK, "SETEX", "l", "10", "w"),
		do(rBulk("w"), "GET", "l"),
	})
}

func TestDispatcherMapsStoreWrongType(t *testing.T) {
	h := newHarness(t)
	reg := NewRegistry()
	err := reg.Register(Spec{Name: "BOOM", Arity: 1, Handler: func(*Context, [][]byte) error {
		return errors.Join(errors.New("context"), store.ErrWrongType) // wrapped errors must still map
	}})
	if err != nil {
		t.Fatal(err)
	}
	h.d = NewDispatcher(reg, nil)
	if got := h.do("BOOM"); got != rWrongType {
		t.Fatalf("got %q", got)
	}
}
