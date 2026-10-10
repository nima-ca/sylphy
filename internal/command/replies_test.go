package command

import (
	"strconv"
	"strings"
	"testing"
)

// Reply fixtures shared by the type-specific command tests.
const (
	rNullArray    = "*-1\r\n"
	rEmptyArray   = "*0\r\n"
	rNoSuchKey    = "-ERR no such key\r\n"
	rBadIndex     = "-ERR index out of range\r\n"
	rMustBePos    = "-ERR value is out of range, must be positive\r\n"
	rNotFloat     = "-ERR value is not a valid float\r\n"
	rOverflow     = "-ERR increment or decrement would overflow\r\n"
	rHashNotInt   = "-ERR hash value is not an integer\r\n"
	rHashNotFloat = "-ERR hash value is not a float\r\n"
	rNaNOrInf     = "-ERR increment would produce NaN or Infinity\r\n"
)

// rArray encodes an array of bulk strings.
func rArray(items ...string) string {
	var sb strings.Builder
	sb.WriteString("*" + strconv.Itoa(len(items)) + "\r\n")
	for _, it := range items {
		sb.WriteString(rBulk(it))
	}
	return sb.String()
}

// rIntArray encodes an array of integers.
func rIntArray(ns ...int64) string {
	var sb strings.Builder
	sb.WriteString("*" + strconv.Itoa(len(ns)) + "\r\n")
	for _, n := range ns {
		sb.WriteString(rInt(n))
	}
	return sb.String()
}

// parseBulk decodes a single bulk-string reply.
func parseBulk(t testing.TB, reply string) string {
	t.Helper()
	i := strings.Index(reply, "\r\n")
	if !strings.HasPrefix(reply, "$") || i < 0 {
		t.Fatalf("not a bulk reply: %q", reply)
	}
	n, err := strconv.Atoi(reply[1:i])
	if err != nil || n < 0 || len(reply) != i+2+n+2 {
		t.Fatalf("malformed bulk reply: %q", reply)
	}
	return reply[i+2 : i+2+n]
}

// parseBulkArray decodes an array of bulk strings; a null element becomes
// "<nil>". Unordered replies (SMEMBERS, SPOP, ...) are checked through it.
func parseBulkArray(t testing.TB, reply string) []string {
	t.Helper()
	rest := reply
	line := func() string {
		i := strings.Index(rest, "\r\n")
		if i < 0 {
			t.Fatalf("malformed reply: %q", reply)
		}
		l := rest[:i]
		rest = rest[i+2:]
		return l
	}
	head := line()
	if !strings.HasPrefix(head, "*") {
		t.Fatalf("not an array reply: %q", reply)
	}
	n, err := strconv.Atoi(head[1:])
	if err != nil || n < 0 {
		t.Fatalf("bad array header in %q", reply)
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		l := line()
		if l == "$-1" {
			out = append(out, "<nil>")
			continue
		}
		size, err := strconv.Atoi(strings.TrimPrefix(l, "$"))
		if !strings.HasPrefix(l, "$") || err != nil || size < 0 || len(rest) < size+2 {
			t.Fatalf("bad element %q in %q", l, reply)
		}
		out = append(out, rest[:size])
		rest = rest[size+2:]
	}
	if rest != "" {
		t.Fatalf("trailing bytes in %q", reply)
	}
	return out
}
