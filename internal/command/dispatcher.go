package command

import (
	"errors"
	"log/slog"
	"strings"
)

// Dispatcher resolves commands, validates arity, runs handlers and turns their
// errors into RESP error replies.
type Dispatcher struct {
	reg *Registry
	log *slog.Logger
}

// NewDispatcher returns a Dispatcher over reg. A nil logger discards logs.
func NewDispatcher(reg *Registry, log *slog.Logger) *Dispatcher {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Dispatcher{reg: reg, log: log}
}

// Dispatch executes argv (command name first) and writes the reply to ctx.W.
// It never returns an error: every failure becomes a RESP error reply. Reply
// write errors are surfaced later by Writer.Flush.
func (d *Dispatcher) Dispatch(ctx *Context, argv [][]byte) {
	if len(argv) == 0 {
		return
	}
	spec, ok := d.reg.Lookup(argv[0])
	if !ok {
		ctx.W.WriteError(unknownCommandMsg(argv))
		return
	}
	if !arityOK(spec.Arity, len(argv)) {
		ctx.W.WriteError(WrongArgs(spec.Name).Msg)
		return
	}
	if err := spec.Handler(ctx, argv[1:]); err != nil {
		var re *ReplyError
		if errors.As(err, &re) {
			ctx.W.WriteError(re.Msg)
			return
		}
		d.log.Error("command failed", "command", spec.Name, "err", err) // never log args: they may hold values
		ctx.W.WriteError("ERR internal error")
	}
}

func arityOK(arity, n int) bool {
	if arity > 0 {
		return n == arity
	}
	return n >= -arity
}

func trunc(b []byte) string {
	if len(b) > 128 {
		b = b[:128]
	}
	return string(b)
}

// unknownCommandMsg mimics Redis: the offending name and the first few args are
// echoed back (truncated) to help debug client mistakes.
func unknownCommandMsg(argv [][]byte) string {
	var sb strings.Builder
	sb.WriteString("ERR unknown command '")
	sb.WriteString(trunc(argv[0]))
	sb.WriteString("', with args beginning with: ")
	for _, a := range argv[1:] {
		if sb.Len() > 256 {
			break
		}
		sb.WriteByte('\'')
		sb.WriteString(trunc(a))
		sb.WriteString("' ")
	}
	return sb.String()
}
