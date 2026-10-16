package command

import (
	"context"
	"errors"
)

// Replies for the persistence commands.
var (
	// ErrPersistenceOff is the reply when persistence is not enabled.
	ErrPersistenceOff = &ReplyError{Msg: "ERR persistence is disabled; start the server with -persistence-mode snapshot or aof"}
	// ErrAOFOff is the reply to BGREWRITEAOF when no append-only file is kept.
	ErrAOFOff = &ReplyError{Msg: "ERR the append-only file is not enabled; start the server with -persistence-mode aof"}

	errSaveRunning    = &ReplyError{Msg: "ERR Background save already in progress"}
	errRewriteRunning = &ReplyError{Msg: "ERR Background append only file rewriting already in progress"}
)

// persistSpecs returns the persistence command table. They are admin commands,
// not writes: they must keep working while persistence is failed (a checkpoint
// is how it recovers) and they are never logged.
func persistSpecs() []Spec {
	const (
		adm = FlagAdmin
		f   = FlagFast
	)
	return []Spec{
		{Name: "SAVE", Arity: 1, Flags: adm, Handler: cmdSave},
		{Name: "BGSAVE", Arity: 1, Flags: adm, Handler: cmdBGSave},
		// BGREWRITEAOF and BGSAVE are the same checkpoint: a new base snapshot
		// plus a fresh log segment is both a snapshot and a rewrite.
		{Name: "BGREWRITEAOF", Arity: 1, Flags: adm, Handler: cmdBGRewriteAOF},
		{Name: "LASTSAVE", Arity: 1, Flags: adm | f, Handler: cmdLastSave},
	}
}

// cmdSave implements SAVE: a blocking checkpoint. The caller's connection waits
// for it; other clients keep being served.
func cmdSave(ctx *Context, _ [][]byte) error {
	if ctx.Persist == nil {
		return ErrPersistenceOff
	}
	if err := ctx.Persist.Save(context.Background()); err != nil {
		if errors.Is(err, ErrCheckpointRunning) {
			return errSaveRunning
		}
		return err
	}
	ctx.W.WriteSimpleString("OK")
	return nil
}

// cmdBGSave implements BGSAVE: start a checkpoint in the background.
func cmdBGSave(ctx *Context, _ [][]byte) error {
	if ctx.Persist == nil {
		return ErrPersistenceOff
	}
	if err := ctx.Persist.BGSave(); err != nil {
		if errors.Is(err, ErrCheckpointRunning) {
			return errSaveRunning
		}
		return err
	}
	ctx.W.WriteSimpleString("Background saving started")
	return nil
}

// cmdBGRewriteAOF implements BGREWRITEAOF. Without an append-only file there
// is nothing to rewrite.
func cmdBGRewriteAOF(ctx *Context, _ [][]byte) error {
	if ctx.Persist == nil {
		return ErrPersistenceOff
	}
	if !ctx.Persist.AOFEnabled() {
		return ErrAOFOff
	}
	if err := ctx.Persist.BGSave(); err != nil {
		if errors.Is(err, ErrCheckpointRunning) {
			return errRewriteRunning
		}
		return err
	}
	ctx.W.WriteSimpleString("Background append only file rewriting started")
	return nil
}

// cmdLastSave implements LASTSAVE: the Unix time of the last checkpoint, or of
// startup if there has not been one. It is 0 when persistence is off.
func cmdLastSave(ctx *Context, _ [][]byte) error {
	if ctx.Persist == nil {
		ctx.W.WriteInteger(0)
		return nil
	}
	ctx.W.WriteInteger(ctx.Persist.LastSave().Unix())
	return nil
}
