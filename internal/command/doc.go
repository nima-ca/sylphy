// Package command implements Sylphy's command layer: a Registry of command
// specs, a Dispatcher that validates arity and converts handler errors into
// Redis-compatible RESP errors, and the Phase 1 command handlers.
//
// Handlers receive only the command's arguments (not its name). They must
// return a *ReplyError before writing anything if they want an error reply,
// since the dispatcher appends the error frame after whatever was written.
package command
