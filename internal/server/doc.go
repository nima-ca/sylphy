// Package server implements Sylphy's TCP server: accepting connections,
// per-connection request loops with pipelining, idle timeouts, a client limit,
// panic isolation, and graceful shutdown.
//
// Goroutine ownership: Serve owns the accept-loop goroutine and waits for it;
// each accepted connection is owned by a goroutine tracked in Server.wg;
// Shutdown owns a short helper that waits on that WaitGroup. Every one of them
// exits when the listener closes and the connection drains or is force-closed.
package server
