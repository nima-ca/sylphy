package command

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

func cmdPing(ctx *Context, args [][]byte) error {
	switch len(args) {
	case 0:
		ctx.W.WriteSimpleString("PONG")
	case 1:
		ctx.W.WriteBulk(args[0])
	default:
		return WrongArgs("ping")
	}
	return nil
}

func cmdEcho(ctx *Context, args [][]byte) error {
	ctx.W.WriteBulk(args[0])
	return nil
}

func cmdQuit(ctx *Context, _ [][]byte) error {
	ctx.W.WriteSimpleString("OK")
	if ctx.Conn != nil {
		ctx.Conn.Closing = true
	}
	return nil
}

// cmdSelect only supports database 0; Sylphy has a single keyspace for now.
func cmdSelect(ctx *Context, args [][]byte) error {
	idx, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return &ReplyError{Msg: "ERR invalid DB index"}
	}
	if idx != 0 {
		return &ReplyError{Msg: "ERR DB index is out of range"}
	}
	ctx.W.WriteSimpleString("OK")
	return nil
}

// cmdClient implements the CLIENT subcommands real clients send on connect.
func cmdClient(ctx *Context, args [][]byte) error {
	switch strings.ToUpper(string(args[0])) {
	case "SETNAME":
		if len(args) != 2 {
			return WrongArgs("client|setname")
		}
		if bytes.ContainsAny(args[1], " \r\n") {
			return &ReplyError{Msg: "ERR Client names cannot contain spaces, newlines or special characters."}
		}
		if ctx.Conn != nil {
			ctx.Conn.Name = string(args[1])
		}
		ctx.W.WriteSimpleString("OK")
	case "SETINFO":
		if len(args) != 3 {
			return WrongArgs("client|setinfo")
		}
		ctx.W.WriteSimpleString("OK")
	default:
		return &ReplyError{Msg: fmt.Sprintf("ERR unknown subcommand '%s'. Try CLIENT HELP.", trunc(args[0]))}
	}
	return nil
}

// cmdCommand is a compatibility shim: redis-cli and some drivers call COMMAND
// or COMMAND DOCS on connect and only need a well-formed reply.
func cmdCommand(ctx *Context, args [][]byte) error {
	if len(args) > 0 {
		switch sub := strings.ToUpper(string(args[0])); sub {
		case "DOCS":
		case "COUNT":
			if len(args) != 1 {
				return WrongArgs("command|count")
			}
		default:
			return &ReplyError{Msg: fmt.Sprintf("ERR unknown subcommand '%s'. Try COMMAND HELP.", trunc(args[0]))}
		}
	}
	ctx.W.WriteArrayHeader(0)
	return nil
}
