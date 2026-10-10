package command

import (
	"strconv"
	"strings"
)

// Reply fixtures shared by the type-specific command tests.
const (
	rNullArray  = "*-1\r\n"
	rEmptyArray = "*0\r\n"
	rNoSuchKey  = "-ERR no such key\r\n"
	rBadIndex   = "-ERR index out of range\r\n"
	rMustBePos  = "-ERR value is out of range, must be positive\r\n"
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
