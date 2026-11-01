//go:build !linux

package bufio

import (
	"syscall"

	N "github.com/metacubex/sing/common/network"
)

func splice(source syscall.RawConn, destination syscall.RawConn, readCounters []N.CountFunc, writeCounters []N.CountFunc, beforeTransfer TransferHook, transferSize TransferSizeHook) (handed bool, n int64, err error) {
	return
}
