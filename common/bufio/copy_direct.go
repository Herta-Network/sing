package bufio

import (
	"errors"
	"io"
	"syscall"

	"github.com/metacubex/sing/common/buf"
	N "github.com/metacubex/sing/common/network"
)

const maxSpliceSize = 1 << 20

func resolveTransferSize(hook TransferSizeHook) int {
	if hook != nil {
		if size := hook(); size > 0 && size < maxSpliceSize {
			return size
		}
	}
	return maxSpliceSize
}

func copyDirect(source syscall.Conn, destination syscall.Conn, readCounters []N.CountFunc, writeCounters []N.CountFunc) (handed bool, n int64, err error) {
	return copyDirectWithOptions(source, destination, readCounters, writeCounters, CopyOptions{})
}

func copyDirectWithOptions(source syscall.Conn, destination syscall.Conn, readCounters []N.CountFunc, writeCounters []N.CountFunc, options CopyOptions) (handed bool, n int64, err error) {
	rawSource, err := source.SyscallConn()
	if err != nil {
		return
	}
	rawDestination, err := destination.SyscallConn()
	if err != nil {
		return
	}
	handed, n, err = splice(rawSource, rawDestination, readCounters, writeCounters, options.BeforeTransfer, options.TransferSize)
	return
}

func copyWaitWithPool(originSource io.Reader, destination N.ExtendedWriter, source N.ReadWaiter, readCounters []N.CountFunc, writeCounters []N.CountFunc) (handled bool, n int64, err error) {
	handled = true
	var (
		buffer       *buf.Buffer
		notFirstTime bool
	)
	for {
		buffer, err = source.WaitReadBuffer()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
				return
			}
			return
		}
		dataLen := buffer.Len()
		err = destination.WriteBuffer(buffer)
		if err != nil {
			buffer.Leak()
			if !notFirstTime {
				err = N.ReportHandshakeFailure(originSource, err)
			}
			return
		}
		n += int64(dataLen)
		for _, counter := range readCounters {
			counter(int64(dataLen))
		}
		for _, counter := range writeCounters {
			counter(int64(dataLen))
		}
		notFirstTime = true
	}
}
