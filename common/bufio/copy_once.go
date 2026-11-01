package bufio

import (
	"io"

	"github.com/metacubex/sing/common"
	"github.com/metacubex/sing/common/buf"
	N "github.com/metacubex/sing/common/network"
)

func CopyExtendedOnce(destination io.Writer, source io.Reader, readCounters []N.CountFunc, writeCounters []N.CountFunc) (n int64, err error) {
	return copyExtendedOnceWithOptions(destination, source, readCounters, writeCounters, CopyOptions{})
}

func copyExtendedOnceWithOptions(destination io.Writer, source io.Reader, readCounters []N.CountFunc, writeCounters []N.CountFunc, policy CopyOptions) (n int64, err error) {
	options := N.NewReadWaitOptions(source, destination)
	readPolicy := policy.readOptions()
	options.MaxBufferSize = readPolicy.MaxBufferSize
	options.IncreaseBuffer = readPolicy.IncreaseBuffer
	var buffer *buf.Buffer
	calledWaitRead := false
	if readWaiter, isReadWaiter := CreateReadWaiter(source); isReadWaiter {
		defer releaseReadWaiter(readWaiter)
		if needCopy := readWaiter.InitializeReadWaiter(options); !needCopy || common.LowMemory {
			calledWaitRead = true
			buffer, err = readWaiter.WaitReadBuffer()
		}
	}
	if !calledWaitRead {
		buffer = options.NewBuffer()
		err = NewExtendedReader(source).ReadBuffer(buffer)
		options.PostReturn(buffer)
	}
	if err != nil {
		buffer.Release()
		return
	}

	dataLen := buffer.Len()
	if err = policy.beforeTransfer(dataLen); err != nil {
		buffer.Release()
		return
	}
	err = NewExtendedWriter(destination).WriteBuffer(buffer)
	if err != nil {
		buffer.Release()
		return
	}
	for _, counter := range readCounters {
		counter(int64(dataLen))
	}
	for _, counter := range writeCounters {
		counter(int64(dataLen))
	}
	n = int64(dataLen)

	return
}
