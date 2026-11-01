package bufio

import (
	"io"
	"os"
	"syscall"

	"github.com/metacubex/sing/common/buf"
	N "github.com/metacubex/sing/common/network"
)

// MaxReadBatchSize bounds both scratch descriptors and simultaneously owned
// buffers, including for callers using the read waiter directly.
const MaxReadBatchSize = 64

func normalizeReadBatchSize(size int) int {
	if size < 1 {
		return 1
	}
	if size > MaxReadBatchSize {
		return MaxReadBatchSize
	}
	return size
}

type vectorisedSyscallReadWaiter struct {
	rawConn     syscall.RawConn
	options     N.ReadWaitOptions
	buffers     []*buf.Buffer
	readResults []*buf.Buffer
	readBuffers int
	readErr     error
	readFunc    func(uintptr) bool
	syscallVectorisedReadFields
}

var (
	_ N.VectorisedReadWaiter = (*vectorisedSyscallReadWaiter)(nil)
	_ N.ReadWaiterReleaser   = (*vectorisedSyscallReadWaiter)(nil)
)

func createVectorisedSyscallReadWaiter(reader any) (*vectorisedSyscallReadWaiter, bool) {
	if conn, ok := reader.(syscall.Conn); ok {
		if raw, err := conn.SyscallConn(); err == nil {
			return &vectorisedSyscallReadWaiter{rawConn: raw}, true
		}
	}
	return nil, false
}

func (w *vectorisedSyscallReadWaiter) InitializeReadWaiter(options N.ReadWaitOptions) bool {
	w.ReleaseReadWaiter()
	options.BatchSize = normalizeReadBatchSize(options.BatchSize)
	w.options = options
	w.buffers = make([]*buf.Buffer, options.BatchSize)
	w.readResults = make([]*buf.Buffer, options.BatchSize)
	w.initializeVectors(options.BatchSize)
	w.readFunc = w.read
	return false
}

func (w *vectorisedSyscallReadWaiter) prepareBuffers() {
	for index, buffer := range w.buffers {
		if buffer == nil {
			buffer = w.options.NewBuffer()
			w.buffers[index] = buffer
		}
		w.setVector(index, buffer)
	}
}

func (w *vectorisedSyscallReadWaiter) completeRead(size int) {
	if size == 0 {
		w.readErr = io.EOF
		return
	}
	for _, buffer := range w.buffers {
		length := buffer.FreeLen()
		if length > size {
			length = size
		}
		buffer.Truncate(length)
		w.options.PostReturn(buffer)
		w.readBuffers++
		size -= length
		if size == 0 {
			break
		}
	}
}

func (w *vectorisedSyscallReadWaiter) releaseBuffers() {
	for index, buffer := range w.buffers {
		buffer.Release()
		w.buffers[index] = nil
	}
	for index := range w.readResults {
		w.readResults[index] = nil
	}
	w.clearVectors()
	w.readBuffers = 0
}

func (w *vectorisedSyscallReadWaiter) ReleaseReadWaiter() {
	w.releaseBuffers()
	w.buffers, w.readResults = nil, nil
	w.readFunc, w.readErr = nil, nil
	w.syscallVectorisedReadFields = syscallVectorisedReadFields{}
}

func (w *vectorisedSyscallReadWaiter) WaitReadBuffers() ([]*buf.Buffer, error) {
	if w.readFunc == nil {
		return nil, os.ErrInvalid
	}
	w.readErr, w.readBuffers = nil, 0
	err := w.rawConn.Read(w.readFunc)
	if err == nil {
		err = w.readErr
	}
	if err != nil {
		w.releaseBuffers()
		return nil, err
	}
	for index := range w.readResults {
		w.readResults[index] = nil
	}
	for index := 0; index < w.readBuffers; index++ {
		w.readResults[index] = w.buffers[index]
		w.buffers[index] = nil
	}
	w.clearVectors()
	return w.readResults[:w.readBuffers], nil
}
