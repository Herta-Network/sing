//go:build !windows

package bufio

import (
	"os"
	"unsafe"

	"github.com/metacubex/sing/common/buf"

	"golang.org/x/sys/unix"
)

type syscallVectorisedReadFields struct {
	iovecList []unix.Iovec
}

func (w *vectorisedSyscallReadWaiter) initializeVectors(size int) {
	w.iovecList = make([]unix.Iovec, size)
}

func (w *vectorisedSyscallReadWaiter) setVector(index int, buffer *buf.Buffer) {
	w.iovecList[index] = buffer.Iovec(buffer.FreeLen())
}

func (w *vectorisedSyscallReadWaiter) clearVectors() {
	for index := range w.iovecList {
		w.iovecList[index] = unix.Iovec{}
	}
}

func (w *vectorisedSyscallReadWaiter) read(fd uintptr) bool {
	w.prepareBuffers()
	for {
		// The descriptors are reused and the buffers remain owned by the waiter
		// until this syscall completes and WaitReadBuffers transfers them.
		n, _, errno := unix.Syscall(unix.SYS_READV, fd, uintptr(unsafe.Pointer(&w.iovecList[0])), uintptr(len(w.iovecList)))
		if errno == unix.EINTR {
			continue
		}
		if errno == unix.EAGAIN || errno == unix.EWOULDBLOCK {
			return false
		}
		if errno != 0 {
			w.readErr = os.NewSyscallError("readv", errno)
			return true
		}
		w.completeRead(int(n))
		return true
	}
}
