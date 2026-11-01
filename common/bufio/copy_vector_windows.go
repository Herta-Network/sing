package bufio

import (
	"os"

	"github.com/metacubex/sing/common/buf"

	"golang.org/x/sys/windows"
)

type syscallVectorisedReadFields struct {
	iovecList []windows.WSABuf
	hasData   bool
}

func (w *vectorisedSyscallReadWaiter) initializeVectors(size int) {
	w.iovecList = make([]windows.WSABuf, size)
}

func (w *vectorisedSyscallReadWaiter) setVector(index int, buffer *buf.Buffer) {
	w.iovecList[index] = buffer.Iovec(buffer.FreeLen())
}

func (w *vectorisedSyscallReadWaiter) clearVectors() {
	w.hasData = false
	for index := range w.iovecList {
		w.iovecList[index] = windows.WSABuf{}
	}
}

func (w *vectorisedSyscallReadWaiter) read(fd uintptr) bool {
	if !w.hasData {
		// Match the existing Windows read waiter: RawRead uses a zero-byte
		// read to wait for readability before a synchronous receive.
		w.hasData = true
		return false
	}
	w.prepareBuffers()
	var n, flags uint32
	err := windows.WSARecv(windows.Handle(fd), &w.iovecList[0], uint32(len(w.iovecList)), &n, &flags, nil, nil)
	if err == windows.WSAEWOULDBLOCK {
		return false
	}
	w.hasData = false
	if err != nil {
		w.readErr = os.NewSyscallError("WSARecv", err)
		return true
	}
	w.completeRead(int(n))
	return true
}
