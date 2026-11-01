//go:build darwin

package bufio

import (
	"io"
	"net/netip"
	"os"
	"sync"
	"syscall"
	"unsafe"

	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/control"
	E "github.com/metacubex/sing/common/exceptions"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"

	"golang.org/x/sys/unix"
)

var (
	_ N.PacketBatchReadWaiter          = (*syscallPacketBatchReadWaiter)(nil)
	_ N.ConnectedPacketBatchReadWaiter = (*syscallPacketBatchReadWaiter)(nil)
)

type msghdrX struct {
	name       *byte
	namelen    uint32
	iov        *unix.Iovec
	iovlen     int32
	control    *byte
	controllen uint32
	flags      int32
	datalen    uint64
}

type syscallPacketBatchReadWaiter struct {
	rawConn      syscall.RawConn
	connected    bool
	destination  M.Socksaddr
	readErr      error
	readN        int
	readFunc     func(fd uintptr) (done bool)
	buffers      []*buf.Buffer
	destinations []M.Socksaddr
	readBuffers  []*buf.Buffer
	readDests    []M.Socksaddr
	overflow     []byte
	names        []unix.RawSockaddrAny
	iovecs       []unix.Iovec
	msgvec       []msghdrX
	options      N.ReadWaitOptions
}

func createSyscallPacketBatchReadWaiter(reader any) (N.PacketBatchReadWaiter, bool) {
	rawConn := syscallPacketBatchRawConnForRead(reader)
	if rawConn == nil {
		return nil, false
	}
	if _, isConnected := syscallPacketBatchPeerDestination(rawConn); isConnected {
		return nil, false
	}
	return &syscallPacketBatchReadWaiter{rawConn: rawConn}, true
}

func createSyscallConnectedPacketBatchReadWaiter(reader any, destination M.Socksaddr) (N.ConnectedPacketBatchReadWaiter, bool) {
	rawConn := syscallPacketBatchRawConnForRead(reader)
	if rawConn == nil {
		return nil, false
	}
	peerDestination, isConnected := syscallPacketBatchPeerDestination(rawConn)
	if !isConnected {
		return nil, false
	}
	if !destination.IsValid() {
		destination = peerDestination
	}
	return &syscallPacketBatchReadWaiter{rawConn: rawConn, connected: true, destination: destination}, true
}

func (w *syscallPacketBatchReadWaiter) InitializeReadWaiter(options N.ReadWaitOptions) (needCopy bool) {
	w.ReleaseReadWaiter()
	if options.BatchSize <= 0 {
		options.BatchSize = DefaultPacketReadBatchSize
	}
	options.BatchSize = normalizeReadBatchSize(options.BatchSize)
	w.options = options
	w.buffers = make([]*buf.Buffer, options.BatchSize)
	w.readBuffers = make([]*buf.Buffer, options.BatchSize)
	if !w.connected {
		w.destinations = make([]M.Socksaddr, options.BatchSize)
		w.readDests = make([]M.Socksaddr, options.BatchSize)
		w.names = make([]unix.RawSockaddrAny, options.BatchSize)
	}
	w.iovecs = make([]unix.Iovec, options.BatchSize*2)
	w.overflow = make([]byte, options.BatchSize)
	w.msgvec = make([]msghdrX, options.BatchSize)
	w.readFunc = func(fd uintptr) (done bool) {
		for i := range w.msgvec {
			buffer := w.buffers[i]
			if buffer == nil {
				buffer = w.options.NewPacketBuffer()
				w.buffers[i] = buffer
			}
			w.iovecs[2*i] = buffer.Iovec(buffer.FreeLen())
			// recvmsg_x may report only copied bytes without a per-message
			// MSG_TRUNC flag. A separate guard byte detects an oversized packet
			// without doubling a power-of-two payload allocation.
			w.iovecs[2*i+1] = unix.Iovec{Base: &w.overflow[i]}
			w.iovecs[2*i+1].SetLen(1)
			w.msgvec[i] = msghdrX{
				iov:    &w.iovecs[2*i],
				iovlen: 2,
			}
			if !w.connected {
				w.names[i] = unix.RawSockaddrAny{}
				w.msgvec[i].name = (*byte)(unsafe.Pointer(&w.names[i]))
				w.msgvec[i].namelen = unix.SizeofSockaddrAny
			}
		}
		for {
			var errno syscall.Errno
			w.readN, errno = recvmsgX(int(fd), w.msgvec, 0)
			switch errno {
			case 0:
				w.readErr = nil
			case syscall.EINTR:
				continue
			case syscall.EAGAIN:
				return false
			default:
				if errno == syscall.EWOULDBLOCK {
					return false
				}
				w.readErr = os.NewSyscallError("recvmsg_x", errno)
			}
			break
		}
		if w.readN == 0 && w.readErr == nil {
			w.readErr = io.EOF
		}
		for i := 0; i < w.readN; i++ {
			buffer := w.buffers[i]
			if w.msgvec[i].flags&unix.MSG_TRUNC != 0 || w.msgvec[i].datalen > uint64(buffer.FreeLen()) {
				w.readErr = io.ErrShortBuffer
				return true
			}
			buffer.Truncate(int(w.msgvec[i].datalen))
			w.options.PostReturn(buffer)
			if !w.connected {
				w.destinations[i] = M.SocksaddrFromRawSockaddrAny(&w.names[i])
			}
		}
		return true
	}
	return false
}

func (w *syscallPacketBatchReadWaiter) WaitReadPackets() (buffers []*buf.Buffer, destinations []M.Socksaddr, err error) {
	if w.connected {
		return nil, nil, os.ErrInvalid
	}
	if w.readFunc == nil {
		return nil, nil, os.ErrInvalid
	}
	err = w.rawConn.Read(w.readFunc)
	if err != nil {
		w.releaseBuffers()
		return
	}
	if w.readErr != nil {
		w.releaseBuffers()
		if w.readErr == io.EOF {
			return nil, nil, io.EOF
		}
		return nil, nil, E.Cause(w.readErr, "raw read")
	}
	clearPacketSlice(w.readBuffers)
	clearPacketSlice(w.readDests)
	for i := 0; i < w.readN; i++ {
		w.readBuffers[i] = w.buffers[i]
		w.buffers[i] = nil
		w.readDests[i] = w.destinations[i]
	}
	buffers = w.readBuffers[:w.readN]
	destinations = w.readDests[:w.readN]
	w.readN = 0
	w.clearReadVectors()
	return
}

func (w *syscallPacketBatchReadWaiter) WaitReadConnectedPackets() (buffers []*buf.Buffer, destination M.Socksaddr, err error) {
	if !w.connected {
		return nil, M.Socksaddr{}, os.ErrInvalid
	}
	if w.readFunc == nil {
		return nil, M.Socksaddr{}, os.ErrInvalid
	}
	err = w.rawConn.Read(w.readFunc)
	if err != nil {
		w.releaseBuffers()
		return
	}
	if w.readErr != nil {
		w.releaseBuffers()
		if w.readErr == io.EOF {
			return nil, M.Socksaddr{}, io.EOF
		}
		return nil, M.Socksaddr{}, E.Cause(w.readErr, "raw read")
	}
	clearPacketSlice(w.readBuffers)
	for i := 0; i < w.readN; i++ {
		w.readBuffers[i] = w.buffers[i]
		w.buffers[i] = nil
	}
	buffers = w.readBuffers[:w.readN]
	w.readN = 0
	destination = w.destination
	w.clearReadVectors()
	return
}

var (
	_ N.PacketBatchWriter          = (*syscallPacketBatchWriter)(nil)
	_ N.ConnectedPacketBatchWriter = (*syscallConnectedPacketBatchWriter)(nil)
)

type syscallPacketBatchWriter struct {
	upstream  any
	rawConn   syscall.RawConn
	access    sync.Mutex
	write     packetMsgvecWriteState
	localAddr netip.AddrPort
	names     []unix.RawSockaddrAny
	iovecs    []unix.Iovec
	msgvec    []msghdrX
}

type syscallConnectedPacketBatchWriter struct {
	upstream any
	rawConn  syscall.RawConn
	access   sync.Mutex
	write    packetMsgvecWriteState
	iovecs   []unix.Iovec
	msgvec   []msghdrX
}

func createSyscallPacketBatchWriter(writer any) (N.PacketBatchWriter, bool) {
	rawConn := syscallPacketBatchRawConnForWrite(writer)
	if rawConn == nil {
		return nil, false
	}
	if _, isConnected := syscallPacketBatchPeerDestination(rawConn); isConnected {
		return nil, false
	}
	batchWriter := &syscallPacketBatchWriter{upstream: writer, rawConn: rawConn}
	batchWriter.write.initialize(rawConn)
	return batchWriter, true
}

func createSyscallConnectedPacketBatchWriter(writer any) (N.ConnectedPacketBatchWriter, bool) {
	rawConn := syscallPacketBatchRawConnForWrite(writer)
	if rawConn == nil {
		return nil, false
	}
	if _, isConnected := syscallPacketBatchPeerDestination(rawConn); !isConnected {
		return nil, false
	}
	batchWriter := &syscallConnectedPacketBatchWriter{upstream: writer, rawConn: rawConn}
	batchWriter.write.initialize(rawConn)
	return batchWriter, true
}

func (w *syscallPacketBatchWriter) WritePacketBatch(buffers []*buf.Buffer, destinations []M.Socksaddr) error {
	_, err := w.WritePacketBatchCount(buffers, destinations)
	return err
}

func (w *syscallPacketBatchWriter) WritePacketBatchCount(buffers []*buf.Buffer, destinations []M.Socksaddr) (int, error) {
	w.access.Lock()
	defer w.access.Unlock()
	defer buf.ReleaseMulti(buffers)
	if len(buffers) == 0 || len(buffers) != len(destinations) {
		return 0, os.ErrInvalid
	}
	for _, destination := range destinations {
		if !destination.IsIP() {
			return 0, os.ErrInvalid
		}
	}
	if !w.localAddr.IsValid() {
		err := control.Raw(w.rawConn, func(fd uintptr) error {
			name, err := unix.Getsockname(int(fd))
			if err != nil {
				return err
			}
			w.localAddr = M.AddrPortFromSockaddr(name)
			return nil
		})
		if err != nil {
			return 0, err
		}
	}
	names := growSlice(w.names, len(buffers))
	iovecs := growSlice(w.iovecs, len(buffers))
	msgvec := growSlice(w.msgvec, len(buffers))
	defer func() {
		clearPacketSlice(names)
		clearPacketSlice(iovecs)
		clearPacketSlice(msgvec)
		w.names = names[:0]
		w.iovecs = iovecs[:0]
		w.msgvec = msgvec[:0]
	}()
	for i, buffer := range buffers {
		names[i] = unix.RawSockaddrAny{}
		iovecs[i] = unix.Iovec{}
		msgvec[i] = msghdrX{
			name:    (*byte)(unsafe.Pointer(&names[i])),
			namelen: M.AddrPortToRawSockaddrAny(&names[i], destinations[i].AddrPort(), w.localAddr.Addr().Is6()),
			iov:     &iovecs[i],
			iovlen:  1,
		}
		if !buffer.IsEmpty() {
			iovecs[i] = buffer.Iovec(buffer.Len())
		}
	}
	return w.write.WriteCount(msgvec)
}

func (w *syscallPacketBatchWriter) Upstream() any {
	return w.upstream
}

func (w *syscallConnectedPacketBatchWriter) WriteConnectedPacketBatch(buffers []*buf.Buffer) error {
	_, err := w.WriteConnectedPacketBatchCount(buffers)
	return err
}

func (w *syscallConnectedPacketBatchWriter) WriteConnectedPacketBatchCount(buffers []*buf.Buffer) (int, error) {
	w.access.Lock()
	defer w.access.Unlock()
	defer buf.ReleaseMulti(buffers)
	if len(buffers) == 0 {
		return 0, os.ErrInvalid
	}
	iovecs := growSlice(w.iovecs, len(buffers))
	msgvec := growSlice(w.msgvec, len(buffers))
	defer func() {
		clearPacketSlice(iovecs)
		clearPacketSlice(msgvec)
		w.iovecs = iovecs[:0]
		w.msgvec = msgvec[:0]
	}()
	for i, buffer := range buffers {
		iovecs[i] = unix.Iovec{}
		msgvec[i] = msghdrX{
			iov:    &iovecs[i],
			iovlen: 1,
		}
		if !buffer.IsEmpty() {
			iovecs[i] = buffer.Iovec(buffer.Len())
		}
	}
	return w.write.WriteCount(msgvec)
}

func (w *syscallConnectedPacketBatchWriter) Upstream() any {
	return w.upstream
}

type packetBatchMessage = msghdrX

type packetMsgvecWriteState struct {
	send         func(int, []packetBatchMessage, int) (int, syscall.Errno)
	rawConn      syscall.RawConn
	writeMsgvec  []msghdrX
	maxBatchSize int
	innerErr     syscall.Errno
	writeFunc    func(fd uintptr) bool
}

func (w *packetMsgvecWriteState) initialize(rawConn syscall.RawConn) {
	w.send = sendmsgX
	w.rawConn = rawConn
	w.writeFunc = w.write
}

func (w *packetMsgvecWriteState) Write(msgvec []msghdrX) error {
	_, err := w.WriteCount(msgvec)
	return err
}

func (w *packetMsgvecWriteState) WriteCount(msgvec []msghdrX) (int, error) {
	w.writeMsgvec = msgvec
	w.maxBatchSize = len(msgvec)
	w.innerErr = 0
	err := w.rawConn.Write(w.writeFunc)
	written := len(msgvec) - len(w.writeMsgvec)
	w.writeMsgvec = nil
	if w.innerErr != 0 && w.innerErr != syscall.EAGAIN && w.innerErr != syscall.EWOULDBLOCK {
		return written, os.NewSyscallError("sendmsg_x", w.innerErr)
	}
	return written, err
}

func (w *packetMsgvecWriteState) write(fd uintptr) (done bool) {
writeLoop:
	for len(w.writeMsgvec) > 0 {
		batchSize := w.maxBatchSize
		if batchSize > len(w.writeMsgvec) {
			batchSize = len(w.writeMsgvec)
		}
		for index := 0; index < batchSize; index++ {
			message := &w.writeMsgvec[index]
			if message.iovlen == 0 || message.iov == nil || message.iov.Len == 0 {
				if index == 0 {
					// Darwin rejects zero-length datagrams in sendmsg_x on a
					// connected socket. sendmsg preserves the packet and ordering.
					//nolint:staticcheck
					_, _, errno := unix.Syscall(unix.SYS_SENDMSG, fd, uintptr(unsafe.Pointer(message)), 0)
					switch errno {
					case 0:
						w.writeMsgvec = w.writeMsgvec[1:]
					case syscall.EINTR:
					case syscall.EAGAIN:
						return false
					default:
						if errno == syscall.EWOULDBLOCK {
							return false
						}
						w.innerErr = errno
						return true
					}
					continue writeLoop
				}
				batchSize = index
				break
			}
		}
		n, errno := w.send(int(fd), w.writeMsgvec[:batchSize], 0)
		switch {
		case errno == 0:
		case errno == syscall.EINTR:
			continue
		case errno == syscall.EMSGSIZE && batchSize > 1:
			w.maxBatchSize = (batchSize + 1) / 2
			continue
		case errno == syscall.EAGAIN || errno == syscall.EWOULDBLOCK:
			return false
		default:
			w.innerErr = errno
			return true
		}
		if n == 0 {
			w.innerErr = syscall.EIO
			return true
		}
		w.writeMsgvec = w.writeMsgvec[n:]
	}
	return true
}

func growSlice[T any](values []T, size int) []T {
	if cap(values) < size {
		return make([]T, size)
	}
	return values[:size]
}

func recvmsgX(fd int, msgvec []msghdrX, flags int) (int, syscall.Errno) {
	//nolint:staticcheck
	return msgxSyscall(unix.SYS_RECVMSG_X, fd, msgvec, flags)
}

func sendmsgX(fd int, msgvec []msghdrX, flags int) (int, syscall.Errno) {
	//nolint:staticcheck
	return msgxSyscall(unix.SYS_SENDMSG_X, fd, msgvec, flags)
}

func msgxSyscall(trap uintptr, fd int, msgvec []msghdrX, flags int) (int, syscall.Errno) {
	r0, _, errno := unix.Syscall6(trap, uintptr(fd), uintptr(unsafe.Pointer(&msgvec[0])), uintptr(len(msgvec)), uintptr(flags), 0, 0)
	if errno != 0 {
		return 0, errno
	}
	return int(r0), 0
}
