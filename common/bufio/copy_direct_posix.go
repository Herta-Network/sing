//go:build !windows

package bufio

import (
	"io"
	"net/netip"
	"os"
	"syscall"

	"github.com/metacubex/sing/common/buf"
	E "github.com/metacubex/sing/common/exceptions"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

var _ N.ReadWaiter = (*syscallReadWaiter)(nil)

type syscallReadWaiter struct {
	rawConn  syscall.RawConn
	readErr  error
	readFunc func(fd uintptr) (done bool)
	buffer   *buf.Buffer
	options  N.ReadWaitOptions
}

func createSyscallReadWaiter(reader any) (*syscallReadWaiter, bool) {
	if syscallConn, isSyscallConn := reader.(syscall.Conn); isSyscallConn {
		rawConn, err := syscallConn.SyscallConn()
		if err == nil {
			return &syscallReadWaiter{rawConn: rawConn}, true
		}
	}
	return nil, false
}

func (w *syscallReadWaiter) InitializeReadWaiter(options N.ReadWaitOptions) (needCopy bool) {
	w.ReleaseReadWaiter()
	w.options = options
	w.readFunc = func(fd uintptr) (done bool) {
		buffer := w.options.NewBuffer()
		var readN int
		readN, w.readErr = syscall.Read(int(fd), buffer.FreeBytes())
		if readN > 0 {
			buffer.Truncate(readN)
			w.options.PostReturn(buffer)
			w.buffer = buffer
		} else {
			buffer.Release()
		}
		//goland:noinspection GoDirectComparisonOfErrors
		if w.readErr == syscall.EAGAIN {
			return false
		}
		if readN == 0 && w.readErr == nil {
			w.readErr = io.EOF
		}
		return true
	}
	return false
}

func (w *syscallReadWaiter) WaitReadBuffer() (buffer *buf.Buffer, err error) {
	if w.readFunc == nil {
		return nil, os.ErrInvalid
	}
	err = w.rawConn.Read(w.readFunc)
	if err != nil {
		w.buffer.Release()
		w.buffer = nil
		return
	}
	if w.readErr != nil {
		w.buffer.Release()
		w.buffer = nil
		if w.readErr == io.EOF {
			return nil, io.EOF
		}
		return nil, E.Cause(w.readErr, "raw read")
	}
	buffer = w.buffer
	w.buffer = nil
	return
}

func (w *syscallReadWaiter) ReleaseReadWaiter() {
	w.buffer.Release()
	w.buffer, w.readFunc, w.readErr = nil, nil, nil
}

var _ N.PacketReadWaiter = (*syscallPacketReadWaiter)(nil)

type syscallPacketReadWaiter struct {
	rawConn  syscall.RawConn
	readErr  error
	readFrom M.Socksaddr
	readFunc func(fd uintptr) (done bool)
	buffer   *buf.Buffer
	options  N.ReadWaitOptions
}

func createSyscallPacketReadWaiter(reader any) (*syscallPacketReadWaiter, bool) {
	if syscallConn, isSyscallConn := reader.(syscall.Conn); isSyscallConn {
		rawConn, err := syscallConn.SyscallConn()
		if err == nil {
			return &syscallPacketReadWaiter{rawConn: rawConn}, true
		}
	}
	return nil, false
}

func (w *syscallPacketReadWaiter) InitializeReadWaiter(options N.ReadWaitOptions) (needCopy bool) {
	w.ReleaseReadWaiter()
	w.options = options
	w.readFunc = func(fd uintptr) (done bool) {
		buffer := w.options.NewPacketBuffer()
		var readN int
		var flags int
		var from syscall.Sockaddr
		readN, _, flags, from, w.readErr = syscall.Recvmsg(int(fd), buffer.FreeBytes(), nil, 0)
		//goland:noinspection GoDirectComparisonOfErrors
		if w.readErr != nil {
			buffer.Release()
			return w.readErr != syscall.EAGAIN
		}
		if flags&syscall.MSG_TRUNC != 0 || readN > buffer.FreeLen() {
			buffer.Release()
			w.readErr = io.ErrShortBuffer
			return true
		}
		if readN > 0 {
			buffer.Truncate(readN)
		}
		w.options.PostReturn(buffer)
		w.buffer = buffer
		switch fromAddr := from.(type) {
		case *syscall.SockaddrInet4:
			w.readFrom = M.SocksaddrFrom(netip.AddrFrom4(fromAddr.Addr), uint16(fromAddr.Port))
		case *syscall.SockaddrInet6:
			w.readFrom = M.SocksaddrFrom(netip.AddrFrom16(fromAddr.Addr), uint16(fromAddr.Port)).Unwrap()
		}
		return true
	}
	return false
}

func (w *syscallPacketReadWaiter) WaitReadPacket() (buffer *buf.Buffer, destination M.Socksaddr, err error) {
	if w.readFunc == nil {
		return nil, M.Socksaddr{}, os.ErrInvalid
	}
	err = w.rawConn.Read(w.readFunc)
	if err != nil {
		w.buffer.Release()
		w.buffer = nil
		return
	}
	if w.readErr != nil {
		w.buffer.Release()
		w.buffer = nil
		err = E.Cause(w.readErr, "raw read")
		return
	}
	buffer = w.buffer
	w.buffer = nil
	destination = w.readFrom
	return
}

func (w *syscallPacketReadWaiter) ReleaseReadWaiter() {
	w.buffer.Release()
	w.buffer, w.readFunc, w.readErr = nil, nil, nil
	w.readFrom = M.Socksaddr{}
}
