//go:build linux || netbsd || darwin

package bufio

import (
	"io"
	"syscall"

	"github.com/metacubex/sing/common/control"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"

	"golang.org/x/sys/unix"
)

func syscallPacketBatchRawConnForRead(reader any) syscall.RawConn {
	if syscallConn, isSyscallConn := reader.(syscall.Conn); isSyscallConn {
		rawConn, err := syscallConn.SyscallConn()
		if err == nil {
			return datagramBatchRawConn(rawConn)
		}
	}
	if ioReader, isReader := reader.(io.Reader); isReader {
		_, rawConn := N.SyscallConnForRead(ioReader)
		return datagramBatchRawConn(rawConn)
	}
	return nil
}

func syscallPacketBatchRawConnForWrite(writer any) syscall.RawConn {
	if syscallConn, isSyscallConn := writer.(syscall.Conn); isSyscallConn {
		rawConn, err := syscallConn.SyscallConn()
		if err == nil {
			return datagramBatchRawConn(rawConn)
		}
	}
	if ioWriter, isWriter := writer.(io.Writer); isWriter {
		_, rawConn := N.SyscallConnForWrite(ioWriter)
		return datagramBatchRawConn(rawConn)
	}
	return nil
}

// Packet progress relies on datagram writes being atomic. A connected TCP
// socket also has a peer address, but sendmmsg may accept only part of a message.
func datagramBatchRawConn(raw syscall.RawConn) syscall.RawConn {
	if raw == nil {
		return nil
	}
	socketType := 0
	err := control.Raw(raw, func(fd uintptr) error {
		var err error
		socketType, err = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_TYPE)
		return err
	})
	if err != nil || socketType != unix.SOCK_DGRAM {
		return nil
	}
	return raw
}

func syscallPacketBatchPeerDestination(rawConn syscall.RawConn) (M.Socksaddr, bool) {
	if rawConn == nil {
		return M.Socksaddr{}, false
	}
	var destination M.Socksaddr
	err := control.Raw(rawConn, func(fd uintptr) error {
		peer, err := unix.Getpeername(int(fd))
		if err != nil {
			return err
		}
		destination = M.SocksaddrFromNetIP(M.AddrPortFromSockaddr(peer)).Unwrap()
		return nil
	})
	return destination, err == nil && destination.IsValid()
}
