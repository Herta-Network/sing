package network

import (
	"github.com/metacubex/sing/common"
	"io"
	"syscall"
)

type SyscallReader interface {
	SyscallConnForRead() syscall.RawConn
	HandleSyscallReadError(inputErr error) ([]byte, error)
}

func SyscallAvailableForRead(reader io.Reader) bool {
	if _, ok := reader.(syscall.Conn); ok {
		return true
	}
	if _, ok := reader.(SyscallReader); ok {
		return true
	}
	if u, ok := reader.(ReaderWithUpstream); !ok || !u.ReaderReplaceable() {
		return false
	}
	if u, ok := reader.(WithUpstreamReader); ok {
		return SyscallAvailableForRead(u.UpstreamReader().(io.Reader))
	}
	if u, ok := reader.(common.WithUpstream); ok {
		return SyscallAvailableForRead(u.Upstream().(io.Reader))
	}
	return false
}

func SyscallConnForRead(reader io.Reader) (SyscallReader, syscall.RawConn) {
	if c, ok := reader.(syscall.Conn); ok {
		conn, _ := c.SyscallConn()
		return nil, conn
	}
	if c, ok := reader.(SyscallReader); ok {
		return c, c.SyscallConnForRead()
	}
	if u, ok := reader.(ReaderWithUpstream); !ok || !u.ReaderReplaceable() {
		return nil, nil
	}
	if u, ok := reader.(WithUpstreamReader); ok {
		return SyscallConnForRead(u.UpstreamReader().(io.Reader))
	}
	if u, ok := reader.(common.WithUpstream); ok {
		return SyscallConnForRead(u.Upstream().(io.Reader))
	}
	return nil, nil
}

type SyscallWriter interface {
	SyscallConnForWrite() syscall.RawConn
}

func SyscallAvailableForWrite(writer io.Writer) bool {
	if _, ok := writer.(syscall.Conn); ok {
		return true
	}
	if _, ok := writer.(SyscallWriter); ok {
		return true
	}
	if u, ok := writer.(WriterWithUpstream); !ok || !u.WriterReplaceable() {
		return false
	}
	if u, ok := writer.(WithUpstreamWriter); ok {
		return SyscallAvailableForWrite(u.UpstreamWriter().(io.Writer))
	}
	if u, ok := writer.(common.WithUpstream); ok {
		return SyscallAvailableForWrite(u.Upstream().(io.Writer))
	}
	return false
}

func SyscallConnForWrite(writer io.Writer) (SyscallWriter, syscall.RawConn) {
	if c, ok := writer.(syscall.Conn); ok {
		conn, _ := c.SyscallConn()
		return nil, conn
	}
	if c, ok := writer.(SyscallWriter); ok {
		return c, c.SyscallConnForWrite()
	}
	if u, ok := writer.(WriterWithUpstream); !ok || !u.WriterReplaceable() {
		return nil, nil
	}
	if u, ok := writer.(WithUpstreamWriter); ok {
		return SyscallConnForWrite(u.UpstreamWriter().(io.Writer))
	}
	if u, ok := writer.(common.WithUpstream); ok {
		return SyscallConnForWrite(u.Upstream().(io.Writer))
	}
	return nil, nil
}
