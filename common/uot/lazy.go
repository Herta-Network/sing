package uot

import (
	"io"
	"net"
	"sync"

	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	N "github.com/metacubex/sing/common/network"
)

type LazyClientConn struct {
	net.Conn
	writer         N.VectorisedWriterWithCount
	writeAccess    sync.Mutex
	writeErr       error
	request        Request
	requestWritten bool
}

func NewLazyClientConn(conn net.Conn, request Request) *LazyClientConn {
	return &LazyClientConn{
		Conn:    conn,
		request: request,
		writer:  bufio.NewVectorisedWriterWithCount(conn),
	}
}

func NewLazyConn(conn net.Conn, request Request) *Conn {
	clientConn := NewLazyClientConn(conn, request)
	return NewConn(clientConn, request)
}

func (c *LazyClientConn) Write(p []byte) (int, error) {
	n, err := c.WriteVectorisedCount([]*buf.Buffer{buf.As(p)})
	return int(n), err
}

func (c *LazyClientConn) WriteVectorised(buffers []*buf.Buffer) error {
	_, err := c.WriteVectorisedCount(buffers)
	return err
}

func (c *LazyClientConn) WriteVectorisedCount(buffers []*buf.Buffer) (int64, error) {
	c.writeAccess.Lock()
	defer c.writeAccess.Unlock()
	if c.writeErr != nil {
		buf.ReleaseMulti(buffers)
		return 0, c.writeErr
	}
	total := int64(buf.LenMulti(buffers))
	var requestLen int64
	vectors := buffers
	if !c.requestWritten {
		request, err := EncodeRequest(c.request)
		if err != nil {
			buf.ReleaseMulti(buffers)
			return 0, err
		}
		requestLen = int64(request.Len())
		vectors = append([]*buf.Buffer{request}, buffers...)
	}
	n, err := c.writer.WriteVectorisedCount(vectors)
	if n < 0 || n > total+requestLen {
		n, err = 0, io.ErrShortWrite
	} else if n != total+requestLen && err == nil {
		err = io.ErrShortWrite
	}
	if n >= requestLen {
		c.requestWritten = true
	}
	n -= requestLen
	if n < 0 {
		n = 0
	}
	if err != nil {
		c.writeErr = err
		_ = c.Conn.Close()
	}
	return n, err
}

func (c *LazyClientConn) NeedHandshake() bool {
	c.writeAccess.Lock()
	defer c.writeAccess.Unlock()
	return !c.requestWritten
}

func (c *LazyClientConn) ReaderReplaceable() bool {
	return true
}

func (c *LazyClientConn) WriterReplaceable() bool {
	c.writeAccess.Lock()
	defer c.writeAccess.Unlock()
	return c.requestWritten
}

func (c *LazyClientConn) Upstream() any {
	return c.Conn
}
