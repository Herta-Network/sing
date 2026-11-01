package udpnat

import (
	"context"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metacubex/sing/common"
	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/cache"
	E "github.com/metacubex/sing/common/exceptions"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
	"github.com/metacubex/sing/common/pipe"
)

type Handler interface {
	N.UDPConnectionHandler
	E.Handler
}

type Service[K comparable] struct {
	nat     *cache.LruCache[K, *conn]
	handler Handler
	closed  atomic.Bool
}

func New[K comparable](maxAge int64, handler Handler) *Service[K] {
	return &Service[K]{
		nat: cache.New(
			cache.WithAge[K, *conn](maxAge),
			cache.WithUpdateAgeOnGet[K, *conn](),
			cache.WithEvict[K, *conn](func(key K, conn *conn) {
				conn.Close()
			}),
		),
		handler: handler,
	}
}

func (s *Service[T]) WriteIsThreadUnsafe() {
}

func (s *Service[T]) NewPacketDirect(ctx context.Context, key T, conn N.PacketConn, buffer *buf.Buffer, metadata M.Metadata) {
	s.NewContextPacket(ctx, key, buffer, metadata, func(natConn N.PacketConn) (context.Context, N.PacketWriter) {
		return ctx, &DirectBackWriter{conn, natConn}
	})
}

type DirectBackWriter struct {
	Source N.PacketConn
	Nat    N.PacketConn
}

func (w *DirectBackWriter) WritePacket(buffer *buf.Buffer, addr M.Socksaddr) error {
	return w.Source.WritePacket(buffer, M.SocksaddrFromNet(w.Nat.LocalAddr()))
}

func (w *DirectBackWriter) Upstream() any {
	return w.Source
}

func (s *Service[T]) NewPacket(ctx context.Context, key T, buffer *buf.Buffer, metadata M.Metadata, init func(natConn N.PacketConn) N.PacketWriter) {
	s.NewContextPacket(ctx, key, buffer, metadata, func(natConn N.PacketConn) (context.Context, N.PacketWriter) {
		return ctx, init(natConn)
	})
}

// NewContextPacket consumes buffer. Packets arriving at a full session queue
// are dropped and released so one slow session cannot block the receive loop.
func (s *Service[T]) NewContextPacket(ctx context.Context, key T, buffer *buf.Buffer, metadata M.Metadata, init func(natConn N.PacketConn) (context.Context, N.PacketWriter)) {
	for {
		if s.closed.Load() || common.Done(ctx) {
			buffer.Release()
			return
		}
		c, loaded := s.nat.LoadOrStore(key, func() *conn {
			c := &conn{
				data:         make(chan packet, 64),
				localAddr:    metadata.Source,
				remoteAddr:   metadata.Destination,
				readDeadline: pipe.MakeDeadline(),
			}
			c.ctx, c.cancel = common.ContextWithCancelCause(ctx)
			return c
		})
		// Close can race with insertion after clearing the cache.
		if s.closed.Load() || common.Done(ctx) {
			// An individual packet's context does not own an existing session.
			if s.closed.Load() || !loaded {
				s.deleteConnection(key, c)
			}
			buffer.Release()
			return
		}
		handlerContext := ctx
		if !loaded {
			var source N.PacketWriter
			handlerContext, source = init(c)
			if !c.initializeSource(source) || s.closed.Load() || common.Done(handlerContext) {
				s.deleteConnection(key, c)
				buffer.Release()
				return
			}
		} else {
			c.stateAccess.Lock()
			c.localAddr = metadata.Source
			c.stateAccess.Unlock()
		}
		if c.enqueue(packet{data: buffer, destination: metadata.Destination}) {
			if !loaded {
				// Publish the first packet before a handler can reject/close the
				// session, rather than retrying that packet into another session.
				go s.handleConnection(handlerContext, key, c, metadata)
			}
			return
		}
		// A closed session may be replaced while its old handler is still exiting.
		s.deleteConnection(key, c)
		if !loaded {
			buffer.Release()
			return
		}
	}
}

func (s *Service[T]) handleConnection(ctx context.Context, key T, c *conn, metadata M.Metadata) {
	defer s.deleteConnection(key, c)
	if err := s.handler.NewPacketConnection(ctx, c, metadata); err != nil {
		s.handler.NewError(ctx, err)
	}
}

func (s *Service[T]) deleteConnection(key T, c *conn) {
	c.Close()
	s.nat.DeleteFunc(key, func(current *conn) bool { return current == c })
}

// Close releases cached sessions and rejects later packets. Handler callbacks
// observe closed connections; Close does not wait for those callbacks to return.
func (s *Service[T]) Close() error {
	if !s.closed.Swap(true) {
		s.nat.Clear()
	}
	return nil
}

type packet struct {
	data        *buf.Buffer
	destination M.Socksaddr
}

var _ N.PacketConn = (*conn)(nil)

type conn struct {
	ctx             context.Context
	cancel          common.ContextCancelCauseFunc
	data            chan packet
	localAddr       M.Socksaddr
	remoteAddr      M.Socksaddr
	stateAccess     sync.RWMutex
	closed          bool
	closeErr        error
	source          N.PacketWriter
	readDeadline    pipe.Deadline
	readWaitOptions N.ReadWaitOptions
}

func (c *conn) ReadPacket(buffer *buf.Buffer) (addr M.Socksaddr, err error) {
	p, err := c.readPacket()
	if err != nil {
		return M.Socksaddr{}, err
	}
	_, err = buffer.ReadOnceFrom(p.data)
	p.data.Release()
	return p.destination, err
}

func (c *conn) readPacket() (packet, error) {
	if common.Done(c.ctx) {
		return packet{}, io.ErrClosedPipe
	}
	select {
	case p := <-c.data:
		if common.Done(c.ctx) {
			p.data.Release()
			return packet{}, io.ErrClosedPipe
		}
		return p, nil
	case <-c.ctx.Done():
		return packet{}, io.ErrClosedPipe
	case <-c.readDeadline.Wait():
		return packet{}, os.ErrDeadlineExceeded
	}
}

func (c *conn) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	c.stateAccess.RLock()
	source := c.source
	closed := c.closed || common.Done(c.ctx)
	c.stateAccess.RUnlock()
	if closed || source == nil {
		buffer.Release()
		return io.ErrClosedPipe
	}
	// Do not hold stateAccess across I/O: Close must interrupt a blocked write.
	return source.WritePacket(buffer, destination)
}

func (c *conn) Close() error {
	c.stateAccess.Lock()
	if c.closed {
		err := c.closeErr
		c.stateAccess.Unlock()
		return err
	}
	c.closed = true
	c.cancel(net.ErrClosed)
	source := c.source
	c.source = nil
	for {
		select {
		case p := <-c.data:
			p.data.Release()
		default:
			c.stateAccess.Unlock()
			// A source close hook may reenter the NAT/cache or this connection.
			err := common.Close(source)
			c.stateAccess.Lock()
			c.closeErr = err
			c.stateAccess.Unlock()
			return err
		}
	}
}

func (c *conn) initializeSource(source N.PacketWriter) bool {
	c.stateAccess.Lock()
	if c.closed || common.Done(c.ctx) {
		c.stateAccess.Unlock()
		common.Close(source)
		return false
	}
	c.source = source
	c.stateAccess.Unlock()
	return true
}

// enqueue transfers ownership on success, including when a full queue drops
// the packet. A closed connection leaves ownership with the caller for retry.
func (c *conn) enqueue(p packet) bool {
	c.stateAccess.RLock()
	if c.closed || common.Done(c.ctx) {
		c.stateAccess.RUnlock()
		return false
	}
	select {
	case c.data <- p:
		c.stateAccess.RUnlock()
		return true
	default:
		c.stateAccess.RUnlock()
		p.data.Release()
		return true
	}
}

func (c *conn) LocalAddr() net.Addr {
	c.stateAccess.RLock()
	defer c.stateAccess.RUnlock()
	return c.localAddr
}

func (c *conn) RemoteAddr() net.Addr {
	return c.remoteAddr
}

func (c *conn) SetDeadline(t time.Time) error {
	return os.ErrInvalid
}

func (c *conn) SetReadDeadline(t time.Time) error {
	c.readDeadline.Set(t)
	return nil
}

func (c *conn) SetWriteDeadline(t time.Time) error {
	return os.ErrInvalid
}

func (c *conn) NeedAdditionalReadDeadline() bool {
	return true
}

func (c *conn) Upstream() any {
	c.stateAccess.RLock()
	defer c.stateAccess.RUnlock()
	return c.source
}
