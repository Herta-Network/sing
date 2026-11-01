package udpnat

import (
	"context"
	"net"
	"net/netip"
	"sync/atomic"
	"time"

	"github.com/metacubex/sing/common"
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
	"github.com/metacubex/sing/common/pipe"
	"github.com/metacubex/sing/contrab/freelru"
	"github.com/metacubex/sing/contrab/maphash"
)

type Service struct {
	cache   freelru.Cache[netip.AddrPort, *natConn]
	handler N.UDPConnectionHandlerEx
	prepare PrepareFunc
	closed  atomic.Bool
}

// PrepareFunc returns a borrowed response writer. Closing a NAT session does
// not close this writer: it may use a shared listener. onClose runs once after
// the accepted session closes, including close during preparation.
type PrepareFunc func(source M.Socksaddr, destination M.Socksaddr, userData any) (bool, context.Context, N.PacketWriter, N.CloseHandlerFunc)

func New(handler N.UDPConnectionHandlerEx, prepare PrepareFunc, timeout time.Duration, shared bool) *Service {
	if timeout <= 0 {
		panic("invalid timeout")
	}
	var cache freelru.Cache[netip.AddrPort, *natConn]
	if !shared {
		cache = common.Must1(freelru.NewSynced[netip.AddrPort, *natConn](1024, maphash.NewHasher[netip.AddrPort]().Hash32))
	} else {
		cache = common.Must1(freelru.NewSharded[netip.AddrPort, *natConn](1024, maphash.NewHasher[netip.AddrPort]().Hash32))
	}
	cache.SetLifetime(timeout)
	cache.SetHealthCheck(func(_ netip.AddrPort, conn *natConn) bool { return !conn.isClosed() })
	cache.SetOnEvict(func(_ netip.AddrPort, conn *natConn) { _ = conn.Close() })
	return &Service{cache: cache, handler: handler, prepare: prepare}
}

// NewPacket borrows bufferSlices for the duration of the call. A full session
// queue drops the new datagram without blocking other sources.
func (s *Service) NewPacket(bufferSlices [][]byte, source M.Socksaddr, destination M.Socksaddr, userData any) {
	if s.closed.Load() || !source.Addr.IsValid() {
		return
	}
	var dataLen int
	for _, part := range bufferSlices {
		if len(part) > 65535-dataLen {
			return
		}
		dataLen += len(part)
	}
	conn, loaded, ok := s.cache.GetAndRefreshOrAdd(source.AddrPort(), func() (*natConn, bool) {
		return &natConn{cache: s.cache, localAddr: source, packetChan: make(chan *N.PacketBuffer, 64), doneChan: make(chan struct{}), readDeadline: pipe.MakeDeadline()}, true
	})
	if !ok {
		return
	}
	// An insertion may have raced with Close's purge. No user callbacks run
	// inside the cache lock, including prepare and eviction callbacks.
	if s.closed.Load() {
		_ = conn.Close()
		s.cache.Remove(source.AddrPort())
		return
	}
	var ctx context.Context
	if !loaded {
		accepted, preparedCtx, writer, onClose := s.prepare(source, destination, userData)
		ctx = preparedCtx
		if !accepted || ctx == nil || writer == nil || common.Done(ctx) {
			_ = conn.Close()
			if onClose != nil {
				onClose(net.ErrClosed)
			}
			return
		}
		if !conn.initialize(writer, onClose) {
			return
		}
	}
	options := conn.readOptions()
	buffer := options.NewBufferSize(dataLen)
	for _, part := range bufferSlices {
		_, _ = buffer.Write(part)
	}
	options.PostReturn(buffer)
	conn.deliver(buffer, destination)
	if !loaded && !conn.isClosed() {
		go s.handler.NewPacketConnectionEx(ctx, conn, source, destination, func(err error) { conn.closeWithError(err) })
	}
}

// NewPacketBatch consumes all buffers, including on invalid lengths or closure.
func (s *Service) NewPacketBatch(buffers []*buf.Buffer, sources []M.Socksaddr, destination M.Socksaddr, userData any) {
	defer buf.ReleaseMulti(buffers)
	if len(buffers) != len(sources) {
		return
	}
	for index, buffer := range buffers {
		if buffer != nil {
			s.NewPacket([][]byte{buffer.Bytes()}, sources[index], destination, userData)
		}
	}
}

func (s *Service) Purge()        { s.cache.Purge() }
func (s *Service) PurgeExpired() { s.cache.PurgeExpired() }
func (s *Service) Len() int      { return s.cache.Len() }
func (s *Service) Close() error {
	if !s.closed.Swap(true) {
		s.Purge()
	}
	return nil
}
