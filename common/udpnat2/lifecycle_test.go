package udpnat

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
	"github.com/metacubex/sing/common/pipe"
)

type connectionHandlerFunc func(context.Context, N.PacketConn, M.Socksaddr, M.Socksaddr, N.CloseHandlerFunc)

func (f connectionHandlerFunc) NewPacketConnectionEx(ctx context.Context, c N.PacketConn, s, d M.Socksaddr, close N.CloseHandlerFunc) {
	f(ctx, c, s, d, close)
}

type packetHandlerFunc func(*buf.Buffer, M.Socksaddr)

func (f packetHandlerFunc) NewPacketEx(b *buf.Buffer, d M.Socksaddr) { f(b, d) }
func await(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("operation blocked")
	}
}
func testConn() *natConn {
	return &natConn{writer: testPacketWriter{}, packetChan: make(chan *N.PacketBuffer, 64), doneChan: make(chan struct{}), readDeadline: pipe.MakeDeadline()}
}

func TestCloseReleasesQueueAndRejectsWrites(t *testing.T) {
	c := testConn()
	var queued []*buf.Buffer
	for i := 0; i < 70; i++ {
		b := buf.NewSize(1)
		b.WriteByte(1)
		queued = append(queued, b)
		c.deliver(b, M.Socksaddr{})
	}
	if len(c.packetChan) != 64 {
		t.Fatal("queue limit changed")
	}
	for _, b := range queued[64:] {
		if b.RawCap() != 0 {
			t.Fatal("overflow packet retained")
		}
	}
	c.Close()
	for _, b := range queued {
		if b.RawCap() != 0 {
			t.Fatal("queued packet retained")
		}
	}
	b := buf.NewSize(1)
	if err := c.WritePacket(b, M.Socksaddr{}); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal("closed write succeeded")
	}
	if b.RawCap() != 0 {
		t.Fatal("closed write retained payload")
	}
	b = buf.NewSize(1)
	if err := c.WritePacketBatch([]*buf.Buffer{b}, []M.Socksaddr{{}}); err == nil {
		t.Fatal("closed batch write succeeded")
	}
	if b.RawCap() != 0 {
		t.Fatal("closed batch retained payload")
	}
}
func TestCloseRacesWithEnqueue(t *testing.T) {
	c := testConn()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				c.deliver(buf.NewSize(1), M.Socksaddr{})
			}
		}()
	}
	c.Close()
	wg.Wait()
	if len(c.packetChan) != 0 {
		t.Fatal("packet queued after close")
	}
}
func TestCloseDuringPrepare(t *testing.T) {
	preparing, finish, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var callbacks, handlers atomic.Int32
	service := New(connectionHandlerFunc(func(context.Context, N.PacketConn, M.Socksaddr, M.Socksaddr, N.CloseHandlerFunc) { handlers.Add(1) }), func(M.Socksaddr, M.Socksaddr, any) (bool, context.Context, N.PacketWriter, N.CloseHandlerFunc) {
		close(preparing)
		<-finish
		return true, context.Background(), testPacketWriter{}, func(error) { callbacks.Add(1) }
	}, time.Minute, true)
	source := M.ParseSocksaddr("127.0.0.1:1234")
	go func() { service.NewPacket([][]byte{[]byte("first")}, source, M.Socksaddr{}, nil); close(returned) }()
	await(t, preparing)
	service.NewPacket([][]byte{[]byte("queued")}, source, M.Socksaddr{}, nil)
	service.Close()
	close(finish)
	await(t, returned)
	if callbacks.Load() != 1 || handlers.Load() != 0 || service.Len() != 0 {
		t.Fatalf("callbacks=%d handlers=%d retained=%d", callbacks.Load(), handlers.Load(), service.Len())
	}
}
func TestPrepareAndCloseCallbacksCanReenterService(t *testing.T) {
	var service *Service
	var calls atomic.Int32
	started := make(chan *natConn, 1)
	service = New(connectionHandlerFunc(func(_ context.Context, c N.PacketConn, _, _ M.Socksaddr, _ N.CloseHandlerFunc) {
		started <- c.(*natConn)
	}), func(M.Socksaddr, M.Socksaddr, any) (bool, context.Context, N.PacketWriter, N.CloseHandlerFunc) {
		_ = service.Len()
		return true, context.Background(), testPacketWriter{}, func(error) { calls.Add(1); service.Purge(); _ = service.Len() }
	}, time.Minute, false)
	done := make(chan struct{})
	go func() { service.NewPacket(nil, M.ParseSocksaddr("127.0.0.1:4321"), M.Socksaddr{}, nil); close(done) }()
	await(t, done)
	var c *natConn
	select {
	case c = <-started:
	case <-time.After(time.Second):
		t.Fatal("handler not started")
	}
	if c.Timeout() <= 0 || !c.SetTimeout(time.Minute) {
		t.Fatal("timeout unavailable")
	}
	done = make(chan struct{})
	go func() { service.Purge(); close(done) }()
	await(t, done)
	if !c.isClosed() || calls.Load() != 1 {
		t.Fatal("eviction did not close exactly once")
	}
	service.Close()
}
func TestHandlerReentrantCloseAndHeadroom(t *testing.T) {
	c := testConn()
	buffer := buf.NewSize(20000)
	buffer.Extend(20000)
	c.deliver(buffer, M.Socksaddr{})
	called := make(chan struct{})
	done := make(chan struct{})
	go func() {
		c.SetHandler(packetHandlerFunc(func(b *buf.Buffer, _ M.Socksaddr) {
			if b.Len() != 20000 {
				t.Errorf("payload truncated to %d", b.Len())
			}
			b.Release()
			c.Close()
			close(called)
		}))
		close(done)
	}()
	await(t, called)
	await(t, done)
	if len(c.packetChan) != 0 {
		t.Fatal("queue retained")
	}
}
func TestBatchOwnershipAfterClose(t *testing.T) {
	c := testConn()
	c.InitializeReadWaiter(N.ReadWaitOptions{BatchSize: 2, FrontHeadroom: 32})
	c.deliver(buf.As([]byte("one")).ToOwned(), M.Socksaddr{})
	c.deliver(buf.As([]byte("two")).ToOwned(), M.Socksaddr{})
	buffers, _, err := c.WaitReadPackets()
	if err != nil || len(buffers) != 2 {
		t.Fatal(err)
	}
	c.Close()
	c.ReleaseReadWaiter()
	for _, b := range buffers {
		if b.Len() != 3 || b.Start() < 32 {
			t.Fatal("caller buffer changed")
		}
		b.Release()
	}
}
