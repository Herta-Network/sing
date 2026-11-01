package udpnat

import (
	"bytes"
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/sing/common"
	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/cache"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
	"github.com/metacubex/sing/common/pipe"
)

type testHandler struct {
	handle func(context.Context, N.PacketConn, M.Metadata) error
}

func (h testHandler) NewPacketConnection(ctx context.Context, conn N.PacketConn, metadata M.Metadata) error {
	return h.handle(ctx, conn, metadata)
}

func (testHandler) NewError(context.Context, error) {}

type testPacketWriter struct {
	closes atomic.Int32
	write  func(*buf.Buffer, M.Socksaddr) error
	close  func() error
}

func (w *testPacketWriter) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	if w.write != nil {
		return w.write(buffer, destination)
	}
	buffer.Release()
	return nil
}

func (w *testPacketWriter) Close() error {
	w.closes.Add(1)
	if w.close != nil {
		return w.close()
	}
	return nil
}

func testConn(writer N.PacketWriter) *conn {
	ctx, cancel := common.ContextWithCancelCause(context.Background())
	return &conn{ctx: ctx, cancel: cancel, data: make(chan *N.PacketBuffer, 64), source: writer, readDeadline: pipe.MakeDeadline()}
}

func testBuffer(size int) *buf.Buffer {
	buffer := buf.NewSize(size)
	buffer.Extend(size)
	return buffer
}

func testQueuedPacket(buffer *buf.Buffer) *N.PacketBuffer {
	packet := N.NewPacketBuffer()
	packet.Buffer = buffer
	return packet
}

func TestServiceFullQueueDoesNotBlock(t *testing.T) {
	entered := make(chan N.PacketConn, 1)
	stop := make(chan struct{})
	handlerDone := make(chan struct{})
	s := New[int](60, testHandler{handle: func(_ context.Context, conn N.PacketConn, _ M.Metadata) error {
		defer close(handlerDone)
		entered <- conn
		<-stop
		return nil
	}})
	writer := &testPacketWriter{}
	init := func(N.PacketConn) N.PacketWriter { return writer }
	for i := 0; i < 64; i++ {
		s.NewPacket(context.Background(), 1, testBuffer(128), M.Metadata{}, init)
	}
	c := (<-entered).(*conn)
	defer func() {
		c.Close()
		close(stop)
		<-handlerDone
		// Also clean up payloads when running against the unfixed implementation.
		for len(c.data) > 0 {
			releasePacket(<-c.data)
		}
	}()

	buffer := testBuffer(128)
	returned := make(chan struct{})
	go func() {
		s.NewPacket(context.Background(), 1, buffer, M.Metadata{}, init)
		close(returned)
	}()
	select {
	case <-returned:
		if buffer.RawCap() != 0 {
			t.Error("packet dropped from a full queue was not released")
		}
	case <-time.After(time.Second):
		// Unblock the old producer so this regression does not itself leak it.
		releasePacket(<-c.data)
		<-returned
		t.Error("full UDP session queue blocked the producer")
	}
}

func TestConnCloseReleasesQueuedPackets(t *testing.T) {
	writer := &testPacketWriter{}
	c := testConn(writer)
	buffers := []*buf.Buffer{testBuffer(128), testBuffer(1024)}
	for _, buffer := range buffers {
		c.data <- testQueuedPacket(buffer)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c.Close()
	if got := writer.closes.Load(); got != 1 {
		t.Errorf("source closed %d times, want 1", got)
	}
	for _, buffer := range buffers {
		if buffer.RawCap() != 0 {
			t.Error("closed connection retained queued payload")
			buffer.Release()
		}
	}
}

func TestServiceCanceledPacketIsReleased(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var initialized atomic.Int32
	s := New[int](60, testHandler{handle: func(context.Context, N.PacketConn, M.Metadata) error { return nil }})
	buffer := testBuffer(128)
	s.NewPacket(ctx, 1, buffer, M.Metadata{}, func(N.PacketConn) N.PacketWriter {
		initialized.Add(1)
		return &testPacketWriter{}
	})
	if initialized.Load() != 0 {
		t.Error("canceled packet initialized a new session")
	}
	if buffer.RawCap() != 0 {
		t.Error("canceled packet was not released")
		buffer.Release()
	}
}

func TestConnWriteAfterCloseReleasesPacket(t *testing.T) {
	c := testConn(&testPacketWriter{})
	c.Close()
	buffer := testBuffer(128)
	if err := c.WritePacket(buffer, M.Socksaddr{}); err != io.ErrClosedPipe {
		t.Errorf("WritePacket after Close = %v, want closed pipe", err)
	}
	if buffer.RawCap() != 0 {
		t.Error("rejected write retained payload")
		buffer.Release()
	}
}

func TestWaitReadPacketPreservesLargePayloadWithHeadroom(t *testing.T) {
	c := testConn(&testPacketWriter{})
	defer c.Close()
	payload := bytes.Repeat([]byte{42}, 20*1024)
	input := testBuffer(len(payload))
	copy(input.Bytes(), payload)
	defer input.Release()
	c.data <- testQueuedPacket(input)
	c.InitializeReadWaiter(N.ReadWaitOptions{FrontHeadroom: 8, RearHeadroom: 16})
	output, _, err := c.WaitReadPacket()
	if err != nil {
		t.Fatal(err)
	}
	defer output.Release()
	if !bytes.Equal(output.Bytes(), payload) || output.Start() < 8 || output.FreeLen() < 16 {
		t.Fatal("read waiter lost payload or required headroom")
	}
	if input.RawCap() != 0 {
		t.Fatal("copied input buffer was not released")
	}
}

func TestOldHandlerDoesNotRemoveReplacement(t *testing.T) {
	oldWriter, newWriter := &testPacketWriter{}, &testPacketWriter{}
	oldConn, newConn := testConn(oldWriter), testConn(newWriter)
	entered, finish := make(chan struct{}), make(chan struct{})
	s := New[int](60, testHandler{handle: func(context.Context, N.PacketConn, M.Metadata) error {
		close(entered)
		<-finish
		return nil
	}})
	defer s.Close()
	s.nat.shard(1).Store(1, oldConn)
	done := make(chan struct{})
	go func() {
		s.handleConnection(context.Background(), 1, oldConn, M.Metadata{})
		close(done)
	}()
	<-entered
	oldConn.Close()
	s.nat.shard(1).Store(1, newConn)
	close(finish)
	<-done
	current, found := s.nat.shard(1).Load(1)
	if !found || current != newConn || newWriter.closes.Load() != 0 {
		t.Fatal("old handler cleanup removed or closed its replacement")
	}
	if oldWriter.closes.Load() != 1 {
		t.Fatal("old session source was not closed exactly once")
	}
}

func TestServiceCloseDuringInitialization(t *testing.T) {
	var handled atomic.Int32
	s := New[int](60, testHandler{handle: func(context.Context, N.PacketConn, M.Metadata) error {
		handled.Add(1)
		return nil
	}})
	entered := make(chan N.PacketConn, 1)
	finish, returned := make(chan struct{}), make(chan struct{})
	buffer := testBuffer(128)
	writer := &testPacketWriter{}
	go func() {
		s.NewPacket(context.Background(), 1, buffer, M.Metadata{}, func(c N.PacketConn) N.PacketWriter {
			// Reentering Close must also be safe when source installation loses
			// to service shutdown.
			writer.close = c.Close
			entered <- c
			<-finish
			return writer
		})
		close(returned)
	}()
	<-entered
	s.Close()
	close(finish)
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("source initialization did not finish after service close")
	}
	if handled.Load() != 0 || writer.closes.Load() != 1 || buffer.RawCap() != 0 || s.nat.shard(1).Exist(1) {
		t.Fatalf("shutdown retained resources: handled=%d source_closes=%d buffer_cap=%d cached=%v",
			handled.Load(), writer.closes.Load(), buffer.RawCap(), s.nat.shard(1).Exist(1))
	}
	later := testBuffer(128)
	s.NewPacket(context.Background(), 2, later, M.Metadata{}, func(N.PacketConn) N.PacketWriter {
		t.Error("closed service initialized another source")
		return writer
	})
	if later.RawCap() != 0 {
		t.Fatal("closed service did not release a later packet")
	}
}

func TestConnCloseInterruptsBlockedWrite(t *testing.T) {
	entered, interrupted := make(chan struct{}), make(chan struct{})
	var interrupt sync.Once
	writer := &testPacketWriter{
		write: func(buffer *buf.Buffer, _ M.Socksaddr) error {
			defer buffer.Release()
			close(entered)
			<-interrupted
			return io.ErrClosedPipe
		},
		close: func() error { interrupt.Do(func() { close(interrupted) }); return nil },
	}
	c := testConn(writer)
	written, closed := make(chan struct{}), make(chan struct{})
	buffer := testBuffer(128)
	go func() { c.WritePacket(buffer, M.Socksaddr{}); close(written) }()
	<-entered
	go func() { c.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		interrupt.Do(func() { close(interrupted) })
		<-closed
		t.Error("Close waited for the write it needed to interrupt")
	}
	<-written
	if buffer.RawCap() != 0 || writer.closes.Load() != 1 {
		t.Fatal("blocked write cleanup retained data or repeated source close")
	}
}

func TestConnConcurrentEnqueueAndClose(t *testing.T) {
	writer := &testPacketWriter{}
	c := testConn(writer)
	const count = 256
	buffers := make([]*buf.Buffer, count)
	start := make(chan struct{})
	var group sync.WaitGroup
	for index := range buffers {
		buffer := testBuffer(128)
		buffers[index] = buffer
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			if !c.enqueue(testQueuedPacket(buffer)) {
				buffer.Release()
			}
		}()
	}
	group.Add(1)
	go func() { defer group.Done(); <-start; c.Close() }()
	close(start)
	group.Wait()
	c.Close()
	if len(c.data) != 0 || writer.closes.Load() != 1 {
		t.Fatal("concurrent close did not settle the queue and source")
	}
	for _, buffer := range buffers {
		if buffer.RawCap() != 0 {
			t.Fatal("enqueue raced past close and retained a packet")
		}
	}
}

func TestServiceEvictionCloseCanReenter(t *testing.T) {
	s := New[int](60, testHandler{handle: func(context.Context, N.PacketConn, M.Metadata) error { return nil }})
	writer := &testPacketWriter{}
	c := testConn(writer)
	writer.close = func() error {
		c.Close()
		s.Close()
		return nil
	}
	s.nat.shard(1).Store(1, c)
	done := make(chan struct{})
	go func() { s.nat.shard(1).Delete(1); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("source close deadlocked while reentering the connection or service")
	}
	if writer.closes.Load() != 1 {
		t.Fatal("reentrant shutdown repeated source close")
	}
}

func TestCanceledPacketLookupPreservesExistingSession(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer := &testPacketWriter{}
	c := testConn(writer)
	defer c.Close()
	s := New[int](60, testHandler{handle: func(context.Context, N.PacketConn, M.Metadata) error { return nil }})
	// Cancel exactly during lookup, after admission's initial context check.
	s.nat.shards[s.nat.hasher.Hash32(1)&s.nat.mask] = cache.New(cache.WithHealthCheck[int, *conn](func(int, *conn) bool {
		cancel()
		return true
	}))
	s.nat.shard(1).Store(1, c)
	buffer := testBuffer(128)
	s.NewPacket(ctx, 1, buffer, M.Metadata{}, func(N.PacketConn) N.PacketWriter {
		t.Error("existing session was unexpectedly replaced")
		return writer
	})
	if buffer.RawCap() != 0 {
		t.Fatal("canceled packet retained its buffer")
	}
	if common.Done(c.ctx) || writer.closes.Load() != 0 || !s.nat.shard(1).Exist(1) {
		t.Fatal("one canceled packet destroyed the existing shared session")
	}
}

func TestServiceDeliversFirstPacketOnce(t *testing.T) {
	result := make(chan error, 1)
	var handled atomic.Int32
	want := bytes.Repeat([]byte{42}, 128)
	s := New[int](60, testHandler{handle: func(_ context.Context, c N.PacketConn, _ M.Metadata) error {
		handled.Add(1)
		output := buf.New()
		defer output.Release()
		_, err := c.ReadPacket(output)
		if err == nil && !bytes.Equal(output.Bytes(), want) {
			err = io.ErrUnexpectedEOF
		}
		result <- err
		return nil
	}})
	defer s.Close()
	input := testBuffer(len(want))
	copy(input.Bytes(), want)
	s.NewPacket(context.Background(), 1, input, M.Metadata{}, func(N.PacketConn) N.PacketWriter { return &testPacketWriter{} })
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("first packet was not delivered")
	}
	if handled.Load() != 1 || input.RawCap() != 0 {
		t.Fatal("first packet was retried or retained after delivery")
	}
}
