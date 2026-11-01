package bufio

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

type ownedCacheSource struct {
	testNetPacketConn
	packets          []*buf.Buffer
	offset, released int
	copy             bool
	started          chan struct{}
	closed           chan struct{}
	once             sync.Once
	late             *buf.Buffer
}

func (s *ownedCacheSource) InitializeReadWaiter(N.ReadWaitOptions) bool { return s.copy }
func (s *ownedCacheSource) ReadPacket(*buf.Buffer) (M.Socksaddr, error) {
	panic("cache hid the owned reader")
}
func (s *ownedCacheSource) WaitReadPacket() (*buf.Buffer, M.Socksaddr, error) {
	if s.offset < len(s.packets) {
		buffer := s.packets[s.offset]
		s.offset++
		return buffer, M.ParseSocksaddr("127.0.0.2:53"), nil
	}
	close(s.started)
	<-s.closed
	return s.late, M.Socksaddr{}, net.ErrClosed
}
func (s *ownedCacheSource) Close() error       { s.once.Do(func() { close(s.closed) }); return nil }
func (s *ownedCacheSource) ReleaseReadWaiter() { s.released++; buf.ReleaseMulti(s.packets[s.offset:]) }

func cachedTestBuffer(value string, front int) *buf.Buffer {
	buffer := buf.NewSize(front + len(value) + 16)
	buffer.Resize(front, 0)
	buffer.WriteString(value)
	return buffer
}

func TestCachedPacketOwnedWaiterPreservesMixedReadOwnership(t *testing.T) {
	first := cachedTestBuffer("", 32)
	source := &ownedCacheSource{closed: make(chan struct{}), packets: []*buf.Buffer{
		cachedTestBuffer("second", 32), cachedTestBuffer("third", 32), cachedTestBuffer("pending", 32),
	}}
	conn := NewCachedPacketConn(source, first, M.ParseSocksaddr("127.0.0.1:53"))
	waiter, ok := CreatePacketReadWaiter(conn)
	if !ok || waiter.InitializeReadWaiter(N.ReadWaitOptions{FrontHeadroom: 16, RearHeadroom: 8}) {
		t.Fatal("cache did not expose an owned waiter")
	}
	buffer, sourceAddr, err := waiter.WaitReadPacket()
	if err != nil || buffer != first || !buffer.IsEmpty() || sourceAddr.Port != 53 || source.offset != 0 {
		t.Fatal("empty cached packet skipped or copied")
	}
	if conn.ReaderReplaceable() {
		t.Fatal("cache waiter can be bypassed")
	}
	scalar := buf.NewSize(64)
	defer scalar.Release()
	if _, err := conn.ReadPacket(scalar); err != nil || string(scalar.Bytes()) != "second" {
		t.Fatal("mixed scalar read bypassed waiter")
	}
	third, _, err := waiter.WaitReadPacket()
	if err != nil || third != source.packets[1] {
		t.Fatal("underlying owned packet copied")
	}
	conn.Close()
	waiter.(N.ReadWaiterReleaser).ReleaseReadWaiter()
	if first.RawCap() == 0 || third.RawCap() == 0 || source.packets[2].RawCap() != 0 || source.released != 1 {
		t.Fatal("close released caller storage or retained pending storage")
	}
	first.Release()
	third.Release()
}

func TestCachedPacketOwnedWaiterCopiesRequiredHeadroom(t *testing.T) {
	first := cachedTestBuffer("cached", 0)
	second := cachedTestBuffer("next", 0)
	source := &ownedCacheSource{copy: true, closed: make(chan struct{}), packets: []*buf.Buffer{second}}
	conn := NewCachedPacketConn(source, first, M.ParseSocksaddr("127.0.0.1:53"))
	defer conn.Close()
	waiter, _ := CreatePacketReadWaiter(conn)
	waiter.InitializeReadWaiter(N.ReadWaitOptions{FrontHeadroom: 32, RearHeadroom: 8})
	for _, original := range []*buf.Buffer{first, second} {
		buffer, _, err := waiter.WaitReadPacket()
		if err != nil {
			t.Fatal(err)
		}
		if buffer == original || original.RawCap() != 0 || buffer.Start() < 32 || buffer.FreeLen() < 8 {
			t.Fatal("required copy did not consume original storage")
		}
		buffer.Release()
	}
}

func TestCachedPacketOwnedWaiterCloseInterruptsRead(t *testing.T) {
	first := cachedTestBuffer("first", 0)
	source := &ownedCacheSource{closed: make(chan struct{}), started: make(chan struct{}), late: buf.NewSize(64)}
	conn := NewCachedPacketConn(source, first, M.Socksaddr{})
	waiter, _ := CreatePacketReadWaiter(conn)
	waiter.InitializeReadWaiter(N.ReadWaitOptions{})
	buffer, _, err := waiter.WaitReadPacket()
	if err != nil {
		t.Fatal(err)
	}
	buffer.Release()
	done := make(chan error, 1)
	go func() {
		b, _, err := waiter.WaitReadPacket()
		if b != nil {
			b.Release()
			t.Error("failed read transferred storage")
		}
		done <- err
	}()
	<-source.started
	closed := make(chan struct{})
	go func() { conn.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("close waited before interrupting read")
	}
	if err := <-done; !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
	if source.late.RawCap() != 0 || source.released != 1 {
		t.Fatal("late owned buffer or waiter retained")
	}
}
