package udpnat

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/sing/common/cache"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

func TestSessionCacheBoundsAndReleasesEvicted(t *testing.T) {
	for _, limit := range []int{1, 3, 7, 64} {
		cache := newSessionCache[int](60, limit)
		var connections []*conn
		var writers []*testPacketWriter
		for key := 0; key < 128; key++ {
			writer := &testPacketWriter{}
			connection := testConn(writer)
			connection.enqueue(testQueuedPacket(testBuffer(128)))
			connections = append(connections, connection)
			writers = append(writers, writer)
			cache.LoadOrStore(key, func() *conn { return connection })
			if count := cache.Len(); count > limit {
				t.Fatalf("limit=%d retained=%d", limit, count)
			}
		}
		cache.Clear()
		if cache.Len() != 0 {
			t.Fatal("cache retained sessions after Clear")
		}
		for index, connection := range connections {
			if len(connection.data) != 0 || writers[index].closes.Load() != 1 {
				t.Fatalf("limit=%d session=%d retained packets or repeated close", limit, index)
			}
		}
	}
}

func TestSessionCacheDeleteUsesConnectionIdentity(t *testing.T) {
	cache := newSessionCache[int](60, 16)
	oldConn, replacement := testConn(&testPacketWriter{}), testConn(&testPacketWriter{})
	cache.LoadOrStore(1, func() *conn { return oldConn })
	if !cache.Delete(1, oldConn) {
		t.Fatal("old owner could not remove itself")
	}
	cache.LoadOrStore(1, func() *conn { return replacement })
	if cache.Delete(1, oldConn) {
		t.Fatal("stale owner removed its replacement")
	}
	current, loaded := cache.LoadOrStore(1, func() *conn { return oldConn })
	if !loaded || current != replacement {
		t.Fatal("replacement was lost")
	}
	cache.Clear()
}

func TestServiceSessionLimitAndShutdown(t *testing.T) {
	var handlers sync.WaitGroup
	s := NewWithOptions[int](60, testHandler{handle: func(_ context.Context, c N.PacketConn, _ M.Metadata) error {
		defer handlers.Done()
		for {
			packet, _, err := c.(N.PacketReadWaiter).WaitReadPacket()
			if err != nil {
				return nil
			}
			packet.Release()
		}
	}}, ServiceOptions{MaxSessions: 3})
	for key := 0; key < 64; key++ {
		s.NewPacket(context.Background(), key, testBuffer(128), M.Metadata{}, func(N.PacketConn) N.PacketWriter {
			handlers.Add(1)
			return &testPacketWriter{}
		})
		if s.Len() > 3 {
			t.Fatal("service exceeded its session budget")
		}
	}
	s.Close()
	done := make(chan struct{})
	go func() { handlers.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("evicted handlers did not exit")
	}
	if s.Len() != 0 {
		t.Fatal("service retained sessions after Close")
	}
}

func TestServiceDeliversEmptyDatagram(t *testing.T) {
	result := make(chan error, 1)
	s := New[int](60, testHandler{handle: func(_ context.Context, c N.PacketConn, _ M.Metadata) error {
		buffer := testBuffer(128)
		buffer.Reset()
		defer buffer.Release()
		_, err := c.ReadPacket(buffer)
		result <- err
		return nil
	}})
	defer s.Close()
	s.NewPacket(context.Background(), 1, testBuffer(0), M.Metadata{}, func(N.PacketConn) N.PacketWriter { return &testPacketWriter{} })
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("empty datagram became a stream error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("empty datagram was not delivered")
	}
}

func BenchmarkSessionCacheParallel(b *testing.B) {
	const keyCount = 4096
	shared := testConn(&testPacketWriter{})
	defer shared.Close()
	constructor := func() *conn { return shared }
	global := cache.New(cache.WithAge[int, *conn](60), cache.WithSize[int, *conn](defaultMaxSessions), cache.WithUpdateAgeOnGet[int, *conn]())
	sharded := newSessionCache[int](60, defaultMaxSessions)
	for key := 0; key < keyCount; key++ {
		global.LoadOrStore(key, constructor)
		sharded.LoadOrStore(key, constructor)
	}
	run := func(b *testing.B, load func(int) (*conn, bool)) {
		b.ReportAllocs()
		var seed atomic.Uint32
		b.RunParallel(func(pb *testing.PB) {
			key := seed.Add(0x9e3779b9)
			for pb.Next() {
				key ^= key << 13
				key ^= key >> 17
				key ^= key << 5
				load(int(key & (keyCount - 1)))
			}
		})
	}
	b.Run("global", func(b *testing.B) {
		run(b, func(key int) (*conn, bool) { return global.LoadOrStore(key, constructor) })
	})
	b.Run("sharded", func(b *testing.B) {
		run(b, func(key int) (*conn, bool) { return sharded.LoadOrStore(key, constructor) })
	})
}
