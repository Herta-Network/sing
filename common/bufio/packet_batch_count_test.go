package bufio

import (
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

type partialCountPacketConn struct{ testNetPacketConn }

func (*partialCountPacketConn) WritePacket(*buf.Buffer, M.Socksaddr) error {
	panic("scalar path bypassed count capability")
}
func (c *partialCountPacketConn) WritePacketBatch(buffers []*buf.Buffer, destinations []M.Socksaddr) error {
	_, err := c.WritePacketBatchCount(buffers, destinations)
	return err
}
func (*partialCountPacketConn) WritePacketBatchCount(buffers []*buf.Buffer, _ []M.Socksaddr) (int, error) {
	defer buf.ReleaseMulti(buffers)
	return 2, io.ErrClosedPipe
}

func TestPacketBatchCountsSuccessfulPrefixIncludingEmpty(t *testing.T) {
	var bytes, packets atomic.Int64
	conn := NewInt64CounterPacketConn(&partialCountPacketConn{}, nil, nil, []*atomic.Int64{&bytes}, []*atomic.Int64{&packets})
	writer, ok := CreatePacketBatchWriterWithCount(conn)
	if !ok {
		t.Fatal("count capability lost")
	}
	buffers := testBuffers("", "bb", "ccc")
	n, err := writer.WritePacketBatchCount(buffers, make([]M.Socksaddr, 3))
	if n != 2 || !errors.Is(err, io.ErrClosedPipe) || bytes.Load() != 2 || packets.Load() != 2 {
		t.Fatalf("n=%d err=%v bytes=%d packets=%d", n, err, bytes.Load(), packets.Load())
	}
	for _, b := range buffers {
		if b.RawCap() != 0 {
			t.Fatal("buffer leaked")
		}
	}
}
func TestCopyPacketCountsPrefixOnBatchError(t *testing.T) {
	reader := &testPacketBatchReader{batches: []testPacketBatch{{payloads: [][]byte{nil, []byte("bb"), []byte("ccc")}, destinations: make([]M.Socksaddr, 3)}}}
	var bytes, packets atomic.Int64
	writer := NewInt64CounterPacketConn(&partialCountPacketConn{}, nil, nil, []*atomic.Int64{&bytes}, []*atomic.Int64{&packets})
	n, err := CopyPacket(writer, reader)
	if n != 2 || !errors.Is(err, io.ErrClosedPipe) || bytes.Load() != 2 || packets.Load() != 2 {
		t.Fatalf("n=%d err=%v bytes=%d packets=%d", n, err, bytes.Load(), packets.Load())
	}
}

type opaqueLegacyBatch struct {
	calls      int
	batchCalls int
}

func (w *opaqueLegacyBatch) WritePacket(buffer *buf.Buffer, _ M.Socksaddr) error {
	defer buffer.Release()
	w.calls++
	if w.calls == 3 {
		return io.ErrClosedPipe
	}
	return nil
}
func (w *opaqueLegacyBatch) WritePacketBatch(buffers []*buf.Buffer, _ []M.Socksaddr) error {
	w.batchCalls++
	buf.ReleaseMulti(buffers)
	return io.ErrClosedPipe
}
func TestCountedBatchFallbackPreservesOpaqueWriter(t *testing.T) {
	writer := &opaqueLegacyBatch{}
	buffers := testBuffers("a", "", "ccc", "tail")
	n, err := NewPacketBatchWriterWithCount(writer).WritePacketBatchCount(buffers, make([]M.Socksaddr, len(buffers)))
	if n != 2 || !errors.Is(err, io.ErrClosedPipe) || writer.calls != 3 || writer.batchCalls != 0 {
		t.Fatalf("n=%d err=%v writer=%+v", n, err, writer)
	}
	for _, b := range buffers {
		if b.RawCap() != 0 {
			t.Fatal("fallback buffer leaked")
		}
	}
}

type shortCountWriter struct{}

func (shortCountWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }
func TestVectorisedCountPreservesShortWriteProgress(t *testing.T) {
	buffers := testBuffers("abc", "def")
	n, err := NewVectorisedWriterWithCount(shortCountWriter{}).WriteVectorisedCount(buffers)
	if n != 5 || !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("n=%d err=%v", n, err)
	}
	for _, b := range buffers {
		if b.RawCap() != 0 {
			t.Fatal("vector buffer leaked")
		}
	}
}

var _ N.PacketBatchWriterWithCount = (*partialCountPacketConn)(nil)

type mappedCountPacketConn struct {
	partialCountPacketConn
	destinations []M.Socksaddr
}

func (c *mappedCountPacketConn) WritePacketBatchCount(buffers []*buf.Buffer, destinations []M.Socksaddr) (int, error) {
	c.destinations = append([]M.Socksaddr(nil), destinations...)
	return c.partialCountPacketConn.WritePacketBatchCount(buffers, destinations)
}
func TestCountedBatchPreservesNATMapping(t *testing.T) {
	origin := M.ParseSocksaddr("127.0.0.1:53")
	target := M.ParseSocksaddr("127.0.0.2:53")
	other := M.ParseSocksaddr("127.0.0.3:54")
	for name, wrap := range map[string]func(N.NetPacketConn, M.Socksaddr, M.Socksaddr) NATPacketConn{"unidirectional": NewUnidirectionalNATPacketConn, "bidirectional": NewNATPacketConn, "destination": NewDestinationNATPacketConn} {
		t.Run(name, func(t *testing.T) {
			raw := &mappedCountPacketConn{}
			writer, ok := CreatePacketBatchWriterWithCount(wrap(raw, origin, target))
			if !ok {
				t.Fatal("NAT count capability lost")
			}
			n, err := writer.WritePacketBatchCount(testBuffers("a", "", "tail"), []M.Socksaddr{target, other, target})
			if n != 2 || !errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("n=%d err=%v", n, err)
			}
			if raw.destinations[0] != origin || raw.destinations[1] != other || raw.destinations[2] != origin {
				t.Fatalf("mapping=%v", raw.destinations)
			}
		})
	}
}

type legacyCounterSource struct {
	N.PacketConn
	batches int
}

func (c *legacyCounterSource) WritePacket(buffer *buf.Buffer, _ M.Socksaddr) error {
	buffer.Release()
	return nil
}
func (c *legacyCounterSource) WritePacketBatch(buffers []*buf.Buffer, _ []M.Socksaddr) error {
	c.batches++
	buf.ReleaseMulti(buffers)
	return nil
}
func TestLegacyCounterBatchRemainsCompatible(t *testing.T) {
	raw := &legacyCounterSource{}
	var size int64
	counter := NewCounterPacketConn(raw, nil, []N.CountFunc{func(n int64) { size += n }})
	writer, ok := CreatePacketBatchWriter(counter)
	if !ok {
		t.Fatal("legacy batch capability lost")
	}
	if err := writer.WritePacketBatch(testBuffers("a", "bb"), make([]M.Socksaddr, 2)); err != nil {
		t.Fatal(err)
	}
	if raw.batches != 1 || size != 3 {
		t.Fatalf("batch=%d bytes=%d", raw.batches, size)
	}
	if _, ok := CreatePacketBatchWriterWithCount(counter); ok {
		t.Fatal("invented progress capability for opaque batch writer")
	}
}

func TestCopyPacketFallsBackForOpaqueBatchProgress(t *testing.T) {
	reader := &testPacketBatchReader{batches: []testPacketBatch{{payloads: [][]byte{[]byte("a"), nil, []byte("ccc")}, destinations: make([]M.Socksaddr, 3)}}}
	writer := &opaqueLegacyBatch{}
	n, err := CopyPacket(writer, reader)
	if n != 1 || !errors.Is(err, io.ErrClosedPipe) || writer.calls != 3 || writer.batchCalls != 0 {
		t.Fatalf("n=%d err=%v writer=%+v", n, err, writer)
	}
}
