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

type emptyHandshakePacketReader struct{ reads, failures int }

func (r *emptyHandshakePacketReader) ReadPacket(buffer *buf.Buffer) (M.Socksaddr, error) {
	r.reads++
	if r.reads == 1 {
		return M.Socksaddr{}, nil
	}
	if r.reads == 2 {
		buffer.WriteString("tail")
		return M.Socksaddr{}, nil
	}
	return M.Socksaddr{}, io.EOF
}
func (r *emptyHandshakePacketReader) HandshakeFailure(error) error { r.failures++; return nil }

type earlyBatchPacketWriter struct {
	target *testFailAfterPacketWriter
	ready  bool
}

func (w *earlyBatchPacketWriter) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	w.ready = true
	return w.target.WritePacket(buffer, destination)
}
func (w *earlyBatchPacketWriter) NeedHandshake() bool     { return !w.ready }
func (w *earlyBatchPacketWriter) WriterReplaceable() bool { return w.ready }
func (w *earlyBatchPacketWriter) Upstream() any           { return w.target }

func TestEmptyDatagramCompletesPacketHandshake(t *testing.T) {
	reader := &emptyHandshakePacketReader{}
	failure := errors.New("second packet failed")
	writer := &earlyBatchPacketWriter{target: &testFailAfterPacketWriter{failAt: 2, err: failure}}
	n, err := CopyPacket(writer, reader)
	if n != 0 || !errors.Is(err, failure) || reader.failures != 0 {
		t.Fatalf("empty packet handshake: n=%d error=%v failures=%d", n, err, reader.failures)
	}
}

func TestWritePacketWithPoolReleasesUnsentRecords(t *testing.T) {
	failure := errors.New("packet rejected")
	writer := &testFailAfterPacketWriter{failAt: 2, err: failure}
	var records []*N.PacketBuffer
	var buffers []*buf.Buffer
	for _, data := range []string{"a", "bb", "ccc"} {
		record := testPacketBuffer(data, M.Socksaddr{})
		records = append(records, record)
		buffers = append(buffers, record.Buffer)
	}
	n, err := WritePacketWithPool(&emptyHandshakePacketReader{}, writer, records, nil, nil)
	if n != 1 || !errors.Is(err, failure) {
		t.Fatalf("copy n=%d error=%v", n, err)
	}
	for _, buffer := range buffers {
		if buffer.RawCap() != 0 {
			t.Fatal("failed batch retained an unsent buffer")
		}
	}
}

type emptyCounterPacketConn struct{ testNetPacketConn }

func (*emptyCounterPacketConn) ReadPacket(*buf.Buffer) (M.Socksaddr, error) {
	return M.Socksaddr{}, nil
}
func (*emptyCounterPacketConn) WritePacket(buffer *buf.Buffer, _ M.Socksaddr) error {
	buffer.Release()
	return nil
}

func TestCounterPacketConnCountsEmptyDatagrams(t *testing.T) {
	var bytes, packets, readBytes, readPackets atomic.Int64
	conn := NewInt64CounterPacketConn(&emptyCounterPacketConn{}, []*atomic.Int64{&readBytes}, []*atomic.Int64{&readPackets}, []*atomic.Int64{&bytes}, []*atomic.Int64{&packets})
	if err := conn.WritePacket(buf.NewSize(0), M.Socksaddr{}); err != nil {
		t.Fatal(err)
	}
	if bytes.Load() != 0 || packets.Load() != 1 {
		t.Fatalf("empty datagram counters: bytes=%d packets=%d", bytes.Load(), packets.Load())
	}
	buffer := buf.New()
	defer buffer.Release()
	if _, err := conn.ReadPacket(buffer); err != nil {
		t.Fatal(err)
	}
	if readBytes.Load() != 0 || readPackets.Load() != 1 {
		t.Fatal("empty read datagram was not counted")
	}
}
