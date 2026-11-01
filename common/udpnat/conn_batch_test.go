package udpnat

import (
	"io"
	"testing"

	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

func TestConnBatchReadOwnershipAndHeadroom(t *testing.T) {
	c := testConn(&testPacketWriter{})
	defer c.Close()
	for _, value := range []string{"first", "", "third"} {
		buffer := buf.NewSize(len(value))
		buffer.WriteString(value)
		c.enqueue(testQueuedPacket(buffer))
	}
	c.InitializeReadWaiter(N.ReadWaitOptions{BatchSize: 2, FrontHeadroom: 8, RearHeadroom: 16})
	buffers, destinations, err := c.WaitReadPackets()
	if err != nil || len(buffers) != 2 || len(destinations) != 2 {
		t.Fatalf("batch len=%d err=%v", len(buffers), err)
	}
	first, empty := buffers[0], buffers[1]
	if string(first.Bytes()) != "first" || !empty.IsEmpty() || first.Start() != 8 || first.FreeLen() < 16 {
		t.Fatal("batch changed packet contents or headroom")
	}
	c.ReleaseReadWaiter()
	if string(first.Bytes()) != "first" {
		t.Fatal("release reclaimed caller-owned payload")
	}
	first.Release()
	empty.Release()
}

func TestConnCachedBatchWriterRejectsAfterClose(t *testing.T) {
	c := testConn(&testPacketWriter{})
	writer, ok := c.CreatePacketBatchWriter()
	if !ok {
		t.Fatal("batch writer unavailable")
	}
	c.Close()
	buffer := testBuffer(128)
	if err := writer.WritePacketBatch([]*buf.Buffer{buffer}, []M.Socksaddr{{}}); err != io.ErrClosedPipe {
		t.Fatalf("closed batch write=%v", err)
	}
	if buffer.RawCap() != 0 {
		t.Fatal("rejected batch retained payload")
	}
}

func TestConnCountedWriterRejectsAfterClose(t *testing.T) {
	c := testConn(&testPacketWriter{})
	writer, ok := c.CreatePacketBatchWriterWithCount()
	if !ok {
		t.Fatal("count writer unavailable")
	}
	c.Close()
	buffer := testBuffer(128)
	n, err := writer.WritePacketBatchCount([]*buf.Buffer{buffer}, []M.Socksaddr{{}})
	if n != 0 || err != io.ErrClosedPipe || buffer.RawCap() != 0 {
		t.Fatalf("n=%d err=%v cap=%d", n, err, buffer.RawCap())
	}
}
