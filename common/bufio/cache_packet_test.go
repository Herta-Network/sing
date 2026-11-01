package bufio

import (
	"testing"

	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
)

func TestCachedEmptyPacketIsDatagram(t *testing.T) {
	dest := M.ParseSocksaddr("127.0.0.1:53")
	cached := buf.NewSize(1)
	conn := NewCachedPacketConn(&testNetPacketConn{}, cached, dest)
	output := buf.NewSize(1)
	defer output.Release()
	got, err := conn.ReadPacket(output)
	if err != nil || got != dest || output.Len() != 0 {
		t.Fatalf("destination=%v err=%v", got, err)
	}
	if cached.RawCap() != 0 {
		t.Fatal("empty cache retained buffer")
	}
	if packet := conn.ReadCachedPacket(); packet != nil {
		t.Fatal("drained cache returned a nil-buffer packet")
	}
}
