package socks

import (
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

type packetTestConn struct {
	data  []byte
	short bool
}

func (c *packetTestConn) Read(p []byte) (int, error) { return copy(p, c.data), nil }
func (c *packetTestConn) Write(p []byte) (int, error) {
	c.data = append(c.data[:0], p...)
	if c.short {
		return len(p) - 1, nil
	}
	return len(p), nil
}
func (c *packetTestConn) Close() error                     { return nil }
func (c *packetTestConn) LocalAddr() net.Addr              { return M.ParseSocksaddr("127.0.0.1:1") }
func (c *packetTestConn) RemoteAddr() net.Addr             { return M.ParseSocksaddr("127.0.0.1:2") }
func (c *packetTestConn) SetDeadline(time.Time) error      { return nil }
func (c *packetTestConn) SetReadDeadline(time.Time) error  { return nil }
func (c *packetTestConn) SetWriteDeadline(time.Time) error { return nil }
func TestAssociatePacketPayloadCountsAndShortWrites(t *testing.T) {
	raw := &packetTestConn{}
	destination := M.ParseSocksaddr("127.0.0.1:53")
	conn := NewAssociatePacketConn(raw, destination, nil)
	for _, payload := range []string{"", "abc"} {
		n, err := conn.WriteTo([]byte(payload), destination)
		if err != nil || n != len(payload) {
			t.Fatalf("payload count=%d err=%v", n, err)
		}
		b := buf.NewSize(128)
		addr, err := conn.ReadPacket(b)
		if err != nil || string(b.Bytes()) != payload || addr != destination {
			t.Fatalf("roundtrip data=%q addr=%v err=%v", b.Bytes(), addr, err)
		}
		b.Release()
	}
	raw.short = true
	if _, err := conn.WriteTo([]byte("abc"), destination); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write err=%v", err)
	}
}
func TestAssociateInvalidAddressReleasesOwnedPayloads(t *testing.T) {
	raw := &packetTestConn{}
	invalid := M.Socksaddr{Fqdn: strings.Repeat("a", 256), Port: 53}
	conn := NewAssociatePacketConn(raw, invalid, nil)
	payload := buf.NewSize(512)
	payload.Resize(300, 1)
	if err := conn.WritePacket(payload, invalid); err == nil {
		t.Fatal("oversized address accepted")
	}
	if payload.RawCap() != 0 {
		t.Fatal("payload retained")
	}
	vector := NewVectorisedAssociateConn(raw, bufio.NewVectorisedWriter(raw), invalid, nil)
	payload = buf.NewSize(1)
	payload.WriteByte(1)
	if err := vector.WriteVectorisedPacket([]*buf.Buffer{payload}, invalid); err == nil {
		t.Fatal("oversized address accepted")
	}
	if payload.RawCap() != 0 {
		t.Fatal("vector payload retained")
	}
}

type packetTestWaiter struct {
	buffer   *buf.Buffer
	released bool
}

func (w *packetTestWaiter) InitializeReadWaiter(N.ReadWaitOptions) bool { return false }
func (w *packetTestWaiter) WaitReadBuffer() (*buf.Buffer, error)        { return w.buffer, nil }
func (w *packetTestWaiter) ReleaseReadWaiter()                          { w.released = true }
func TestAssociateInvalidFrameReleasesWaiterBuffer(t *testing.T) {
	payload := buf.NewSize(4)
	payload.Write([]byte{0, 0, 0, 1})
	inner := &packetTestWaiter{buffer: payload}
	waiter := &AssociatePacketReadWaiter{conn: NewAssociatePacketConn(&packetTestConn{}, M.Socksaddr{}, nil), readWaiter: inner}
	b, _, err := waiter.WaitReadPacket()
	if err == nil || b != nil || payload.RawCap() != 0 {
		t.Fatalf("buffer=%v err=%v", b, err)
	}
	waiter.ReleaseReadWaiter()
	if !inner.released {
		t.Fatal("release not forwarded")
	}
}
